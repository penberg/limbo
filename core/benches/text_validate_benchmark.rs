//! Head-to-head microbenchmark of two UTF-8 text validation strategies,
//! at the function level (no database engine involved).
//!
//! Baseline: `simdutf8::basic::from_utf8`, exactly what the TEXT serial-type
//! arm of `nth_into_register` in `core/vdbe/mod.rs` calls on main today.
//! Candidates: an ASCII OR-reduction fast path that falls back to the baseline
//! for non-ASCII input, byte by byte or a word at a time.
//!
//! Real TEXT values are decoded from b-tree page cells at arbitrary byte
//! offsets, and short-input `from_utf8` is alignment-sensitive. Each measured
//! iteration therefore validates slices at a fixed schedule of varying
//! offsets whose low three bits cycle through 0..=7.
//!
//! Run:  cargo bench -p turso_core --bench text_validate_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use std::time::Duration;

/// Baseline: verbatim the validation call from the TEXT decode arm in
/// `core/vdbe/mod.rs` (`nth_into_register`).
fn validate_simdutf8(data: &[u8]) -> Option<&str> {
    simdutf8::basic::from_utf8(data).ok()
}

/// The OR-reduction fast path without a size cutoff: it always OR-reduces
/// every byte, takes the unchecked-ASCII path if none has the high bit set,
/// and falls back to the baseline (`simdutf8::basic::from_utf8`) otherwise.
/// Kept as a benchmark variant to document why `validate_utf8` in
/// `core/vdbe/mod.rs` has a cutoff at all.
#[inline]
fn validate_ascii_or(data: &[u8]) -> Option<&str> {
    let mut acc = 0u8;
    for &byte in data {
        acc |= byte;
    }
    if acc.is_ascii() {
        // SAFETY: all bytes are ASCII, which is valid UTF-8.
        return Some(unsafe { core::str::from_utf8_unchecked(data) });
    }
    simdutf8::basic::from_utf8(data).ok()
}

/// The byte-at-a-time OR-reduction with a cutoff: the OR-reduction only
/// runs where it wins (short strings, where simdutf8's std fallback is
/// alignment-sensitive); longer inputs go straight to real SIMD validation.
#[inline]
fn validate_ascii_or_cutoff(data: &[u8]) -> Option<&str> {
    const ASCII_SCAN_CUTOFF: usize = 512;
    if data.len() <= ASCII_SCAN_CUTOFF {
        let mut acc = 0u8;
        for &byte in data {
            acc |= byte;
        }
        if acc.is_ascii() {
            // SAFETY: all bytes are ASCII, which is valid UTF-8.
            return Some(unsafe { core::str::from_utf8_unchecked(data) });
        }
    }
    simdutf8::basic::from_utf8(data).ok()
}

/// Verbatim copy of `validate_utf8` in `core/vdbe/mod.rs`: the OR-reduction
/// of the cutoff variant done eight bytes at a time, then four, two and one
/// for the rest, so a value takes at most `len / 8 + 3` unaligned loads.
#[inline]
fn validate_ascii_words(data: &[u8]) -> Option<&str> {
    const ASCII_SCAN_CUTOFF: usize = 512;
    if data.len() <= ASCII_SCAN_CUTOFF && is_ascii(data) {
        // SAFETY: all bytes are ASCII, which is valid UTF-8.
        return Some(unsafe { core::str::from_utf8_unchecked(data) });
    }
    simdutf8::basic::from_utf8(data).ok()
}

#[inline]
fn is_ascii(data: &[u8]) -> bool {
    let mut acc = 0u64;
    let mut rest = data;
    while let Some((word, tail)) = rest.split_first_chunk::<8>() {
        acc |= u64::from_ne_bytes(*word);
        rest = tail;
    }
    if let Some((word, tail)) = rest.split_first_chunk::<4>() {
        acc |= u64::from(u32::from_ne_bytes(*word));
        rest = tail;
    }
    if let Some((word, tail)) = rest.split_first_chunk::<2>() {
        acc |= u64::from(u16::from_ne_bytes(*word));
        rest = tail;
    }
    if let Some(&byte) = rest.first() {
        acc |= u64::from(byte);
    }
    acc & 0x8080_8080_8080_8080 == 0
}

const BUF_LEN: usize = 8192;
const SLICES_PER_ROUND: usize = 64;
const SIZES: [usize; 13] = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096];

/// Offsets at pseudo-random positions whose low three bits cycle 0..=7, so
/// consecutive calls see differently-aligned slices like record decoding does.
fn ascii_schedule(size: usize) -> Vec<usize> {
    let bases = (BUF_LEN - size) / 8;
    (0..SLICES_PER_ROUND)
        .map(|i| {
            let pr = (i as u32).wrapping_mul(0x9E37_79B1) as usize;
            (pr % bases) * 8 + (i % 8)
        })
        .collect()
}

/// A buffer holding a valid 64-byte multibyte string at each scheduled
/// offset, so odd-offset slices never split a codepoint. Spacing is 72 bytes
/// (a multiple of 8), so alignment still cycles 0..=7.
fn multibyte_fixture() -> (Vec<u8>, Vec<usize>) {
    let mut buf = vec![b'a'; BUF_LEN];
    let pattern = "é".repeat(32).into_bytes();
    assert_eq!(pattern.len(), 64);
    let offsets: Vec<usize> = (0..SLICES_PER_ROUND).map(|i| i * 72 + (i % 8)).collect();
    for &off in &offsets {
        buf[off..off + 64].copy_from_slice(&pattern);
    }
    (buf, offsets)
}

fn run_schedule(
    buf: &[u8],
    schedule: &[usize],
    size: usize,
    rounds: usize,
    validate: impl Fn(&[u8]) -> Option<&str>,
) {
    for _ in 0..rounds {
        for &off in schedule {
            let slice = &buf[off..off + size];
            black_box(validate(black_box(slice)).is_some());
        }
    }
}

/// Batch enough calls per iteration that the smallest sizes are well above
/// timer noise.
fn rounds_for(size: usize) -> usize {
    (32_768 / (SLICES_PER_ROUND * size)).max(1)
}

fn assert_functions_agree() {
    let long_multibyte = "é".repeat(32);
    let cases: [&[u8]; 6] = [
        b"",
        b"hello world",
        "héllo wörld".as_bytes(),
        b"\xff\xfe invalid",
        b"ascii then bad \xc3",
        long_multibyte.as_bytes(),
    ];
    for case in cases {
        assert_eq!(
            validate_simdutf8(case),
            validate_ascii_or(case),
            "functions disagree on {case:?}"
        );
        assert_eq!(
            validate_simdutf8(case),
            validate_ascii_or_cutoff(case),
            "cutoff variant disagrees on {case:?}"
        );
        assert_eq!(
            validate_simdutf8(case),
            validate_ascii_words(case),
            "word variant disagrees on {case:?}"
        );
    }
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_text_validate(criterion: &mut Criterion) {
    assert_functions_agree();

    let mut group = criterion.benchmark_group("text_validate");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    let ascii_buf = vec![b'a'; BUF_LEN];
    for size in SIZES {
        let schedule = ascii_schedule(size);
        let rounds = rounds_for(size);
        group.bench_function(format!("simdutf8_{size}"), |b| {
            b.iter(|| run_schedule(&ascii_buf, &schedule, size, rounds, validate_simdutf8));
        });
        group.bench_function(format!("ascii_or_{size}"), |b| {
            b.iter(|| run_schedule(&ascii_buf, &schedule, size, rounds, validate_ascii_or));
        });
        group.bench_function(format!("ascii_or_cutoff_{size}"), |b| {
            b.iter(|| {
                run_schedule(
                    &ascii_buf,
                    &schedule,
                    size,
                    rounds,
                    validate_ascii_or_cutoff,
                )
            });
        });
        group.bench_function(format!("ascii_words_{size}"), |b| {
            b.iter(|| run_schedule(&ascii_buf, &schedule, size, rounds, validate_ascii_words));
        });
    }

    let (mb_buf, mb_schedule) = multibyte_fixture();
    let rounds = rounds_for(64);
    group.bench_function("simdutf8_multibyte_64", |b| {
        b.iter(|| run_schedule(&mb_buf, &mb_schedule, 64, rounds, validate_simdutf8));
    });
    group.bench_function("ascii_or_multibyte_64", |b| {
        b.iter(|| run_schedule(&mb_buf, &mb_schedule, 64, rounds, validate_ascii_or));
    });
    group.bench_function("ascii_or_cutoff_multibyte_64", |b| {
        b.iter(|| run_schedule(&mb_buf, &mb_schedule, 64, rounds, validate_ascii_or_cutoff));
    });
    group.bench_function("ascii_words_multibyte_64", |b| {
        b.iter(|| run_schedule(&mb_buf, &mb_schedule, 64, rounds, validate_ascii_words));
    });

    group.finish();
}

criterion_group!(benches, bench_text_validate);
criterion_main!(benches);
