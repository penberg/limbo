use crate::{
    vector::vector_types::{Vector, VectorSparse, VectorType},
    LimboError, Result,
};
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
use simsimd::SpatialSimilarity;

pub fn vector_distance_dot(v1: &Vector, v2: &Vector) -> Result<f64> {
    if v1.dims != v2.dims {
        return Err(LimboError::ConversionError(
            "Vectors must have the same dimensions".to_string(),
        ));
    }
    if v1.vector_type != v2.vector_type {
        return Err(LimboError::ConversionError(
            "Vectors must be of the same type".to_string(),
        ));
    }
    match v1.vector_type {
        VectorType::Float32Dense => Ok(vector_f32_distance_dot_simsimd(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        VectorType::Float64Dense => Ok(vector_f64_distance_dot_simsimd(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        VectorType::Float32Sparse => Ok(vector_f32_sparse_distance_dot(
            v1.as_f32_sparse(),
            v2.as_f32_sparse(),
        )),
        VectorType::Float1Bit => Ok(vector_1bit_distance_dot(v1, v2)),
        VectorType::Float8 => Ok(vector_f8_distance_dot(v1, v2)),
    }
}

fn vector_1bit_distance_dot(v1: &Vector, v2: &Vector) -> f64 {
    // 1-bit values represent +1/-1.
    // Dot product = dims - 2 * hamming_distance
    // Return negated (consistent with existing dot distance convention).
    let d1 = v1.as_1bit_data();
    let d2 = v2.as_1bit_data();
    let mut hamming = 0u32;
    for (&a, &b) in d1.iter().zip(d2.iter()) {
        hamming += (a ^ b).count_ones();
    }
    let dot = v1.dims as f64 - 2.0 * hamming as f64;
    -dot
}

fn vector_f8_distance_dot(v1: &Vector, v2: &Vector) -> f64 {
    let (data1, alpha1, shift1) = v1.as_f8_data();
    let (data2, alpha2, shift2) = v2.as_f8_data();
    let dims = v1.dims;

    let (mut sum1, mut sum2, mut doti) = (0u64, 0u64, 0u64);
    for i in 0..dims {
        let q1 = data1[i] as u64;
        let q2 = data2[i] as u64;
        sum1 += q1;
        sum2 += q2;
        doti += q1 * q2;
    }

    let a1 = alpha1 as f64;
    let a2 = alpha2 as f64;
    let s1 = shift1 as f64;
    let s2 = shift2 as f64;
    let d = dims as f64;

    let dot = a1 * a2 * doti as f64 + a1 * s2 * sum1 as f64 + a2 * s1 * sum2 as f64 + s1 * s2 * d;
    -dot
}

#[allow(dead_code)]
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
fn vector_f32_distance_dot_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    -f32::dot(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD does not support WASM, and Windows AArch64 has linker issues with simsimd.lib.
#[allow(dead_code)]
fn vector_f32_distance_dot_rust(v1: &[f32], v2: &[f32]) -> f64 {
    let mut dot = 0.0;
    for (a, b) in v1.iter().zip(v2.iter()) {
        dot += (*a as f64) * (*b as f64);
    }
    -dot
}

#[allow(dead_code)]
#[cfg(not(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
)))]
fn vector_f32_distance_dot_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    vector_f32_distance_dot_rust(v1, v2)
}

#[allow(dead_code)]
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
fn vector_f64_distance_dot_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    -f64::dot(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD does not support WASM, and Windows AArch64 has linker issues with simsimd.lib.
#[allow(dead_code)]
fn vector_f64_distance_dot_rust(v1: &[f64], v2: &[f64]) -> f64 {
    let mut dot = 0.0;
    for (a, b) in v1.iter().zip(v2.iter()) {
        dot += *a * *b;
    }
    -dot
}

#[allow(dead_code)]
#[cfg(not(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
)))]
fn vector_f64_distance_dot_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    vector_f64_distance_dot_rust(v1, v2)
}

fn vector_f32_sparse_distance_dot(v1: VectorSparse<f32>, v2: VectorSparse<f32>) -> f64 {
    let mut v1_pos = 0;
    let mut v2_pos = 0;
    let mut dot = 0.0;
    while v1_pos < v1.idx.len() && v2_pos < v2.idx.len() {
        let idx1 = v1.idx[v1_pos];
        let idx2 = v2.idx[v2_pos];
        if idx1 == idx2 {
            let e1 = v1.values[v1_pos];
            let e2 = v2.values[v2_pos];
            dot += (e1 as f64) * (e2 as f64);
            v1_pos += 1;
            v2_pos += 1;
        } else if idx1 < idx2 {
            v1_pos += 1;
        } else {
            v2_pos += 1;
        }
    }
    -dot
}

#[cfg(test)]
mod tests {
    use crate::vector::{
        operations::convert::vector_convert, vector_types::tests::ArbitraryVector,
    };

    use super::*;
    use quickcheck_macros::quickcheck;

    #[test]
    fn test_vector_distance_dot_f32() {
        assert_eq!(vector_f32_distance_dot_simsimd(&[], &[]), 0.0);
        assert_eq!(
            vector_f32_distance_dot_simsimd(&[1.0, 0.0], &[0.0, 1.0]),
            0.0
        );
        assert!((vector_f32_distance_dot_simsimd(&[1.0, 2.0], &[1.0, 2.0]) - (-5.0)).abs() < 1e-6);
        assert!((vector_f32_distance_dot_simsimd(&[1.0, 2.0], &[2.0, 4.0]) - (-10.0)).abs() < 1e-6);
        assert!((vector_f32_distance_dot_simsimd(&[1.0, 2.0], &[-1.0, -2.0]) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_vector_distance_dot_f64() {
        assert_eq!(vector_f64_distance_dot_simsimd(&[], &[]), 0.0);
        assert_eq!(
            vector_f64_distance_dot_simsimd(&[1.0, 0.0], &[0.0, 1.0]),
            0.0
        );
        assert!((vector_f64_distance_dot_simsimd(&[1.0, 2.0], &[1.0, 2.0]) - (-5.0)).abs() < 1e-6);
        assert!((vector_f64_distance_dot_simsimd(&[1.0, 2.0], &[-1.0, -2.0]) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_vector_distance_dot_f32_sparse() {
        let v1_sparse = VectorSparse {
            idx: &[1, 2],
            values: &[1.0, 2.0],
        };
        let v2_sparse = VectorSparse {
            idx: &[1, 3],
            values: &[2.0, 3.0],
        };
        let v1_dense = &[0.0, 1.0, 2.0, 0.0];
        let v2_dense = &[0.0, 2.0, 0.0, 3.0];
        let sparse_dist = vector_f32_sparse_distance_dot(v1_sparse, v2_sparse);
        let dense_dist = vector_f32_distance_dot_simsimd(v1_dense, v2_dense);
        assert!((sparse_dist - dense_dist).abs() < 1e-7);
        assert!((sparse_dist - (-2.0)).abs() < 1e-7);
    }

    #[quickcheck]
    fn prop_vector_distance_dot_dense_vs_sparse(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1_dense = vector_convert(
            v1.try_into().expect("generated vector must be valid"),
            VectorType::Float32Dense,
        )
        .unwrap();
        let v2_dense = vector_convert(
            v2.try_into().expect("generated vector must be valid"),
            VectorType::Float32Dense,
        )
        .unwrap();
        let d_dense =
            vector_f32_distance_dot_rust(v1_dense.as_f32_slice(), v2_dense.as_f32_slice());
        let v1_sparse = vector_convert(v1_dense, VectorType::Float32Sparse).unwrap();
        let v2_sparse = vector_convert(v2_dense, VectorType::Float32Sparse).unwrap();
        let d_sparse =
            vector_f32_sparse_distance_dot(v1_sparse.as_f32_sparse(), v2_sparse.as_f32_sparse());
        (d_dense.is_nan() && d_sparse.is_nan()) || (d_dense - d_sparse).abs() < 1e-5
    }

    #[quickcheck]
    fn prop_vector_distance_dot_rust_vs_simsimd_f32(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(
            v1.try_into().expect("generated vector must be valid"),
            VectorType::Float32Dense,
        )
        .unwrap();
        let v2 = vector_convert(
            v2.try_into().expect("generated vector must be valid"),
            VectorType::Float32Dense,
        )
        .unwrap();
        let d_rust = vector_f32_distance_dot_rust(v1.as_f32_slice(), v2.as_f32_slice());
        let d_simd = vector_f32_distance_dot_simsimd(v1.as_f32_slice(), v2.as_f32_slice());
        (d_rust.is_nan() && d_simd.is_nan()) || (d_rust - d_simd).abs() < 1e-4
    }

    #[quickcheck]
    fn prop_vector_distance_dot_rust_vs_simsimd_f64(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(
            v1.try_into().expect("generated vector must be valid"),
            VectorType::Float64Dense,
        )
        .unwrap();
        let v2 = vector_convert(
            v2.try_into().expect("generated vector must be valid"),
            VectorType::Float64Dense,
        )
        .unwrap();
        let d_rust = vector_f64_distance_dot_rust(v1.as_f64_slice(), v2.as_f64_slice());
        let d_simd = vector_f64_distance_dot_simsimd(v1.as_f64_slice(), v2.as_f64_slice());
        (d_rust.is_nan() && d_simd.is_nan()) || (d_rust - d_simd).abs() < 1e-6
    }

    /// Float8 optimized dot distance matches dequantized Float32 dot distance.
    #[quickcheck]
    fn prop_vector_distance_dot_f8_vs_dequantized(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(
            v1.try_into().expect("generated vector must be valid"),
            VectorType::Float32Dense,
        )
        .unwrap();
        let v2 = vector_convert(
            v2.try_into().expect("generated vector must be valid"),
            VectorType::Float32Dense,
        )
        .unwrap();
        let v1_f8 = vector_convert(v1, VectorType::Float8).unwrap();
        let v2_f8 = vector_convert(v2, VectorType::Float8).unwrap();
        let d_f8 = vector_distance_dot(&v1_f8, &v2_f8).unwrap();
        let v1_deq = vector_convert(v1_f8, VectorType::Float32Dense).unwrap();
        let v2_deq = vector_convert(v2_f8, VectorType::Float32Dense).unwrap();
        let d_deq = vector_distance_dot(&v1_deq, &v2_deq).unwrap();
        (d_f8.is_nan() && d_deq.is_nan()) || (d_f8 - d_deq).abs() < 1e-3
    }

    /// Float1Bit dot distance matches dequantized ±1 Float32 dot distance.
    #[quickcheck]
    fn prop_vector_distance_dot_1bit_vs_dequantized(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        let v1 = vector_convert(
            v1.try_into().expect("generated vector must be valid"),
            VectorType::Float1Bit,
        )
        .unwrap();
        let v2 = vector_convert(
            v2.try_into().expect("generated vector must be valid"),
            VectorType::Float1Bit,
        )
        .unwrap();
        let d_1bit = vector_distance_dot(&v1, &v2).unwrap();
        let v1_f32 = vector_convert(v1, VectorType::Float32Dense).unwrap();
        let v2_f32 = vector_convert(v2, VectorType::Float32Dense).unwrap();
        let d_f32 = vector_distance_dot(&v1_f32, &v2_f32).unwrap();
        (d_1bit - d_f32).abs() < 1e-6
    }
}
