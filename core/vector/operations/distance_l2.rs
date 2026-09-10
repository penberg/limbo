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

pub fn vector_distance_l2(v1: &Vector, v2: &Vector) -> Result<f64> {
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
        VectorType::Float32Dense => Ok(vector_f32_distance_l2_simsimd(
            v1.as_f32_slice(),
            v2.as_f32_slice(),
        )),
        VectorType::Float64Dense => Ok(vector_f64_distance_l2_simsimd(
            v1.as_f64_slice(),
            v2.as_f64_slice(),
        )),
        VectorType::Float32Sparse => Ok(vector_f32_sparse_distance_l2(
            v1.as_f32_sparse(),
            v2.as_f32_sparse(),
        )),
        VectorType::Float1Bit => Err(LimboError::ConversionError(
            "L2 distance is not supported for float1bit vectors".to_string(),
        )),
        VectorType::Float8 => Ok(vector_f8_distance_l2(v1, v2)),
    }
}

fn vector_f8_distance_l2(v1: &Vector, v2: &Vector) -> f64 {
    let (data1, alpha1, shift1) = v1.as_f8_data();
    let (data2, alpha2, shift2) = v2.as_f8_data();
    let mut sum = 0.0f64;
    for i in 0..v1.dims {
        let f1 = alpha1 as f64 * data1[i] as f64 + shift1 as f64;
        let f2 = alpha2 as f64 * data2[i] as f64 + shift2 as f64;
        let d = f1 - f2;
        sum += d * d;
    }
    sum.sqrt()
}

#[allow(dead_code)]
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
fn vector_f32_distance_l2_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    f32::euclidean(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD does not support WASM, and Windows AArch64 has linker issues with simsimd.lib.
#[allow(dead_code)]
fn vector_f32_distance_l2_rust(v1: &[f32], v2: &[f32]) -> f64 {
    let sum = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f32>() as f64;
    sum.sqrt()
}

#[allow(dead_code)]
#[cfg(not(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
)))]
fn vector_f32_distance_l2_simsimd(v1: &[f32], v2: &[f32]) -> f64 {
    vector_f32_distance_l2_rust(v1, v2)
}

#[allow(dead_code)]
#[cfg(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
))]
fn vector_f64_distance_l2_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    f64::euclidean(v1, v2).unwrap_or(f64::NAN)
}

// SimSIMD does not support WASM, and Windows AArch64 has linker issues with simsimd.lib.
#[allow(dead_code)]
fn vector_f64_distance_l2_rust(v1: &[f64], v2: &[f64]) -> f64 {
    let sum = v1
        .iter()
        .zip(v2.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f64>();
    sum.sqrt()
}

#[allow(dead_code)]
#[cfg(not(all(
    feature = "simd",
    not(any(
        target_family = "wasm",
        all(target_os = "windows", target_arch = "aarch64")
    ))
)))]
fn vector_f64_distance_l2_simsimd(v1: &[f64], v2: &[f64]) -> f64 {
    vector_f64_distance_l2_rust(v1, v2)
}

fn vector_f32_sparse_distance_l2(v1: VectorSparse<f32>, v2: VectorSparse<f32>) -> f64 {
    let mut v1_pos = 0;
    let mut v2_pos = 0;
    let mut sum = 0.0;
    while v1_pos < v1.idx.len() && v2_pos < v2.idx.len() {
        if v1.idx[v1_pos] == v2.idx[v2_pos] {
            sum += (v1.values[v1_pos] - v2.values[v2_pos]).powi(2);
            v1_pos += 1;
            v2_pos += 1;
        } else if v1.idx[v1_pos] < v2.idx[v2_pos] {
            sum += v1.values[v1_pos].powi(2);
            v1_pos += 1;
        } else {
            sum += v2.values[v2_pos].powi(2);
            v2_pos += 1;
        }
    }
    while v1_pos < v1.idx.len() {
        sum += v1.values[v1_pos].powi(2);
        v1_pos += 1;
    }
    while v2_pos < v2.idx.len() {
        sum += v2.values[v2_pos].powi(2);
        v2_pos += 1;
    }
    (sum as f64).sqrt()
}

#[cfg(test)]
mod tests {
    use quickcheck_macros::quickcheck;

    use crate::vector::{
        operations::convert::vector_convert, vector_types::tests::ArbitraryVector,
    };

    use super::*;

    #[test]
    fn test_vector_distance_l2_f32_another() {
        let vectors = [
            (0..8).map(|x| x as f32).collect::<Vec<f32>>(),
            (1..9).map(|x| x as f32).collect::<Vec<f32>>(),
            (2..10).map(|x| x as f32).collect::<Vec<f32>>(),
            (3..11).map(|x| x as f32).collect::<Vec<f32>>(),
        ];
        let query = (2..10).map(|x| x as f32).collect::<Vec<f32>>();

        let expected: Vec<f64> = vec![
            32.0_f64.sqrt(),
            8.0_f64.sqrt(),
            0.0_f64.sqrt(),
            8.0_f64.sqrt(),
        ];
        let results = vectors
            .iter()
            .map(|v| vector_f32_distance_l2_rust(&query, v))
            .collect::<Vec<f64>>();
        assert_eq!(results, expected);
    }

    #[test]
    fn test_vector_distance_l2_odd_len() {
        let v = (0..5).map(|x| x as f32).collect::<Vec<f32>>();
        let query = (2..7).map(|x| x as f32).collect::<Vec<f32>>();
        assert_eq!(vector_f32_distance_l2_rust(&v, &query), 20.0_f64.sqrt());
    }

    #[test]
    fn test_vector_distance_l2_f32() {
        assert_eq!(vector_f32_distance_l2_rust(&[], &[]), 0.0);
        assert_eq!(
            vector_f32_distance_l2_rust(&[1.0, 2.0], &[0.0, 0.0]),
            (1f64 + 2f64 * 2f64).sqrt()
        );
        assert_eq!(vector_f32_distance_l2_rust(&[1.0, 2.0], &[1.0, 2.0]), 0.0);
        assert_eq!(
            vector_f32_distance_l2_rust(&[1.0, 2.0], &[-1.0, -2.0]),
            (2f64 * 2f64 + 4f64 * 4f64).sqrt()
        );
        assert_eq!(
            vector_f32_distance_l2_rust(&[1.0, 2.0], &[-2.0, 1.0]),
            (3f64 * 3f64 + 1f64 * 1f64).sqrt()
        );
    }

    #[test]
    fn test_vector_distance_l2_f64() {
        assert_eq!(vector_f64_distance_l2_rust(&[], &[]), 0.0);
        assert_eq!(
            vector_f64_distance_l2_rust(&[1.0, 2.0], &[0.0, 0.0]),
            (1f64 + 2f64 * 2f64).sqrt()
        );
        assert_eq!(vector_f64_distance_l2_rust(&[1.0, 2.0], &[1.0, 2.0]), 0.0);
        assert_eq!(
            vector_f64_distance_l2_rust(&[1.0, 2.0], &[-1.0, -2.0]),
            (2f64 * 2f64 + 4f64 * 4f64).sqrt()
        );
        assert_eq!(
            vector_f64_distance_l2_rust(&[1.0, 2.0], &[-2.0, 1.0]),
            (3f64 * 3f64 + 1f64 * 1f64).sqrt()
        );
    }

    #[test]
    fn test_vector_distance_l2_f32_sparse() {
        assert!(
            (vector_f32_sparse_distance_l2(
                VectorSparse {
                    idx: &[0, 1],
                    values: &[1.0, 2.0]
                },
                VectorSparse {
                    idx: &[1, 2],
                    values: &[1.0, 3.0]
                },
            ) - vector_f32_distance_l2_rust(&[1.0, 2.0, 0.0], &[0.0, 1.0, 3.0]))
            .abs()
                < 1e-7
        );
    }

    #[quickcheck]
    fn prop_vector_distance_l2_dense_vs_sparse(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        // Dense uses simsimd, sparse uses rust impl. These can differ by up to 1e-4
        // (as demonstrated by prop_vector_distance_l2_rust_vs_simsimd_f32).
        let tolerance = 1e-4;
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
        let d1 = vector_distance_l2(&v1, &v2).unwrap();

        let sparse1 = vector_convert(v1, VectorType::Float32Sparse).unwrap();
        let sparse2 = vector_convert(v2, VectorType::Float32Sparse).unwrap();
        let d2 = vector_f32_sparse_distance_l2(sparse1.as_f32_sparse(), sparse2.as_f32_sparse());

        (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < tolerance
    }

    #[quickcheck]
    fn prop_vector_distance_l2_rust_vs_simsimd_f32(
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
        let d1 = vector_f32_distance_l2_rust(v1.as_f32_slice(), v2.as_f32_slice());
        let d2 = vector_f32_distance_l2_simsimd(v1.as_f32_slice(), v2.as_f32_slice());
        (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-4
    }

    #[quickcheck]
    fn prop_vector_distance_l2_rust_vs_simsimd_f64(
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
        let d1 = vector_f64_distance_l2_rust(v1.as_f64_slice(), v2.as_f64_slice());
        let d2 = vector_f64_distance_l2_simsimd(v1.as_f64_slice(), v2.as_f64_slice());
        (d1.is_nan() && d2.is_nan()) || (d1 - d2).abs() < 1e-6
    }

    /// Float8 L2 distance matches dequantized Float32 L2 distance.
    #[quickcheck]
    fn prop_vector_distance_l2_f8_vs_dequantized(
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
        let d_f8 = vector_distance_l2(&v1_f8, &v2_f8).unwrap();
        let v1_deq = vector_convert(v1_f8, VectorType::Float32Dense).unwrap();
        let v2_deq = vector_convert(v2_f8, VectorType::Float32Dense).unwrap();
        let d_deq = vector_distance_l2(&v1_deq, &v2_deq).unwrap();
        (d_f8.is_nan() && d_deq.is_nan()) || (d_f8 - d_deq).abs() < 1e-4
    }

    /// Float1Bit L2 distance returns an error.
    #[test]
    fn test_vector_distance_l2_1bit_error() {
        let v1 = Vector::from_1bit(4, crate::alloc::vec![0b1010]);
        let v2 = Vector::from_1bit(4, crate::alloc::vec![0b0101]);
        assert!(vector_distance_l2(&v1, &v2).is_err());
    }
}
