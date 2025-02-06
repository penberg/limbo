use crate::types::{OwnedValue, OwnedValueType};
use crate::{LimboError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum VectorType {
    Float32,
    Float64,
}

impl VectorType {
    pub fn size_to_dims(&self, size: usize) -> usize {
        match self {
            VectorType::Float32 => size / 4,
            VectorType::Float64 => size / 8,
        }
    }
}

#[derive(Debug)]
pub struct Vector {
    pub vector_type: VectorType,
    pub dims: usize,
    pub data: Vec<u8>,
}

impl Vector {
    pub fn as_f32_slice(&self) -> &[f32] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const f32, self.dims) }
    }

    pub fn as_f64_slice(&self) -> &[f64] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const f64, self.dims) }
    }
}

/// Parse a vector in text representation into a Vector.
///
/// The format of a vector in text representation looks as follows:
///
/// ```console
/// [1.0, 2.0, 3.0]
/// ```
pub fn parse_string_vector(vector_type: VectorType, value: &OwnedValue) -> Result<Vector> {
    let Some(text) = value.to_text() else {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    };
    let text = text.trim();
    let mut chars = text.chars();
    if chars.next() != Some('[') || chars.last() != Some(']') {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }
    let mut data: Vec<u8> = Vec::new();
    let text = &text[1..text.len() - 1];
    if text.trim().is_empty() {
        return Ok(Vector {
            vector_type,
            dims: 0,
            data,
        });
    }
    let xs = text.split(',');
    for x in xs {
        let x = x.trim();
        if x.is_empty() {
            return Err(LimboError::ConversionError(
                "Invalid vector value".to_string(),
            ));
        }
        match vector_type {
            VectorType::Float32 => {
                let x = x
                    .parse::<f32>()
                    .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
                if !x.is_finite() {
                    return Err(LimboError::ConversionError(
                        "Invalid vector value".to_string(),
                    ));
                }
                data.extend_from_slice(&x.to_le_bytes());
            }
            VectorType::Float64 => {
                let x = x
                    .parse::<f64>()
                    .map_err(|_| LimboError::ConversionError("Invalid vector value".to_string()))?;
                if !x.is_finite() {
                    return Err(LimboError::ConversionError(
                        "Invalid vector value".to_string(),
                    ));
                }
                data.extend_from_slice(&x.to_le_bytes());
            }
        };
    }
    let dims = vector_type.size_to_dims(data.len());
    Ok(Vector {
        vector_type,
        dims,
        data,
    })
}

pub fn parse_vector(value: &OwnedValue, vec_ty: Option<VectorType>) -> Result<Vector> {
    match value.value_type() {
        OwnedValueType::Text => parse_string_vector(vec_ty.unwrap_or(VectorType::Float32), value),
        OwnedValueType::Blob => {
            let Some(blob) = value.to_blob() else {
                return Err(LimboError::ConversionError(
                    "Invalid vector value".to_string(),
                ));
            };
            let vector_type = vector_type(&blob)?;
            if let Some(vec_ty) = vec_ty {
                if vec_ty != vector_type {
                    return Err(LimboError::ConversionError(
                        "Invalid vector type".to_string(),
                    ));
                }
            }
            vector_deserialize(vector_type, &blob)
        }
        _ => Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        )),
    }
}

pub fn vector_to_text(vector: &Vector) -> String {
    let mut text = String::new();
    text.push('[');
    match vector.vector_type {
        VectorType::Float32 => {
            let data = vector.as_f32_slice();
            for i in 0..vector.dims {
                text.push_str(&data[i].to_string());
                if i < vector.dims - 1 {
                    text.push(',');
                }
            }
        }
        VectorType::Float64 => {
            let data = vector.as_f64_slice();
            for i in 0..vector.dims {
                text.push_str(&data[i].to_string());
                if i < vector.dims - 1 {
                    text.push(',');
                }
            }
        }
    }
    text.push(']');
    text
}

pub fn vector_deserialize(vector_type: VectorType, blob: &[u8]) -> Result<Vector> {
    match vector_type {
        VectorType::Float32 => vector_deserialize_f32(blob),
        VectorType::Float64 => vector_deserialize_f64(blob),
    }
}

pub fn vector_serialize_f64(x: Vector) -> OwnedValue {
    let mut blob = Vec::with_capacity(x.dims * 8 + 1);
    blob.extend_from_slice(&x.data);
    blob.push(2);
    OwnedValue::from_blob(blob)
}

pub fn vector_deserialize_f64(blob: &[u8]) -> Result<Vector> {
    Ok(Vector {
        vector_type: VectorType::Float64,
        dims: (blob.len() - 1) / 8,
        data: blob[..blob.len() - 1].to_vec(),
    })
}

pub fn vector_serialize_f32(x: Vector) -> OwnedValue {
    OwnedValue::from_blob(x.data)
}

pub fn vector_deserialize_f32(blob: &[u8]) -> Result<Vector> {
    Ok(Vector {
        vector_type: VectorType::Float32,
        dims: blob.len() / 4,
        data: blob.to_vec(),
    })
}

pub fn do_vector_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
    match v1.vector_type {
        VectorType::Float32 => vector_f32_distance_cos(v1, v2),
        VectorType::Float64 => vector_f64_distance_cos(v1, v2),
    }
}

pub fn vector_f32_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
    if v1.dims != v2.dims {
        return Err(LimboError::ConversionError(
            "Invalid vector dimensions".to_string(),
        ));
    }
    if v1.vector_type != v2.vector_type {
        return Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        ));
    }
    let (mut dot, mut norm1, mut norm2) = (0.0, 0.0, 0.0);
    let v1_data = v1.as_f32_slice();
    let v2_data = v2.as_f32_slice();

    // Check for non-finite values
    if v1_data.iter().any(|x| !x.is_finite()) || v2_data.iter().any(|x| !x.is_finite()) {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }

    for i in 0..v1.dims {
        let e1 = v1_data[i];
        let e2 = v2_data[i];
        dot += e1 * e2;
        norm1 += e1 * e1;
        norm2 += e2 * e2;
    }

    // Check for zero norms to avoid division by zero
    if norm1 == 0.0 || norm2 == 0.0 {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }

    Ok(1.0 - (dot / (norm1 * norm2).sqrt()) as f64)
}

pub fn vector_f64_distance_cos(v1: &Vector, v2: &Vector) -> Result<f64> {
    if v1.dims != v2.dims {
        return Err(LimboError::ConversionError(
            "Invalid vector dimensions".to_string(),
        ));
    }
    if v1.vector_type != v2.vector_type {
        return Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        ));
    }
    let (mut dot, mut norm1, mut norm2) = (0.0, 0.0, 0.0);
    let v1_data = v1.as_f64_slice();
    let v2_data = v2.as_f64_slice();

    // Check for non-finite values
    if v1_data.iter().any(|x| !x.is_finite()) || v2_data.iter().any(|x| !x.is_finite()) {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }

    for i in 0..v1.dims {
        let e1 = v1_data[i];
        let e2 = v2_data[i];
        dot += e1 * e2;
        norm1 += e1 * e1;
        norm2 += e2 * e2;
    }

    // Check for zero norms
    if norm1 == 0.0 || norm2 == 0.0 {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }

    Ok(1.0 - (dot / (norm1 * norm2).sqrt()))
}

pub fn vector_type(blob: &[u8]) -> Result<VectorType> {
    if blob.is_empty() {
        return Err(LimboError::ConversionError(
            "Invalid vector value".to_string(),
        ));
    }
    // Even-sized blobs are always float32.
    if blob.len() % 2 == 0 {
        return Ok(VectorType::Float32);
    }
    // Odd-sized blobs have type byte at the end
    let (data_blob, type_byte) = blob.split_at(blob.len() - 1);
    let vector_type = type_byte[0];
    match vector_type {
        1 => {
            if data_blob.len() % 4 != 0 {
                return Err(LimboError::ConversionError(
                    "Invalid vector value".to_string(),
                ));
            }
            Ok(VectorType::Float32)
        }
        2 => {
            if data_blob.len() % 8 != 0 {
                return Err(LimboError::ConversionError(
                    "Invalid vector value".to_string(),
                ));
            }
            Ok(VectorType::Float64)
        }
        _ => Err(LimboError::ConversionError(
            "Invalid vector type".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    // Helper to generate arbitrary vectors of specific type and dimensions
    #[derive(Debug, Clone)]
    struct ArbitraryVector<const DIMS: usize> {
        vector_type: VectorType,
        data: Vec<u8>,
    }

    /// How to create an arbitrary vector of DIMS dims.
    impl<const DIMS: usize> ArbitraryVector<DIMS> {
        fn generate_f32_vector(g: &mut Gen) -> Vec<f32> {
            (0..DIMS)
                .map(|_| {
                    loop {
                        let f = f32::arbitrary(g);
                        // f32::arbitrary() can generate "problem values" like NaN, infinity, and very small values
                        // Skip these values
                        if f.is_finite() && f.abs() >= 1e-6 {
                            // Scale to [-1, 1] range
                            return f % 2.0 - 1.0;
                        }
                    }
                })
                .collect()
        }

        fn generate_f64_vector(g: &mut Gen) -> Vec<f64> {
            (0..DIMS)
                .map(|_| {
                    loop {
                        let f = f64::arbitrary(g);
                        // f64::arbitrary() can generate "problem values" like NaN, infinity, and very small values
                        // Skip these values
                        if f.is_finite() && f.abs() >= 1e-6 {
                            // Scale to [-1, 1] range
                            return f % 2.0 - 1.0;
                        }
                    }
                })
                .collect()
        }
    }

    /// Convert an ArbitraryVector to a Vector.
    impl<const DIMS: usize> From<ArbitraryVector<DIMS>> for Vector {
        fn from(v: ArbitraryVector<DIMS>) -> Self {
            Vector {
                vector_type: v.vector_type,
                dims: DIMS,
                data: v.data,
            }
        }
    }

    /// Implement the quickcheck Arbitrary trait for ArbitraryVector.
    impl<const DIMS: usize> Arbitrary for ArbitraryVector<DIMS> {
        fn arbitrary(g: &mut Gen) -> Self {
            let vector_type = if bool::arbitrary(g) {
                VectorType::Float32
            } else {
                VectorType::Float64
            };

            let data = match vector_type {
                VectorType::Float32 => {
                    let floats = Self::generate_f32_vector(g);
                    floats.iter().flat_map(|f| f.to_le_bytes()).collect()
                }
                VectorType::Float64 => {
                    let floats = Self::generate_f64_vector(g);
                    floats.iter().flat_map(|f| f.to_le_bytes()).collect()
                }
            };

            ArbitraryVector { vector_type, data }
        }
    }

    #[quickcheck]
    fn prop_vector_type_identification_2d(v: ArbitraryVector<2>) -> bool {
        test_vector_type::<2>(v.into())
    }

    #[quickcheck]
    fn prop_vector_type_identification_3d(v: ArbitraryVector<3>) -> bool {
        test_vector_type::<3>(v.into())
    }

    #[quickcheck]
    fn prop_vector_type_identification_4d(v: ArbitraryVector<4>) -> bool {
        test_vector_type::<4>(v.into())
    }

    #[quickcheck]
    fn prop_vector_type_identification_100d(v: ArbitraryVector<100>) -> bool {
        test_vector_type::<100>(v.into())
    }

    #[quickcheck]
    fn prop_vector_type_identification_1536d(v: ArbitraryVector<1536>) -> bool {
        test_vector_type::<1536>(v.into())
    }

    /// Test if the vector type identification is correct for a given vector.
    fn test_vector_type<const DIMS: usize>(v: Vector) -> bool {
        let vtype = v.vector_type.clone();
        let value = match &vtype {
            VectorType::Float32 => vector_serialize_f32(v),
            VectorType::Float64 => vector_serialize_f64(v),
        };

        let blob = value.to_blob().unwrap();
        match vector_type(blob) {
            Ok(detected_type) => detected_type == vtype,
            Err(_) => false,
        }
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_2d(v: ArbitraryVector<2>) -> bool {
        test_slice_conversion::<2>(v.into())
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_3d(v: ArbitraryVector<3>) -> bool {
        test_slice_conversion::<3>(v.into())
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_4d(v: ArbitraryVector<4>) -> bool {
        test_slice_conversion::<4>(v.into())
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_100d(v: ArbitraryVector<100>) -> bool {
        test_slice_conversion::<100>(v.into())
    }

    #[quickcheck]
    fn prop_slice_conversion_safety_1536d(v: ArbitraryVector<1536>) -> bool {
        test_slice_conversion::<1536>(v.into())
    }

    /// Test if the slice conversion is safe for a given vector:
    /// - The slice length matches the dimensions
    /// - The data length is correct (4 bytes per float for f32, 8 bytes per float for f64)
    fn test_slice_conversion<const DIMS: usize>(v: Vector) -> bool {
        match v.vector_type {
            VectorType::Float32 => {
                let slice = v.as_f32_slice();
                // Check if the slice length matches the dimensions and the data length is correct (4 bytes per float)
                slice.len() == DIMS && (slice.len() * 4 == v.data.len())
            }
            VectorType::Float64 => {
                let slice = v.as_f64_slice();
                // Check if the slice length matches the dimensions and the data length is correct (8 bytes per float)
                slice.len() == DIMS && (slice.len() * 8 == v.data.len())
            }
        }
    }

    // Test size_to_dims calculation with different dimensions
    #[quickcheck]
    fn prop_size_to_dims_calculation_2d(v: ArbitraryVector<2>) -> bool {
        test_size_to_dims::<2>(v.into())
    }

    #[quickcheck]
    fn prop_size_to_dims_calculation_3d(v: ArbitraryVector<3>) -> bool {
        test_size_to_dims::<3>(v.into())
    }

    #[quickcheck]
    fn prop_size_to_dims_calculation_4d(v: ArbitraryVector<4>) -> bool {
        test_size_to_dims::<4>(v.into())
    }

    #[quickcheck]
    fn prop_size_to_dims_calculation_100d(v: ArbitraryVector<100>) -> bool {
        test_size_to_dims::<100>(v.into())
    }

    #[quickcheck]
    fn prop_size_to_dims_calculation_1536d(v: ArbitraryVector<1536>) -> bool {
        test_size_to_dims::<1536>(v.into())
    }

    /// Test if the size_to_dims calculation is correct for a given vector.
    fn test_size_to_dims<const DIMS: usize>(v: Vector) -> bool {
        let size = v.data.len();
        let calculated_dims = v.vector_type.size_to_dims(size);
        calculated_dims == DIMS
    }

    #[quickcheck]
    fn prop_vector_distance_safety_2d(v1: ArbitraryVector<2>, v2: ArbitraryVector<2>) -> bool {
        test_vector_distance::<2>(&v1.into(), &v2.into())
    }

    #[quickcheck]
    fn prop_vector_distance_safety_3d(v1: ArbitraryVector<3>, v2: ArbitraryVector<3>) -> bool {
        test_vector_distance::<3>(&v1.into(), &v2.into())
    }

    #[quickcheck]
    fn prop_vector_distance_safety_4d(v1: ArbitraryVector<4>, v2: ArbitraryVector<4>) -> bool {
        test_vector_distance::<4>(&v1.into(), &v2.into())
    }

    #[quickcheck]
    fn prop_vector_distance_safety_100d(
        v1: ArbitraryVector<100>,
        v2: ArbitraryVector<100>,
    ) -> bool {
        test_vector_distance::<100>(&v1.into(), &v2.into())
    }

    #[quickcheck]
    fn prop_vector_distance_safety_1536d(
        v1: ArbitraryVector<1536>,
        v2: ArbitraryVector<1536>,
    ) -> bool {
        test_vector_distance::<1536>(&v1.into(), &v2.into())
    }

    /// Test if the vector distance calculation is correct for a given pair of vectors:
    /// - The vectors have the same dimensions
    /// - The vectors have the same type
    /// - The distance must be between 0 and 2
    fn test_vector_distance<const DIMS: usize>(v1: &Vector, v2: &Vector) -> bool {
        if v1.vector_type != v2.vector_type {
            // Skip test if types are different
            return true;
        }
        match do_vector_distance_cos(&v1, &v2) {
            Ok(distance) => {
                // Cosine distance is always between 0 and 2
                (0.0..=2.0).contains(&distance)
            }
            Err(_) => false,
        }
    }

    #[test]
    fn parse_string_vector_zero_length() {
        let value = OwnedValue::from_text("[]");
        let vector = parse_string_vector(VectorType::Float32, &value).unwrap();
        assert_eq!(vector.dims, 0);
        assert_eq!(vector.vector_type, VectorType::Float32);
    }

    #[test]
    fn test_parse_string_vector_valid_whitespace() {
        let value = OwnedValue::from_text("  [  1.0  ,  2.0  ,  3.0  ]  ");
        let vector = parse_string_vector(VectorType::Float32, &value).unwrap();
        assert_eq!(vector.dims, 3);
        assert_eq!(vector.vector_type, VectorType::Float32);
    }

    #[test]
    fn test_parse_string_vector_valid() {
        let value = OwnedValue::from_text("[1.0, 2.0, 3.0]");
        let vector = parse_string_vector(VectorType::Float32, &value).unwrap();
        assert_eq!(vector.dims, 3);
        assert_eq!(vector.vector_type, VectorType::Float32);
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_2d(v: ArbitraryVector<2>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_3d(v: ArbitraryVector<3>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_4d(v: ArbitraryVector<4>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_100d(v: ArbitraryVector<100>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    #[quickcheck]
    fn prop_vector_text_roundtrip_1536d(v: ArbitraryVector<1536>) -> bool {
        test_vector_text_roundtrip(v.into())
    }

    /// Test that a vector can be converted to text and back without loss of precision
    fn test_vector_text_roundtrip(v: Vector) -> bool {
        // Convert to text
        let text = vector_to_text(&v);

        // Parse back from text
        let value = OwnedValue::from_text(&text);
        let parsed = parse_string_vector(v.vector_type.clone(), &value);

        match parsed {
            Ok(parsed_vector) => {
                // Check dimensions match
                if v.dims != parsed_vector.dims {
                    return false;
                }

                match v.vector_type {
                    VectorType::Float32 => {
                        let original = v.as_f32_slice();
                        let parsed = parsed_vector.as_f32_slice();
                        original.iter().zip(parsed.iter()).all(|(a, b)| a == b)
                    }
                    VectorType::Float64 => {
                        let original = v.as_f64_slice();
                        let parsed = parsed_vector.as_f64_slice();
                        original.iter().zip(parsed.iter()).all(|(a, b)| a == b)
                    }
                }
            }
            Err(_) => false,
        }
    }
}
