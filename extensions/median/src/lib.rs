use limbo_extension::{declare_aggregate, AggFunc, Value};

#[declare_aggregate(name = "median")]
pub struct MedianState {
    values: Vec<f64>,
}

impl AggFunc for MedianState {
    fn step(&mut self, args: &[Value]) {
        if let Some(val) = args.get(0).and_then(Value::to_float) {
            self.values.push(val);
        }
    }

    fn finalize(self) -> Value {
        if self.values.is_empty() {
            return Value::null();
        }

        let mut sorted = self.values;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let len = sorted.len();
        if len % 2 == 1 {
            // Odd number of elements: return the middle one
            Value::from_float(sorted[len / 2])
        } else {
            // Even number of elements: return the average of the two middle ones
            let mid1 = sorted[len / 2 - 1];
            let mid2 = sorted[len / 2];
            Value::from_float((mid1 + mid2) / 2.0)
        }
    }
}
