use limbo_ext::{register_extension, AggFunc, AggregateDerive, Value};

register_extension! {
    aggregates: { MedianState },
}

#[derive(AggregateDerive)]
struct MedianState;

impl AggFunc for MedianState {
    type State = Vec<f64>;

    fn name(&self) -> &'static str {
        "median"
    }

    fn args(&self) -> i32 {
        1
    }

    fn step(state: &mut Self::State, args: &[Value]) {
        if let Some(val) = args.first().and_then(Value::to_float) {
            state.push(val);
        }
    }

    fn finalize(state: Self::State) -> Value {
        if state.is_empty() {
            return Value::null();
        }

        let mut sorted = state;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let len = sorted.len();
        if len % 2 == 1 {
            Value::from_float(sorted[len / 2])
        } else {
            let mid1 = sorted[len / 2 - 1];
            let mid2 = sorted[len / 2];
            Value::from_float((mid1 + mid2) / 2.0)
        }
    }
}
