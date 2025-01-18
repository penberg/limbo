use limbo_ext::{register_extension, AggFunc, AggregateDerive, ResultCode, Value};

register_extension! {
    aggregates: { Median, Percentile, PercentileCont, PercentileDisc }
}

#[derive(AggregateDerive)]
struct Median;

impl AggFunc for Median {
    type State = Vec<f64>;
    const NAME: &'static str = "median";
    const ARGS: i32 = 1;

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

#[derive(AggregateDerive)]
struct Percentile;

impl AggFunc for Percentile {
    type State = (Vec<f64>, Option<f64>, Option<()>);

    const NAME: &'static str = "percentile";
    const ARGS: i32 = 2;

    fn step(state: &mut Self::State, args: &[Value]) {
        let (values, p_value, err_value) = state;
        if let (Some(y), Some(p)) = (
            args.first().and_then(Value::to_float),
            args.get(1).and_then(Value::to_float),
        ) {
            if !(0.0..=100.0).contains(&p) {
                err_value.get_or_insert(());
                return;
            }

            if let Some(existing_p) = *p_value {
                if (existing_p - p).abs() >= 0.001 {
                    err_value.get_or_insert(());
                    return;
                }
            } else {
                *p_value = Some(p);
            }
            values.push(y);
        }
    }

    fn finalize(state: Self::State) -> Value {
        let (mut values, p_value, err_value) = state;
        if values.is_empty() {
            return Value::null();
        }
        if err_value.is_some() {
            return Value::error(ResultCode::Error);
        }
        if values.len() == 1 {
            return Value::from_float(values[0]);
        }

        let p = p_value.unwrap();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = values.len() as f64;
        let index = p * (n - 1.0) / 100.0;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;

        if lower == upper {
            Value::from_float(values[lower])
        } else {
            let weight = index - lower as f64;
            Value::from_float(values[lower] * (1.0 - weight) + values[upper] * weight)
        }
    }
}

#[derive(AggregateDerive)]
struct PercentileCont;

impl AggFunc for PercentileCont {
    type State = (Vec<f64>, Option<f64>, Option<()>);

    const NAME: &'static str = "percentile_cont";
    const ARGS: i32 = 2;

    fn step(state: &mut Self::State, args: &[Value]) {
        let (values, p_value, err_state) = state;
        if let (Some(y), Some(p)) = (
            args.first().and_then(Value::to_float),
            args.get(1).and_then(Value::to_float),
        ) {
            if !(0.0..=1.0).contains(&p) {
                err_state.get_or_insert(());
                return;
            }

            if let Some(existing_p) = *p_value {
                if (existing_p - p).abs() >= 0.001 {
                    err_state.get_or_insert(());
                    return;
                }
            } else {
                *p_value = Some(p);
            }
            values.push(y);
        }
    }

    fn finalize(state: Self::State) -> Value {
        let (mut values, p_value, err_state) = state;
        if values.is_empty() {
            return Value::null();
        }
        if err_state.is_some() {
            return Value::error(ResultCode::Error);
        }
        if values.len() == 1 {
            return Value::from_float(values[0]);
        }

        let p = p_value.unwrap();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = values.len() as f64;
        let index = p * (n - 1.0);
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;

        if lower == upper {
            Value::from_float(values[lower])
        } else {
            let weight = index - lower as f64;
            Value::from_float(values[lower] * (1.0 - weight) + values[upper] * weight)
        }
    }
}

#[derive(AggregateDerive)]
struct PercentileDisc;

impl AggFunc for PercentileDisc {
    type State = (Vec<f64>, Option<f64>, Option<()>);

    const NAME: &'static str = "percentile_disc";
    const ARGS: i32 = 2;

    fn step(state: &mut Self::State, args: &[Value]) {
        Percentile::step(state, args);
    }

    fn finalize(state: Self::State) -> Value {
        let (mut values, p_value, err_value) = state;
        if values.is_empty() {
            return Value::null();
        }
        if err_value.is_some() {
            return Value::error(ResultCode::Error);
        }

        let p = p_value.unwrap();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = values.len() as f64;
        let index = (p * (n - 1.0)).floor() as usize;
        Value::from_float(values[index])
    }
}
