use crate::sync::Arc;

use turso_ext::{
    Connection, ConstraintInfo, ConstraintOp, ConstraintUsage, ExtensionApi, IndexInfo,
    OrderByInfo, ResultCode, VTabCursor, VTabKind, VTabModule, VTabModuleDerive, VTable, Value,
    ValueType,
};

pub fn register_extension(ext_api: &mut ExtensionApi) {
    // FIXME: Add macro magic to register functions automatically.
    unsafe {
        GenerateSeriesVTabModule::register_GenerateSeriesVTabModule(ext_api);
    }
}

macro_rules! extract_arg_integer {
    ($args:expr, $idx:expr) => {
        $args.get($idx).and_then(|v| v.to_integer())
    };
}

/// A virtual table that generates a sequence of integers
#[derive(Debug, VTabModuleDerive, Default)]
struct GenerateSeriesVTabModule;

impl VTabModule for GenerateSeriesVTabModule {
    type Table = GenerateSeriesTable;
    const NAME: &'static str = "generate_series";
    const VTAB_KIND: VTabKind = VTabKind::TableValuedFunction;

    fn create(_args: &[Value]) -> Result<(String, Self::Table), ResultCode> {
        let schema = "CREATE TABLE generate_series (
            value INTEGER,
            start INTEGER HIDDEN,
            stop INTEGER HIDDEN,
            step INTEGER HIDDEN
        )"
        .into();
        Ok((schema, GenerateSeriesTable {}))
    }
}

struct GenerateSeriesTable {}

impl VTable for GenerateSeriesTable {
    type Cursor = GenerateSeriesCursor;
    type Error = ResultCode;

    fn open(&self, _conn: Option<Arc<Connection>>) -> Result<Self::Cursor, Self::Error> {
        Ok(GenerateSeriesCursor {
            start: 0,
            stop: 0,
            step: 0,
            current: 0,
        })
    }

    fn best_index(
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> Result<IndexInfo, ResultCode> {
        const START_COLUMN_INDEX: u32 = 1;
        const STEP_COLUMN_INDEX: u32 = 3;

        // The bits of `idx_num` are used to indicate which arguments are available to the filter method:
        // - Bit 0 set -> 'start' is available
        // - Bit 1 set -> 'stop' is available
        // - Bit 2 set -> 'step' is available
        let mut idx_num = 0;
        let mut positions = [None; 4]; // maps column index to constraint position
        let mut start_exists = false;
        let mut usable = true;

        for (i, c) in constraints.iter().enumerate() {
            if c.column_index == START_COLUMN_INDEX && c.op == ConstraintOp::Eq {
                start_exists = true;
            }
            if c.column_index >= START_COLUMN_INDEX && c.column_index <= STEP_COLUMN_INDEX {
                if !c.usable {
                    usable = false;
                } else if c.op == ConstraintOp::Eq {
                    let bit = 1 << (c.column_index - 1);
                    idx_num |= bit;
                    positions[c.column_index as usize] = Some(i);
                }
            }
        }

        if !start_exists {
            return Err(ResultCode::InvalidArgs);
        }
        if !usable {
            return Err(ResultCode::ConstraintViolation);
        }

        // Assign argv indexes contiguously
        let mut argv_idx = 1;
        let mut argv_indexes = [None; 4];

        for (i, pos) in positions.iter().enumerate() {
            if pos.is_some() {
                argv_indexes[i] = Some(argv_idx);
                argv_idx += 1;
            }
        }

        let constraint_usages = constraints
            .iter()
            .enumerate()
            .map(|(idx, c)| {
                let argv_index = positions.get(c.column_index as usize).and_then(|&pos| {
                    pos.filter(|&i| i == idx)
                        .and_then(|_| argv_indexes[c.column_index as usize])
                });

                ConstraintUsage {
                    argv_index,
                    omit: argv_index.is_some(),
                }
            })
            .collect();

        Ok(IndexInfo {
            idx_num,
            idx_str: Some(idx_num.to_string()),
            constraint_usages,
            ..Default::default()
        })
    }
}

/// The cursor for iterating over the generated sequence
#[derive(Debug)]
struct GenerateSeriesCursor {
    start: i64,
    stop: i64,
    step: i64,
    current: i64,
}

impl GenerateSeriesCursor {
    /// Returns true if this is an ascending series (positive step) but start > stop
    fn is_invalid_ascending_series(&self) -> bool {
        self.step > 0 && self.start > self.stop
    }

    /// Returns true if this is a descending series (negative step) but start < stop
    fn is_invalid_descending_series(&self) -> bool {
        self.step < 0 && self.start < self.stop
    }

    /// Returns true if this is an invalid range that should produce an empty series
    fn is_invalid_range(&self) -> bool {
        self.is_invalid_ascending_series() || self.is_invalid_descending_series()
    }

    /// Returns true if we would exceed the stop value in the current direction
    fn would_exceed(&self) -> bool {
        (self.step > 0 && self.current.saturating_add(self.step) > self.stop)
            || (self.step < 0 && self.current.saturating_add(self.step) < self.stop)
    }
}

impl VTabCursor for GenerateSeriesCursor {
    type Error = ResultCode;

    fn filter(&mut self, args: &[Value], idx_info: Option<(&str, i32)>) -> ResultCode {
        // SQLite returns no rows when any selected constraint has a NULL value.
        if args.iter().any(|arg| arg.value_type() == ValueType::Null) {
            return ResultCode::EOF;
        }
        let mut start: Option<i64> = None;
        let mut stop: Option<i64> = None;
        let mut step = 1;
        // SQLite default for stop when it is omitted
        const DEFAULT_STOP_OMITTED: Option<i64> = Some(u32::MAX as i64);

        if let Some((_, idx_num)) = idx_info {
            let mut arg_idx = 0;
            // For the semantics of `idx_num`, see the comment in the `best_index` method.
            if idx_num & 1 != 0 {
                start = extract_arg_integer!(args, arg_idx);
                arg_idx += 1;
            }
            if idx_num & 2 != 0 {
                stop = extract_arg_integer!(args, arg_idx);
                arg_idx += 1;
            } else {
                stop = DEFAULT_STOP_OMITTED;
            }
            if idx_num & 4 != 0 {
                step = args
                    .get(arg_idx)
                    .map(|v| v.to_integer().unwrap_or(1))
                    .unwrap_or(1);
            }
        }

        if start.is_none() {
            return ResultCode::InvalidArgs;
        }
        if stop.is_none() {
            return ResultCode::EOF; // Sqlite returns an empty series for wacky args
        }

        // Convert zero step to 1, matching SQLite behavior
        if step == 0 {
            step = 1;
        }

        self.start = start.unwrap();
        self.step = step;
        self.stop = stop.unwrap();

        // Set initial value based on range validity
        // For invalid input SQLite returns an empty series
        self.current = if self.is_invalid_range() {
            return ResultCode::EOF;
        } else {
            self.start
        };

        ResultCode::OK
    }

    fn next(&mut self) -> ResultCode {
        if self.eof() {
            return ResultCode::EOF;
        }

        self.current = match self.current.checked_add(self.step) {
            Some(val) => val,
            None => {
                return ResultCode::EOF;
            }
        };

        ResultCode::OK
    }

    fn eof(&self) -> bool {
        // Check for invalid ranges (empty series) first
        if self.is_invalid_range() {
            return true;
        }

        // Check if we would exceed the stop value in the current direction
        if self.would_exceed() {
            return true;
        }

        if self.current == i64::MAX && self.step > 0 {
            return true;
        }

        if self.current == i64::MIN && self.step < 0 {
            return true;
        }

        false
    }

    fn column(&self, idx: u32) -> Result<Value, Self::Error> {
        Ok(match idx {
            0 => Value::from_integer(self.current),
            1 => Value::from_integer(self.start),
            2 => Value::from_integer(self.stop),
            3 => Value::from_integer(self.step),
            _ => Value::null(),
        })
    }

    fn rowid(&self) -> i64 {
        // SQLite uses each generated value as its rowid. Thus, a value keeps
        // the same identity across scans that use different arguments.
        self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    #[derive(Debug, Clone)]
    struct Series {
        start: i64,
        stop: i64,
        step: i64,
    }

    impl Arbitrary for Series {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut start = i64::arbitrary(g);
            let mut stop = i64::arbitrary(g);
            let mut iters = 0;
            while stop.checked_sub(start).is_none() {
                start = i64::arbitrary(g);
                stop = i64::arbitrary(g);
                iters += 1;
                if iters > 1000 {
                    panic!("Failed to generate valid range after 1000 attempts");
                }
            }
            // step should be a reasonable value proportional to the range
            let mut divisor = i8::arbitrary(g);
            if divisor == 0 {
                divisor = 1;
            }
            let step = (stop - start).saturating_abs() / divisor as i64;
            Series { start, stop, step }
        }
    }
    // Helper function to collect all values from a cursor, returns Result with error code
    fn collect_series(series: Series) -> Result<Vec<i64>, ResultCode> {
        let tbl = GenerateSeriesTable {};
        let mut cursor = tbl.open(None)?;

        // Create args array for filter
        let args = vec![
            Value::from_integer(series.start),
            Value::from_integer(series.stop),
            Value::from_integer(series.step),
        ];

        // Initialize cursor through filter
        match cursor.filter(&args, Some(("idx", 1 | 2 | 4))) {
            ResultCode::OK => (),
            ResultCode::EOF => return Ok(vec![]),
            err => return Err(err),
        }

        let mut values = Vec::new();
        loop {
            values.push(cursor.column(0)?.to_integer().unwrap());
            if values.len() > 1000 {
                panic!(
                    "Generated more than 1000 values, expected this many: {:?}",
                    (series.stop - series.start) / series.step + 1
                );
            }
            match cursor.next() {
                ResultCode::OK => (),
                ResultCode::EOF => break,
                err => return Err(err),
            }
        }
        Ok(values)
    }

    #[quickcheck]
    /// Test that the series length is correct
    /// Example:
    /// start = 1, stop = 10, step = 1
    /// expected length = 10
    fn prop_series_length(series: Series) {
        let start = series.start;
        let stop = series.stop;
        let step = series.step;
        let values = collect_series(series.clone()).unwrap_or_else(|e| {
            panic!("Failed to generate series for start={start}, stop={stop}, step={step}: {e:?}")
        });

        if series_is_invalid_or_empty(&series) {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={start}, stop={stop}, step={step}, got {values:?}"
            );
        } else {
            let expected_len = series_expected_length(&series);
            assert_eq!(
                values.len(),
                expected_len,
                "Series length mismatch for start={}, stop={}, step={}: expected {}, got {}, values: {:?}",
                start,
                stop,
                step,
                expected_len,
                values.len(),
                values
            );
        }
    }

    #[quickcheck]
    /// Test that the series is monotonically increasing
    /// Example:
    /// start = 1, stop = 10, step = 1
    /// expected series = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    fn prop_series_monotonic_increasing_or_decreasing(series: Series) {
        let start = series.start;
        let stop = series.stop;
        let step = series.step;

        let values = collect_series(series.clone()).unwrap_or_else(|e| {
            panic!("Failed to generate series for start={start}, stop={stop}, step={step}: {e:?}")
        });

        if series_is_invalid_or_empty(&series) {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={start}, stop={stop}, step={step}"
            );
        } else {
            assert!(
                values
                    .windows(2)
                    .all(|w| if step > 0 { w[0] < w[1] } else { w[0] > w[1] }),
                "Series not monotonically {}: {:?} (start={}, stop={}, step={})",
                if step > 0 { "increasing" } else { "decreasing" },
                values,
                start,
                stop,
                step
            );
        }
    }

    #[quickcheck]
    /// Test that the series step size is consistent
    /// Example:
    /// start = 1, stop = 10, step = 1
    /// expected step size = 1
    fn prop_series_step_size(series: Series) {
        let start = series.start;
        let stop = series.stop;
        let step = series.step;

        let values = collect_series(series.clone()).unwrap_or_else(|e| {
            panic!("Failed to generate series for start={start}, stop={stop}, step={step}: {e:?}")
        });

        if series_is_invalid_or_empty(&series) {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={start}, stop={stop}, step={step}"
            );
        } else if !values.is_empty() {
            assert!(
                values
                    .windows(2)
                    .all(|w| (w[1].saturating_sub(w[0])).abs() == step.abs()),
                "Step size not consistent: {:?} (expected step size: {})",
                values
                    .windows(2)
                    .map(|w| w[1].saturating_sub(w[0]))
                    .collect::<Vec<_>>(),
                step.abs()
            );
        }
    }

    #[quickcheck]
    /// Test that the series bounds are correct
    /// Example:
    /// start = 1, stop = 10, step = 1
    /// expected bounds = [1, 10]
    fn prop_series_bounds(series: Series) {
        let start = series.start;
        let stop = series.stop;
        let step = series.step;

        let values = collect_series(series.clone()).unwrap_or_else(|e| {
            panic!("Failed to generate series for start={start}, stop={stop}, step={step}: {e:?}")
        });

        if series_is_invalid_or_empty(&series) {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={start}, stop={stop}, step={step}"
            );
        } else if !values.is_empty() {
            assert_eq!(
                values.first(),
                Some(&start),
                "Series doesn't start with start value: {values:?} (expected start: {start})"
            );
            assert!(
                values.last().is_none_or(|&last| if step > 0 {
                    last <= stop
                } else {
                    last >= stop
                }),
                "Series exceeds stop value: {values:?} (stop: {stop})"
            );
        }
    }

    #[test]

    fn test_series_empty_positive_step() {
        let values = collect_series(Series {
            start: 10,
            stop: 5,
            step: 1,
        })
        .expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Series should be empty when start > stop with positive step"
        );
    }

    #[test]
    fn test_series_empty_negative_step() {
        let values = collect_series(Series {
            start: 5,
            stop: 10,
            step: -1,
        })
        .expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Series should be empty when start < stop with negative step"
        );
    }

    #[test]
    fn test_series_single_element() {
        let values = collect_series(Series {
            start: 5,
            stop: 5,
            step: 1,
        })
        .expect("Failed to generate single element series");
        assert_eq!(
            values,
            vec![5],
            "Single element series should contain only the start value"
        );
    }

    #[test]
    fn test_zero_step_is_interpreted_as_1() {
        let values = collect_series(Series {
            start: 1,
            stop: 10,
            step: 0,
        })
        .expect("Failed to generate series");
        assert_eq!(
            values,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "Zero step should be interpreted as 1"
        );
    }

    #[test]
    fn test_invalid_inputs() {
        // Test that invalid ranges return empty series instead of errors
        let values = collect_series(Series {
            start: 10,
            stop: 1,
            step: 1,
        })
        .expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Invalid positive range should return empty series, got {values:?}"
        );

        let values = collect_series(Series {
            start: 1,
            stop: 10,
            step: -1,
        })
        .expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Invalid negative range should return empty series"
        );

        // Test that extreme ranges return empty series
        let values = collect_series(Series {
            start: i64::MAX,
            stop: i64::MIN,
            step: 1,
        })
        .expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Extreme range (MAX to MIN) should return empty series"
        );

        let values = collect_series(Series {
            start: i64::MIN,
            stop: i64::MAX,
            step: -1,
        })
        .expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Extreme range (MIN to MAX) should return empty series"
        );
    }

    #[quickcheck]
    /// Test that each rowid is the generated value.
    fn prop_series_rowid_is_value(series: Series) {
        let start = series.start;
        let stop = series.stop;
        let step = series.step;
        let tbl = GenerateSeriesTable {};
        let mut cursor = tbl.open(None).unwrap();

        let args = vec![
            Value::from_integer(start),
            Value::from_integer(stop),
            Value::from_integer(step),
        ];

        // Initialize cursor through filter
        cursor.filter(&args, Some(("idx", 1 | 2 | 4)));

        while !cursor.eof() {
            assert_eq!(
                cursor.rowid(),
                cursor.current,
                "rowid differs from value for start={start}, stop={stop}, step={step}"
            );
            match cursor.next() {
                ResultCode::OK => {}
                ResultCode::EOF => break,
                err => {
                    panic!("Unexpected error {err:?} for start={start}, stop={stop}, step={step}")
                }
            }
        }
    }

    #[quickcheck]
    /// Test that empty series are handled consistently
    fn prop_series_empty(series: Series) {
        let start = series.start;
        let stop = series.stop;
        let step = series.step;

        let values = collect_series(series.clone()).unwrap_or_else(|e| {
            panic!("Failed to generate series for start={start}, stop={stop}, step={step}: {e:?}")
        });

        if series_is_invalid_or_empty(&series) {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={start}, stop={stop}, step={step}"
            );
        } else if start == stop {
            assert_eq!(
                values,
                vec![start],
                "Series with start==stop should contain exactly one element"
            );
        }
    }

    fn series_is_invalid_or_empty(series: &Series) -> bool {
        let start = series.start;
        let stop = series.stop;
        let step = series.step;
        (start > stop && step > 0) || (start < stop && step < 0) || (step == 0 && start != stop)
    }

    fn series_expected_length(series: &Series) -> usize {
        let start = series.start;
        let stop = series.stop;
        let step = series.step;
        if step == 0 {
            if start == stop {
                1
            } else {
                0
            }
        } else {
            ((stop.saturating_sub(start)).saturating_div(step)).saturating_add(1) as usize
        }
    }

    #[test]
    fn test_best_index_argv_order_all_constraints() {
        // Test when start, stop, and step constraints are present
        let constraints = vec![
            usable_constraint(1), // start
            usable_constraint(2), // stop
            usable_constraint(3), // step
        ];

        let index_info = GenerateSeriesTable::best_index(&constraints, &[]).unwrap();

        // Verify start gets argv_index 1, stop gets 2, step gets 3
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // start
        assert_eq!(index_info.constraint_usages[1].argv_index, Some(2)); // stop
        assert_eq!(index_info.constraint_usages[2].argv_index, Some(3)); // step
        assert_eq!(index_info.idx_num, 7); // All bits set (1 | 2 | 4)
    }

    #[test]
    fn test_best_index_argv_order_start_stop_only() {
        let constraints = vec![
            usable_constraint(1), // start
            usable_constraint(2), // stop
        ];

        let index_info = GenerateSeriesTable::best_index(&constraints, &[]).unwrap();

        // Verify start gets argv_index 1, stop gets 2
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // start
        assert_eq!(index_info.constraint_usages[1].argv_index, Some(2)); // stop
        assert_eq!(index_info.idx_num, 3); // Bits 0 and 1 set (1 | 2)
    }

    #[test]
    fn test_best_index_argv_order_only_start() {
        let constraints = vec![
            usable_constraint(1), // start
        ];

        let index_info = GenerateSeriesTable::best_index(&constraints, &[]).unwrap();

        // Verify start gets argv_index 1
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // start
        assert_eq!(index_info.idx_num, 1); // Only bit 0 set
    }

    #[test]
    fn test_best_index_argv_order_reverse_constraint_order() {
        // Test when constraints are provided in reverse order (step, stop, start)
        let constraints = vec![
            usable_constraint(3), // step
            usable_constraint(2), // stop
            usable_constraint(1), // start
        ];

        let index_info = GenerateSeriesTable::best_index(&constraints, &[]).unwrap();

        // Verify start still gets argv_index 1, stop gets 2, step gets 3 regardless of constraint order
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(3)); // step
        assert_eq!(index_info.constraint_usages[1].argv_index, Some(2)); // stop
        assert_eq!(index_info.constraint_usages[2].argv_index, Some(1)); // start
        assert_eq!(index_info.idx_num, 7); // All bits set (1 | 2 | 4)
    }

    #[test]
    fn test_best_index_argv_order_missing_start() {
        // Test when start constraint is missing but stop and step are present
        let constraints = vec![
            usable_constraint(2), // stop
            usable_constraint(3), // step
        ];

        let result = GenerateSeriesTable::best_index(&constraints, &[]);

        assert!(matches!(result, Err(ResultCode::InvalidArgs)));
    }

    #[test]
    fn test_best_index_no_usable_constraints() {
        let constraints = vec![ConstraintInfo {
            column_index: 1,
            op: ConstraintOp::Eq,
            usable: false,
            index: 0,
        }];

        let result = GenerateSeriesTable::best_index(&constraints, &[]);

        assert!(matches!(result, Err(ResultCode::ConstraintViolation)));
    }

    fn usable_constraint(column_index: u32) -> ConstraintInfo {
        ConstraintInfo {
            column_index,
            op: ConstraintOp::Eq,
            usable: true,
            index: 0,
        }
    }
}
