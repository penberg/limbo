use limbo_ext::{
    register_extension, ExtensionApi, ResultCode, VTabCursor, VTabModule, VTabModuleDerive, Value,
};

register_extension! {
    vtabs: { GenerateSeriesVTab }
}

macro_rules! try_option {
    ($expr:expr, $err:expr) => {
        match $expr {
            Some(val) => val,
            None => return $err,
        }
    };
}

/// A virtual table that generates a sequence of integers
#[derive(Debug, VTabModuleDerive)]
struct GenerateSeriesVTab;

impl VTabModule for GenerateSeriesVTab {
    type VCursor = GenerateSeriesCursor;
    const NAME: &'static str = "generate_series";

    fn connect(api: &ExtensionApi) -> ResultCode {
        // Create table schema
        let sql = "CREATE TABLE generate_series(
            value INTEGER,
            start INTEGER HIDDEN,
            stop INTEGER HIDDEN,
            step INTEGER HIDDEN
        )";
        api.declare_virtual_table(Self::NAME, sql)
    }

    fn open() -> Self::VCursor {
        GenerateSeriesCursor {
            start: 0,
            stop: 0,
            step: 0,
            current: 0,
        }
    }

    fn filter(cursor: &mut Self::VCursor, arg_count: i32, args: &[Value]) -> ResultCode {
        // args are the start, stop, and step
        if arg_count == 0 || arg_count > 3 {
            return ResultCode::InvalidArgs;
        }
        let start = try_option!(args[0].to_integer(), ResultCode::InvalidArgs);
        let stop = try_option!(
            args.get(1).map(|v| v.to_integer().unwrap_or(i64::MAX)),
            ResultCode::EOF // Sqlite returns an empty series for wacky args
        );
        let mut step = args
            .get(2)
            .map(|v| v.to_integer().unwrap_or(1))
            .unwrap_or(1);

        // Convert zero step to 1, matching SQLite behavior
        if step == 0 {
            step = 1;
        }

        cursor.start = start;
        cursor.step = step;
        cursor.stop = stop;

        // Set initial value based on range validity
        // For invalid input SQLite returns an empty series
        cursor.current = if cursor.is_invalid_range() {
            return ResultCode::EOF;
        } else {
            start
        };

        ResultCode::OK
    }

    fn column(cursor: &Self::VCursor, idx: u32) -> Value {
        cursor.column(idx)
    }

    fn next(cursor: &mut Self::VCursor) -> ResultCode {
        // Check for invalid ranges (empty series) first
        if cursor.is_invalid_range() {
            return ResultCode::EOF;
        }

        // Check if we've reached the end of the sequence
        if cursor.would_exceed() {
            return ResultCode::EOF;
        }

        // Handle overflow by truncating to MAX/MIN
        cursor.current = match cursor.step {
            step if step > 0 => match cursor.current.checked_add(step) {
                Some(val) => val,
                None => i64::MAX,
            },
            step => match cursor.current.checked_add(step) {
                Some(val) => val,
                None => i64::MIN,
            },
        };

        ResultCode::OK
    }

    fn eof(cursor: &Self::VCursor) -> bool {
        cursor.is_invalid_range() || cursor.would_exceed()
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
        (self.step > 0 && self.current + self.step > self.stop)
            || (self.step < 0 && self.current + self.step < self.stop)
    }
}

impl VTabCursor for GenerateSeriesCursor {
    type Error = ResultCode;

    fn next(&mut self) -> ResultCode {
        // Check for invalid ranges (empty series) first
        if self.eof() {
            return ResultCode::EOF;
        }

        // Handle overflow by truncating to MAX/MIN
        self.current = self.current.saturating_add(self.step);

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

        false
    }

    fn column(&self, idx: u32) -> Value {
        match idx {
            0 => Value::from_integer(self.current),
            1 => Value::from_integer(self.start),
            2 => Value::from_integer(self.stop),
            3 => Value::from_integer(self.step),
            _ => Value::null(),
        }
    }

    fn rowid(&self) -> i64 {
        let sub = self.current.saturating_sub(self.start);

        // Handle overflow in rowid calculation by capping at MAX/MIN
        match sub.checked_div(self.step) {
            Some(val) => val.saturating_add(1),
            None => {
                if self.step > 0 {
                    i64::MAX
                } else {
                    i64::MIN
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    // Helper function to collect all values from a cursor, returns Result with error code
    fn collect_series(start: i64, stop: i64, step: i64) -> Result<Vec<i64>, ResultCode> {
        let mut cursor = GenerateSeriesCursor {
            start: 0,
            stop: 0,
            step: 0,
            current: 0,
        };

        // Create args array for filter
        let args = vec![
            Value::from_integer(start),
            Value::from_integer(stop),
            Value::from_integer(step),
        ];

        // Validate inputs through filter
        match GenerateSeriesVTab::filter(&mut cursor, 3, &args) {
            ResultCode::OK => (),
            ResultCode::EOF => return Ok(vec![]),
            err => return Err(err),
        }

        let mut values = Vec::new();
        loop {
            values.push(cursor.column(0).to_integer().unwrap());
            match GenerateSeriesVTab::next(&mut cursor) {
                ResultCode::OK => (),
                ResultCode::EOF => break,
                err => return Err(err),
            }
        }
        Ok(values)
    }

    #[quickcheck]
    /// Test that the series length is correct for a positive step
    /// Example:
    /// start = 1, stop = 10, step = 1
    /// expected length = 10
    fn prop_series_length_positive_step(start: i64, stop: i64) {
        // Limit the range to make test run faster, since we're testing with step 1.
        let start = start % 1000;
        let stop = stop % 1000;
        let step = 1;

        let values = collect_series(start, stop, step).unwrap_or_else(|e| {
            panic!(
                "Failed to generate series for start={}, stop={}, step={}: {:?}",
                start, stop, step, e
            )
        });

        if start > stop {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={}, stop={}, step={}, got {:?}",
                start,
                stop,
                step,
                values
            );
        } else {
            let expected_len = ((stop - start) / step + 1) as usize;
            assert_eq!(
                values.len(),
                expected_len,
                "Series length mismatch for start={}, stop={}, step={}: expected {}, got {}",
                start,
                stop,
                step,
                expected_len,
                values.len()
            );
        }
    }

    #[quickcheck]
    /// Test that the series length is correct for a negative step
    /// Example:
    /// start = 10, stop = 1, step = -1
    /// expected length = 10
    fn prop_series_length_negative_step(start: i64, stop: i64) {
        // Limit the range to make test run faster, since we're testing with step -1.
        let start = start % 1000;
        let stop = stop % 1000;
        let step = -1;

        let values = collect_series(start, stop, step).unwrap_or_else(|e| {
            panic!(
                "Failed to generate series for start={}, stop={}, step={}: {:?}",
                start, stop, step, e
            )
        });

        if start < stop {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={}, stop={}, step={}",
                start,
                stop,
                step
            );
        } else {
            let expected_len = ((start - stop) / step.abs() + 1) as usize;
            assert_eq!(
                values.len(),
                expected_len,
                "Series length mismatch for start={}, stop={}, step={}: expected {}, got {}",
                start,
                stop,
                step,
                expected_len,
                values.len()
            );
        }
    }

    #[quickcheck]
    /// Test that the series is monotonically increasing
    /// Example:
    /// start = 1, stop = 10, step = 1
    /// expected series = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    fn prop_series_monotonic_increasing(start: i64, stop: i64, step: i64) {
        // Limit the range to make test run faster.
        let start = start % 10000;
        let stop = stop % 10000;
        let step = (step % 100).max(1); // Ensure positive non-zero step

        let values = collect_series(start, stop, step).unwrap_or_else(|e| {
            panic!(
                "Failed to generate series for start={}, stop={}, step={}: {:?}",
                start, stop, step, e
            )
        });

        if start > stop {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={}, stop={}, step={}",
                start,
                stop,
                step
            );
        } else {
            assert!(
                values.windows(2).all(|w| w[0] < w[1]),
                "Series not monotonically increasing: {:?} (start={}, stop={}, step={})",
                values,
                start,
                stop,
                step
            );
        }
    }

    #[quickcheck]
    /// Test that the series is monotonically decreasing
    /// Example:
    /// start = 10, stop = 1, step = -1
    /// expected series = [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    fn prop_series_monotonic_decreasing(start: i64, stop: i64, step: i64) {
        // Limit the range to make test run faster.
        let start = start % 10000;
        let stop = stop % 10000;
        let step = -((step % 100).max(1)); // Ensure negative non-zero step

        let values = collect_series(start, stop, step).unwrap_or_else(|e| {
            panic!(
                "Failed to generate series for start={}, stop={}, step={}: {:?}",
                start, stop, step, e
            )
        });

        if start < stop {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={}, stop={}, step={}",
                start,
                stop,
                step
            );
        } else {
            assert!(
                values.windows(2).all(|w| w[0] > w[1]),
                "Series not monotonically decreasing: {:?} (start={}, stop={}, step={})",
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
    fn prop_series_step_size_positive(start: i64, stop: i64, step: i64) {
        // Limit the range to make test run faster.
        let start = start % 1000;
        let stop = stop % 1000;
        let step = (step % 100).max(1); // Ensure positive non-zero step

        let values = collect_series(start, stop, step).unwrap_or_else(|e| {
            panic!(
                "Failed to generate series for start={}, stop={}, step={}: {:?}",
                start, stop, step, e
            )
        });

        if start > stop {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={}, stop={}, step={}",
                start,
                stop,
                step
            );
        } else if !values.is_empty() {
            assert!(
                values.windows(2).all(|w| (w[1] - w[0]).abs() == step.abs()),
                "Step size not consistent: {:?} (expected step size: {})",
                values,
                step.abs()
            );
        }
    }

    #[quickcheck]
    /// Test that the series step size is consistent for a negative step
    /// Example:
    /// start = 10, stop = 1, step = -1
    /// expected step size = 1
    fn prop_series_step_size_negative(start: i64, stop: i64, step: i64) {
        // Limit the range to make test run faster.
        let start = start % 1000;
        let stop = stop % 1000;
        let step = -((step % 100).max(1)); // Ensure negative non-zero step

        let values = collect_series(start, stop, step).unwrap_or_else(|e| {
            panic!(
                "Failed to generate series for start={}, stop={}, step={}: {:?}",
                start, stop, step, e
            )
        });

        if start < stop {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={}, stop={}, step={}",
                start,
                stop,
                step
            );
        } else if !values.is_empty() {
            assert!(
                values.windows(2).all(|w| (w[1] - w[0]).abs() == step.abs()),
                "Step size not consistent: {:?} (expected step size: {})",
                values,
                step.abs()
            );
        }
    }

    #[quickcheck]
    /// Test that the series bounds are correct for a positive step
    /// Example:
    /// start = 1, stop = 10, step = 1
    /// expected bounds = [1, 10]
    fn prop_series_bounds_positive(start: i64, stop: i64, step: i64) {
        // Limit the range to make test run faster.
        let start = start % 10000;
        let stop = stop % 10000;
        let step = (step % 100).max(1); // Ensure positive non-zero step

        let values = collect_series(start, stop, step).unwrap_or_else(|e| {
            panic!(
                "Failed to generate series for start={}, stop={}, step={}: {:?}",
                start, stop, step, e
            )
        });

        if start > stop {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={}, stop={}, step={}",
                start,
                stop,
                step
            );
        } else if !values.is_empty() {
            assert_eq!(
                values.first(),
                Some(&start),
                "Series doesn't start with start value: {:?} (expected start: {})",
                values,
                start
            );
            assert!(
                values.last().map_or(true, |&last| last <= stop),
                "Series exceeds stop value: {:?} (stop: {})",
                values,
                stop
            );
        }
    }

    #[quickcheck]
    /// Test that the series bounds are correct for a negative step
    /// Example:
    /// start = 10, stop = 1, step = -1
    /// expected bounds = [10, 1]
    fn prop_series_bounds_negative(start: i64, stop: i64, step: i64) {
        // Limit the range to make test run faster.
        let start = start % 10000;
        let stop = stop % 10000;
        let step = -((step % 100).max(1)); // Ensure negative non-zero step

        let values = collect_series(start, stop, step).unwrap_or_else(|e| {
            panic!(
                "Failed to generate series for start={}, stop={}, step={}: {:?}",
                start, stop, step, e
            )
        });

        if start < stop {
            assert!(
                values.is_empty(),
                "Series should be empty for invalid range: start={}, stop={}, step={}",
                start,
                stop,
                step
            );
        } else if !values.is_empty() {
            assert_eq!(
                values.first(),
                Some(&start),
                "Series doesn't start with start value: {:?} (expected start: {})",
                values,
                start
            );
            assert!(
                values.last().map_or(true, |&last| last >= stop),
                "Series exceeds stop value: {:?} (stop: {})",
                values,
                stop
            );
        }
    }

    #[test]

    fn test_series_empty_positive_step() {
        let values = collect_series(10, 5, 1).expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Series should be empty when start > stop with positive step"
        );
    }

    #[test]
    fn test_series_empty_negative_step() {
        let values = collect_series(5, 10, -1).expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Series should be empty when start < stop with negative step"
        );
    }

    #[test]
    fn test_series_single_element() {
        let values = collect_series(5, 5, 1).expect("Failed to generate single element series");
        assert_eq!(
            values,
            vec![5],
            "Single element series should contain only the start value"
        );
    }

    #[test]
    fn test_zero_step_is_interpreted_as_1() {
        let values = collect_series(1, 10, 0).expect("Failed to generate series");
        assert_eq!(
            values,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "Zero step should be interpreted as 1"
        );
    }

    #[test]
    fn test_invalid_inputs() {
        // Test that invalid ranges return empty series instead of errors
        let values = collect_series(10, 1, 1).expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Invalid positive range should return empty series, got {:?}",
            values
        );

        let values = collect_series(1, 10, -1).expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Invalid negative range should return empty series"
        );

        // Test that extreme ranges return empty series
        let values = collect_series(i64::MAX, i64::MIN, 1).expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Extreme range (MAX to MIN) should return empty series"
        );

        let values = collect_series(i64::MIN, i64::MAX, -1).expect("Failed to generate series");
        assert!(
            values.is_empty(),
            "Extreme range (MIN to MAX) should return empty series"
        );
    }

    #[quickcheck]
    /// Test that rowid is always monotonically increasing regardless of step direction
    fn prop_series_rowid_monotonic(start: i64, stop: i64, step: i64) {
        // Limit the range to make test run faster
        let start = start % 1000;
        let stop = stop % 1000;
        let step = if step == 0 { 1 } else { step % 100 }; // Ensure non-zero step

        let mut cursor = GenerateSeriesCursor {
            start: 0,
            stop: 0,
            step: 0,
            current: 0,
        };

        let args = vec![
            Value::from_integer(start),
            Value::from_integer(stop),
            Value::from_integer(step),
        ];

        // Initialize cursor through filter
        GenerateSeriesVTab::filter(&mut cursor, 3, &args);

        let mut rowids = vec![];
        while !GenerateSeriesVTab::eof(&cursor) {
            let cur_rowid = cursor.rowid();
            match GenerateSeriesVTab::next(&mut cursor) {
                ResultCode::OK => rowids.push(cur_rowid),
                ResultCode::EOF => break,
                err => panic!(
                    "Unexpected error {:?} for start={}, stop={}, step={}",
                    err, start, stop, step
                ),
            }
        }

        assert!(
            rowids.windows(2).all(|w| w[1] == w[0] + 1),
            "Rowids not monotonically increasing: {:?} (start={}, stop={}, step={})",
            rowids,
            start,
            stop,
            step
        );
    }

    // #[quickcheck]
    // /// Test that integer overflow is properly handled
    // fn prop_series_overflow(start: i64, step: i64) {
    //     // Use values close to MAX/MIN to test overflow
    //     let start = if start >= 0 {
    //         i64::MAX - (start % 100)
    //     } else {
    //         i64::MIN + (start.abs() % 100)
    //     };
    //     let step = if step == 0 { 1 } else { step }; // Ensure non-zero step
    //     let stop = if step > 0 { i64::MAX } else { i64::MIN };

    //     let result = collect_series(start, stop, step);

    //     // If we would overflow, expect InvalidArgs
    //     if start.checked_add(step).is_none() {
    //         assert!(
    //             matches!(result, Err(ResultCode::InvalidArgs)),
    //             "Expected InvalidArgs for overflow case: start={}, stop={}, step={}",
    //             start, stop, step
    //         );
    //     } else {
    //         // Otherwise the series should be valid
    //         let values = result.unwrap_or_else(|e| {
    //             panic!(
    //                 "Failed to generate series for start={}, stop={}, step={}: {:?}",
    //                 start, stop, step, e
    //             )
    //         });

    //         // Verify no values overflow
    //         for window in values.windows(2) {
    //             assert!(
    //                 window[0].checked_add(step).is_some(),
    //                 "Overflow occurred in series: {:?} + {} (start={}, stop={}, step={})",
    //                 window[0], step, start, stop, step
    //             );
    //         }
    //     }
    // }

    #[quickcheck]
    /// Test that empty series are handled consistently
    fn prop_series_empty(start: i64, stop: i64, step: i64) {
        // Limit the range to make test run faster
        let start = start % 1000;
        let stop = stop % 1000;
        let step = if step == 0 { 1 } else { step % 100 }; // Ensure non-zero step

        let result = collect_series(start, stop, step);

        match result {
            Ok(values) => {
                if (step > 0 && start > stop) || (step < 0 && start < stop) {
                    assert!(
                        values.is_empty(),
                        "Series should be empty for invalid range: start={}, stop={}, step={}",
                        start,
                        stop,
                        step
                    );
                } else if start == stop {
                    assert_eq!(
                        values,
                        vec![start],
                        "Series with start==stop should contain exactly one element"
                    );
                }
            }
            Err(e) => panic!(
                "Unexpected error for start={}, stop={}, step={}: {:?}",
                start, stop, step, e
            ),
        }
    }
}
