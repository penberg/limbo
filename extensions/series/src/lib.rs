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
            ResultCode::InvalidArgs
        );
        let step = try_option!(
            args.get(2).map(|v| v.to_integer().unwrap_or(1)),
            ResultCode::InvalidArgs
        );
        cursor.start = start;
        cursor.current = start;
        cursor.step = step;
        cursor.stop = stop;
        ResultCode::OK
    }

    fn column(cursor: &Self::VCursor, idx: u32) -> Value {
        cursor.column(idx)
    }

    fn next(cursor: &mut Self::VCursor) -> ResultCode {
        GenerateSeriesCursor::next(cursor)
    }

    fn eof(cursor: &Self::VCursor) -> bool {
        cursor.eof()
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
    fn next(&mut self) -> ResultCode {
        let current = self.current;

        // Check if we've reached the end
        if (self.step > 0 && current >= self.stop) || (self.step < 0 && current <= self.stop) {
            return ResultCode::EOF;
        }

        self.current = current.saturating_add(self.step);
        ResultCode::OK
    }
}

impl VTabCursor for GenerateSeriesCursor {
    fn next(&mut self) -> ResultCode {
        GenerateSeriesCursor::next(self)
    }

    fn eof(&self) -> bool {
        (self.step > 0 && self.current > self.stop) || (self.step < 0 && self.current < self.stop)
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
        ((self.current - self.start) / self.step) + 1
    }
}
