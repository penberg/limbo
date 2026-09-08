use crate::storage::pager::Pager;
use crate::sync::Arc;
use crate::sync::RwLock;
use crate::util::IOExt;
use crate::vtab::{InternalVirtualTable, InternalVirtualTableCursor};
use crate::{Connection, LimboError, Result, Value};
use turso_ext::{
    ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, OrderByInfo, ResultCode,
};

pub const DBPAGE_TABLE_NAME: &str = "sqlite_dbpage";

#[derive(Debug)]
pub struct DbPageTable;

impl Default for DbPageTable {
    fn default() -> Self {
        Self::new()
    }
}

impl DbPageTable {
    pub fn new() -> Self {
        Self
    }
}

impl InternalVirtualTable for DbPageTable {
    fn name(&self) -> String {
        DBPAGE_TABLE_NAME.to_string()
    }

    fn sql(&self) -> String {
        "CREATE TABLE sqlite_dbpage(pgno INTEGER PRIMARY KEY, data BLOB, schema HIDDEN)".to_string()
    }

    fn open(&self, conn: Arc<Connection>) -> Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        let pager = conn.get_pager();
        let cursor = DbPageCursor::new(pager);
        Ok(Arc::new(RwLock::new(cursor)))
    }

    /// TODO: sqlite does where_onerow optimization using idx_flag, we should do that eventually.. probably not needed for now.
    /// Analyzes query constraints and returns cost estimates to help pick the best query plan.
    ///
    /// We encode constraint info into `idx_num` as a bitmask, which `filter()` later uses:
    /// - Bit 0 (0x1): equality on `pgno` - enables single-page lookup
    /// - Bit 1 (0x2): equality on `schema` - we only support "main", so we let Turso handle filtering if the user provides a different schema.
    fn best_index(
        &self,
        constraints: &[ConstraintInfo],
        _order_by: &[OrderByInfo],
    ) -> std::result::Result<IndexInfo, ResultCode> {
        let mut idx_num = 0;
        let mut estimated_cost = 1_000_000.0;

        let constraint_usages = constraints
            .iter()
            .map(|constraint| {
                let mut usage = ConstraintUsage {
                    argv_index: None,
                    omit: false,
                };
                if constraint.op == ConstraintOp::Eq {
                    match constraint.column_index {
                        // pgno column
                        0 => {
                            idx_num |= 1;
                            usage.argv_index = Some(1);
                            usage.omit = true;
                            estimated_cost = 1.0;
                        }
                        // schema column
                        2 => {
                            idx_num |= 2;
                            usage.argv_index = Some(if (idx_num & 1) != 0 { 2 } else { 1 });
                            usage.omit = true;
                        }
                        _ => {}
                    }
                }
                usage
            })
            .collect();

        let index_info = IndexInfo {
            idx_num,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost,
            estimated_rows: if (idx_num & 1) != 0 { 1 } else { 1_000_000 },
            constraint_usages,
        };

        Ok(index_info)
    }
}

pub struct DbPageCursor {
    pager: Arc<Pager>,
    pgno: i64,
    mx_pgno: i64,
    /// If true, schema constraint was for non-"main" schema, so return no rows
    schema_mismatch: bool,
}

impl DbPageCursor {
    fn new(pager: Arc<Pager>) -> Self {
        Self {
            pager,
            pgno: 1,
            mx_pgno: 0,
            schema_mismatch: false,
        }
    }
}

impl InternalVirtualTableCursor for DbPageCursor {
    /// iterates based on constraints identified by `best_index()`.
    ///
    /// When `idx_num` has bit 0 set, we do a single-page lookup using `args[0]` as the page number.
    /// If the requested page is out of range (≤0 or beyond db size), the scan returns empty.
    /// Otherwise, we do a full table scan over all pages starting from page 1.
    fn filter(&mut self, args: &[Value], _idx_str: Option<String>, idx_num: i32) -> Result<bool> {
        self.schema_mismatch = false;

        let db_size = self
            .pager
            .io
            .block(|| self.pager.with_header(|header| header.database_size.get()))?;

        self.mx_pgno = db_size as i64;

        let mut arg_idx = 0;

        if (idx_num & 1) != 0 {
            let pgno = if let Some(Value::Numeric(crate::numeric::Numeric::Integer(val))) =
                args.get(arg_idx)
            {
                *val
            } else {
                0
            };
            arg_idx += 1;

            if pgno > 0 && pgno <= self.mx_pgno {
                self.pgno = pgno;
                self.mx_pgno = pgno;
            } else {
                self.mx_pgno = 0;
            }
        } else {
            self.pgno = 1;
        }

        if (idx_num & 2) != 0 {
            if let Some(Value::Text(schema)) = args.get(arg_idx) {
                if schema.as_str() != "main" {
                    self.schema_mismatch = true;
                    return Ok(false);
                }
            }
        }

        Ok(self.pgno <= self.mx_pgno)
    }

    fn next(&mut self) -> Result<bool> {
        if self.schema_mismatch {
            return Ok(false);
        }
        self.pgno += 1;
        Ok(self.pgno <= self.mx_pgno)
    }

    fn column(&self, column: usize) -> Result<Value> {
        match column {
            0 => Ok(Value::from_i64(self.pgno)),
            1 => {
                // check for the pending byte page  - this only needs when db is more than 1 gb.
                if let Some(pending_page) = self.pager.pending_byte_page_id() {
                    if self.pgno == pending_page as i64 {
                        let page_size = self.pager.usable_space()
                            + self.pager.get_reserved_space().unwrap_or(0) as usize;
                        return Ok(Value::from_blob(crate::alloc::vec![0u8; page_size]));
                    }
                }

                let (page_ref, completion) =
                    self.pager.io.block(|| self.pager.read_page(self.pgno))?;
                if let Some(c) = completion {
                    self.pager.io.wait_for_completion(c)?;
                }

                let page_contents = page_ref.get_contents();
                let data_slice = page_contents.as_ptr();
                Ok(Value::from_slice(data_slice)?)
            }
            2 => Ok(Value::from_text("main")), // we don't support multiple databases - todo when we do
            _ => Ok(Value::Null),
        }
    }

    fn rowid(&self) -> i64 {
        self.pgno
    }
}

fn parse_rowid(value: &Value) -> Result<Option<i64>> {
    match value {
        Value::Null => Ok(None),
        Value::Numeric(crate::numeric::Numeric::Integer(i)) => Ok(Some(*i)),
        _ => Err(LimboError::InvalidArgument(
            "sqlite_dbpage rowid must be an integer".to_string(),
        )),
    }
}

fn ensure_main_schema(value: &Value) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Text(schema) if schema.as_str() == "main" => Ok(()),
        Value::Text(schema) => Err(LimboError::InvalidArgument(format!(
            "sqlite_dbpage only supports main schema (got {})",
            schema.as_str()
        ))),
        _ => Err(LimboError::InvalidArgument(
            "sqlite_dbpage schema must be text or null".to_string(),
        )),
    }
}

pub(crate) fn update_dbpage(pager: &Arc<Pager>, args: &[Value]) -> Result<Option<i64>> {
    if args.len() < 2 {
        return Err(LimboError::InternalError(
            "sqlite_dbpage update expects at least 2 arguments".to_string(),
        ));
    }

    let old_rowid = parse_rowid(&args[0])?;
    let new_rowid = parse_rowid(&args[1])?;

    if old_rowid.is_some() && new_rowid.is_none() {
        return Err(LimboError::InvalidArgument(
            "sqlite_dbpage does not support DELETE".to_string(),
        ));
    }

    let columns = if args.len() > 2 { &args[2..] } else { &[] };
    let column_pgno = columns.first().and_then(|value| value.as_int());
    let column_data = columns.get(1);
    let column_schema = columns.get(2);

    let target_pgno = match (new_rowid, old_rowid, column_pgno) {
        (Some(rowid), _, Some(pgno)) if rowid != pgno => {
            return Err(LimboError::InvalidArgument(
                "sqlite_dbpage pgno does not match rowid".to_string(),
            ));
        }
        (Some(rowid), _, _) => rowid,
        (None, Some(rowid), Some(pgno)) if rowid != pgno => {
            return Err(LimboError::InvalidArgument(
                "sqlite_dbpage pgno does not match rowid".to_string(),
            ));
        }
        (None, Some(rowid), _) => rowid,
        (None, None, Some(pgno)) => pgno,
        _ => {
            return Err(LimboError::InvalidArgument(
                "sqlite_dbpage requires a target page number".to_string(),
            ))
        }
    };

    if target_pgno <= 0 {
        return Err(LimboError::InvalidArgument(
            "sqlite_dbpage pgno must be positive".to_string(),
        ));
    }

    if let Some(schema) = column_schema {
        ensure_main_schema(schema)?;
    }

    let data = match column_data {
        Some(Value::Blob(blob)) => blob.as_slice(),
        Some(Value::Null) | None => {
            return Err(LimboError::InvalidArgument(
                "sqlite_dbpage requires data for updates".to_string(),
            ))
        }
        _ => {
            return Err(LimboError::InvalidArgument(
                "sqlite_dbpage data must be a blob".to_string(),
            ))
        }
    };

    let db_size = pager
        .io
        .block(|| pager.with_header(|header| header.database_size.get()))?;
    if target_pgno as u64 > db_size as u64 {
        return Err(LimboError::InvalidArgument(format!(
            "sqlite_dbpage pgno {target_pgno} is out of range"
        )));
    }

    if let Some(pending_page) = pager.pending_byte_page_id() {
        if target_pgno == pending_page as i64 {
            return Err(LimboError::InvalidArgument(
                "sqlite_dbpage cannot write the pending byte page".to_string(),
            ));
        }
    }

    let expected_len = pager.usable_space() + pager.get_reserved_space().unwrap_or(0) as usize;
    if data.len() != expected_len {
        return Err(LimboError::InvalidArgument(format!(
            "sqlite_dbpage data length must be {expected_len} bytes"
        )));
    }

    let (page_ref, completion) = pager.io.block(|| pager.read_page(target_pgno))?;
    if let Some(c) = completion {
        pager.io.wait_for_completion(c)?;
    }

    pager.add_dirty(&page_ref)?;
    let contents = page_ref.get_contents();
    let buffer = contents
        .buffer()
        .expect("sqlite_dbpage page buffer should be loaded");
    buffer.as_mut_slice().copy_from_slice(data);

    let is_insert = old_rowid.is_none();
    Ok(if is_insert { Some(target_pgno) } else { None })
}
