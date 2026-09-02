use crate::sync::Arc;
use crate::{Connection, LimboError, Statement, StepResult, Value};
use bitflags::bitflags;
use strum::IntoEnumIterator;
use turso_ext::{ConstraintInfo, ConstraintOp, ConstraintUsage, IndexInfo, ResultCode};
use turso_parser::ast::PragmaName;

bitflags! {
    // Flag names match those used in SQLite:
    // https://github.com/sqlite/sqlite/blob/b3c1884b65400da85636458298bd77cbbfdfb401/tool/mkpragmatab.tcl#L22-L29
    pub struct PragmaFlags: u8 {
        const NeedSchema = 0x01; /* Force schema load before running */
        const NoColumns  = 0x02; /* OP_ResultRow called with zero columns */
        const NoColumns1 = 0x04; /* zero columns if RHS argument is present */
        const ReadOnly   = 0x08; /* Read-only HEADER_VALUE */
        const Result0    = 0x10; /* Acts as query when no argument */
        const Result1    = 0x20; /* Acts as query when has one argument */
        const SchemaOpt  = 0x40; /* Schema restricts name search if present */
        const SchemaReq  = 0x80; /* Schema required - "main" is default */
    }
}

pub struct Pragma {
    pub flags: PragmaFlags,
    pub columns: &'static [&'static str],
}

impl Pragma {
    const fn new(flags: PragmaFlags, columns: &'static [&'static str]) -> Self {
        Self { flags, columns }
    }
}

pub fn pragma_for(pragma: &PragmaName) -> Pragma {
    use PragmaName::*;

    match pragma {
        ApplicationId => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["application_id"],
        ),
        CacheSize => Pragma::new(
            PragmaFlags::NeedSchema
                | PragmaFlags::Result0
                | PragmaFlags::SchemaReq
                | PragmaFlags::NoColumns1,
            &["cache_size"],
        ),
        CountChanges => Pragma::new(
            PragmaFlags::Result0 | PragmaFlags::NoColumns1,
            &["count_changes"],
        ),
        DataSyncRetry => Pragma::new(
            PragmaFlags::Result0 | PragmaFlags::NoColumns1,
            &["data_sync_retry"],
        ),
        DatabaseList => Pragma::new(PragmaFlags::Result0, &["seq", "name", "file"]),
        Encoding => Pragma::new(
            PragmaFlags::Result0 | PragmaFlags::NoColumns1,
            &["encoding"],
        ),
        JournalMode => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result0 | PragmaFlags::SchemaReq,
            &["journal_mode"],
        ),
        LockingMode => Pragma::new(PragmaFlags::Result0, &["locking_mode"]),
        FullColumnNames | ShortColumnNames => {
            unreachable!("pragma_for() called with FullColumnNames/ShortColumnNames, which are deprecated no-ops")
        }
        LegacyFileFormat => {
            unreachable!("pragma_for() called with LegacyFileFormat, which is unsupported")
        }
        EmptyResultCallbacks => {
            unreachable!("pragma_for() called with EmptyResultCallbacks, which is a no-op")
        }
        ModuleList => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result0 | PragmaFlags::SchemaReq,
            &["module_list"],
        ),
        PageCount => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result0 | PragmaFlags::SchemaReq,
            &["page_count"],
        ),
        PageSize => Pragma::new(
            PragmaFlags::Result0 | PragmaFlags::SchemaReq | PragmaFlags::NoColumns1,
            &["page_size"],
        ),
        MaxPageCount => Pragma::new(
            PragmaFlags::NeedSchema
                | PragmaFlags::Result0
                | PragmaFlags::SchemaReq
                | PragmaFlags::NoColumns1,
            &["max_page_count"],
        ),
        SchemaVersion => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["schema_version"],
        ),
        Synchronous => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["synchronous"],
        ),
        TempStore => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["temp_store"],
        ),
        IndexInfo => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result1 | PragmaFlags::SchemaOpt,
            &["seqno", "cid", "name"],
        ),
        IndexXinfo => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result1 | PragmaFlags::SchemaOpt,
            &["seqno", "cid", "name", "desc", "coll", "key"],
        ),
        IndexList => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result1 | PragmaFlags::SchemaOpt,
            &["seq", "name", "unique", "origin", "partial"],
        ),
        TableList => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result0 | PragmaFlags::Result1,
            &["schema", "name", "type", "ncol", "wr", "strict"],
        ),
        TableInfo => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result1 | PragmaFlags::SchemaOpt,
            &["cid", "name", "type", "notnull", "dflt_value", "pk"],
        ),
        TableXinfo => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result1 | PragmaFlags::SchemaOpt,
            &[
                "cid",
                "name",
                "type",
                "notnull",
                "dflt_value",
                "pk",
                "hidden",
            ],
        ),
        UserVersion => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["user_version"],
        ),
        WalCheckpoint => Pragma::new(PragmaFlags::NeedSchema, &["busy", "log", "checkpointed"]),
        AutoVacuum => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["auto_vacuum"],
        ),
        BusyTimeout => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["busy_timeout"],
        ),
        IntegrityCheck => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::ReadOnly | PragmaFlags::Result0,
            &["message"],
        ),
        QuickCheck => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::ReadOnly | PragmaFlags::Result0,
            &["message"],
        ),
        CaptureDataChangesConn | UnstableCaptureDataChangesConn => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result0 | PragmaFlags::SchemaReq,
            &["mode", "table", "version"],
        ),
        QueryOnly => Pragma::new(
            PragmaFlags::Result0 | PragmaFlags::NoColumns1,
            &["query_only"],
        ),
        IAmADummy | RequireWhere => Pragma::new(
            PragmaFlags::Result0 | PragmaFlags::NoColumns1,
            &["require_where"],
        ),
        FreelistCount => Pragma::new(PragmaFlags::Result0, &["freelist_count"]),
        EncryptionKey => Pragma::new(
            PragmaFlags::Result0 | PragmaFlags::SchemaReq | PragmaFlags::NoColumns1,
            &["hexkey"],
        ),
        EncryptionCipher => Pragma::new(
            PragmaFlags::Result0 | PragmaFlags::SchemaReq | PragmaFlags::NoColumns1,
            &["cipher"],
        ),
        PragmaName::MvccCheckpointThreshold => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["mvcc_checkpoint_threshold"],
        ),
        PragmaName::MvccGcThreshold => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["mvcc_gc_threshold"],
        ),
        PragmaName::MvccGroupCommit => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["mvcc_group_commit"],
        ),
        ForeignKeys => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["foreign_keys"],
        ),
        ForeignKeyList => Pragma::new(
            PragmaFlags::NeedSchema | PragmaFlags::Result1 | PragmaFlags::SchemaOpt,
            &[
                "id",
                "seq",
                "table",
                "from",
                "to",
                "on_update",
                "on_delete",
                "match",
            ],
        ),
        FunctionList => Pragma::new(
            PragmaFlags::Result0,
            &["name", "builtin", "type", "enc", "narg", "flags"],
        ),
        PragmaName::CacheSpill => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["cache_spill"],
        ),
        #[cfg(target_vendor = "apple")]
        PragmaName::Fullfsync => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["fullfsync"],
        ),
        IgnoreCheckConstraints => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["ignore_check_constraints"],
        ),
        ListTypes => Pragma::new(
            PragmaFlags::Result0,
            &["type", "parent", "encode", "decode", "default", "operators"],
        ),
        VdbeTrace => Pragma::new(
            PragmaFlags::NoColumns1 | PragmaFlags::Result0,
            &["vdbe_trace"],
        ),
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PragmaVirtualTable {
    pub(crate) pragma_name: String,
    visible_column_count: usize,
    max_arg_count: usize,
    has_pragma_arg: bool,
}

impl PragmaVirtualTable {
    pub(crate) fn functions() -> Vec<(PragmaVirtualTable, String)> {
        PragmaName::iter()
            .filter(|name| {
                *name != PragmaName::LegacyFileFormat
                    && *name != PragmaName::FullColumnNames
                    && *name != PragmaName::ShortColumnNames
                    && *name != PragmaName::EmptyResultCallbacks
            })
            .filter_map(|name| {
                let pragma = pragma_for(&name);
                if pragma
                    .flags
                    .intersects(PragmaFlags::Result0 | PragmaFlags::Result1)
                {
                    Some(Self::create(name.to_string(), pragma))
                } else {
                    None
                }
            })
            .collect()
    }

    fn create(pragma_name: String, pragma: Pragma) -> (Self, String) {
        let mut max_arg_count = 0;
        let mut has_pragma_arg = false;

        let mut sql = String::from("CREATE TABLE x (");
        let col_defs = pragma
            .columns
            .iter()
            .map(|col| format!("\"{col}\""))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&col_defs);
        if pragma.flags.contains(PragmaFlags::Result1) {
            sql.push_str(", arg HIDDEN");
            max_arg_count += 1;
            has_pragma_arg = true;
        }
        if pragma
            .flags
            .intersects(PragmaFlags::SchemaOpt | PragmaFlags::SchemaReq)
        {
            sql.push_str(", schema HIDDEN");
            max_arg_count += 1;
        }
        sql.push(')');

        (
            PragmaVirtualTable {
                pragma_name,
                visible_column_count: pragma.columns.len(),
                max_arg_count,
                has_pragma_arg,
            },
            sql,
        )
    }

    pub(crate) fn open(&self, conn: Arc<Connection>) -> crate::Result<PragmaVirtualTableCursor> {
        Ok(PragmaVirtualTableCursor {
            pragma_name: self.pragma_name.clone(),
            pos: 0,
            conn,
            stmt: None,
            arg: None,
            visible_column_count: self.visible_column_count,
            max_arg_count: self.max_arg_count,
            has_pragma_arg: self.has_pragma_arg,
        })
    }

    pub(crate) fn best_index(
        &self,
        constraints: &[ConstraintInfo],
    ) -> Result<IndexInfo, ResultCode> {
        let mut arg0_idx = None;
        let mut arg1_idx = None;

        for (i, c) in constraints.iter().enumerate() {
            if c.op != ConstraintOp::Eq {
                continue;
            }
            let visible_count = self.visible_column_count as u32;
            if c.column_index < visible_count {
                continue;
            }
            if !c.usable {
                return Err(ResultCode::ConstraintViolation);
            }
            let hidden_idx = c.column_index - visible_count;
            match hidden_idx {
                0 => arg0_idx = Some(i),
                1 => arg1_idx = Some(i),
                _ => unreachable!("Unexpected hidden column index: {}", hidden_idx),
            }
        }

        let argv_arg0_idx = arg0_idx.map_or(0, |_| 1);
        let argv_arg1_idx = arg1_idx.map_or(argv_arg0_idx, |_| argv_arg0_idx + 1);

        let constraint_usages = constraints
            .iter()
            .enumerate()
            .map(|(i, _)| {
                let argv_index = if Some(i) == arg0_idx {
                    Some(argv_arg0_idx)
                } else if Some(i) == arg1_idx {
                    Some(argv_arg1_idx)
                } else {
                    None
                };
                ConstraintUsage {
                    argv_index,
                    omit: argv_index.is_some(),
                }
            })
            .collect();

        Ok(IndexInfo {
            constraint_usages,
            ..Default::default()
        })
    }
}

pub struct PragmaVirtualTableCursor {
    pragma_name: String,
    pos: usize,
    conn: Arc<Connection>,
    stmt: Option<Statement>,
    arg: Option<String>,
    visible_column_count: usize,
    max_arg_count: usize,
    has_pragma_arg: bool,
}

impl PragmaVirtualTableCursor {
    pub(crate) fn rowid(&self) -> i64 {
        self.pos as i64
    }

    pub(crate) fn next(&mut self) -> crate::Result<bool> {
        let stmt = self
            .stmt
            .as_mut()
            .ok_or_else(|| LimboError::InternalError("Statement is missing".into()))?;
        let result = stmt.step()?;
        match result {
            StepResult::Done => Ok(false),
            _ => {
                self.pos += 1;
                Ok(true)
            }
        }
    }

    pub(crate) fn column(&self, idx: usize) -> crate::Result<Value> {
        if idx < self.visible_column_count {
            let value = self
                .stmt
                .as_ref()
                .ok_or_else(|| LimboError::InternalError("Statement is missing".into()))?
                .row()
                .ok_or_else(|| LimboError::InternalError("No row available".into()))?
                .get_value(idx)
                .clone();
            return Ok(value);
        }

        let value = match idx - self.visible_column_count {
            0 => self
                .arg
                .as_ref()
                .map_or(Value::Null, |arg| Value::from_text(arg.to_string())),
            _ => Value::Null,
        };
        Ok(value)
    }

    pub(crate) fn filter(&mut self, args: crate::alloc::Vec<Value>) -> crate::Result<bool> {
        if args.len() > self.max_arg_count {
            return Err(LimboError::ParseError(format!(
                "Too many arguments for pragma {}: expected at most {}, got {}",
                self.pragma_name,
                self.max_arg_count,
                args.len()
            )));
        }

        let to_text = |v: &Value| v.to_text().map(str::to_owned);
        let (arg, schema) = match args.as_slice() {
            [arg0] if self.has_pragma_arg => (to_text(arg0), None),
            [arg0] => (None, to_text(arg0)),
            [arg0, arg1] => (to_text(arg0), to_text(arg1)),
            _ => (None, None),
        };

        self.arg = arg;
        // VFilter starts a new scan. Reset the position so repeated scans
        // return the same rowids, as SQLite does.
        self.pos = 0;

        if let Some(schema) = schema {
            // Schema-qualified PRAGMA statements are not supported yet
            return Err(LimboError::ParseError(format!(
                "Schema argument is not supported yet (got schema: '{schema}')"
            )));
        }

        let mut sql = format!("PRAGMA {}", self.pragma_name);
        if let Some(arg) = &self.arg {
            sql.push_str(&format!("=\"{arg}\""));
        }

        // Table-valued pragma helpers execute inside the parent statement's VM step.
        // For example, UPDATE ... FROM pragma_table_info('dst') runs this helper while the
        // outer UPDATE still has an active MVCC transaction and write cursor open on `dst`.
        // The helper must stay nested so it does not disturb the parent's transaction state.
        self.stmt = Some(self.conn.prepare_internal(sql)?);

        self.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, MemoryIO, SqliteDialect};

    #[test]
    fn filter_starts_each_pragma_scan_at_rowid_one() {
        let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
        let db =
            Database::open_file(io, crate::util::MEMORY_PATH, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE scan_target(first, second)")
            .unwrap();

        let pragma_vtab = PragmaVirtualTable {
            pragma_name: "table_info".to_string(),
            visible_column_count: 6,
            max_arg_count: 2,
            has_pragma_arg: true,
        };
        let mut cursor = pragma_vtab.open(conn).unwrap();

        assert!(cursor
            .filter(crate::alloc::vec![Value::from_text("scan_target")])
            .unwrap());
        assert_eq!(cursor.rowid(), 1);
        assert!(cursor.next().unwrap());
        assert_eq!(cursor.rowid(), 2);
        assert!(!cursor.next().unwrap());

        assert!(cursor
            .filter(crate::alloc::vec![Value::from_text("scan_target")])
            .unwrap());
        assert_eq!(cursor.rowid(), 1);
    }

    #[test]
    fn test_best_index_argv_order_both_hidden_constraints() {
        // Test when both hidden constraints are present (arg and schema)
        let pragma_vtab = PragmaVirtualTable {
            pragma_name: "table_info".to_string(),
            visible_column_count: 6,
            max_arg_count: 2,
            has_pragma_arg: true,
        };

        let constraints = vec![
            usable_constraint(6), // arg (first hidden column)
            usable_constraint(7), // schema (second hidden column)
        ];

        let index_info = pragma_vtab.best_index(&constraints).unwrap();

        // Verify arg gets argv_index 1, schema gets argv_index 2
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // arg
        assert_eq!(index_info.constraint_usages[1].argv_index, Some(2)); // schema
        assert!(index_info.constraint_usages[0].omit);
        assert!(index_info.constraint_usages[1].omit);
    }

    #[test]
    fn test_best_index_argv_order_only_arg() {
        let pragma_vtab = PragmaVirtualTable {
            pragma_name: "table_info".to_string(),
            visible_column_count: 6,
            max_arg_count: 2,
            has_pragma_arg: true,
        };

        let constraints = vec![
            usable_constraint(6), // arg (first hidden column)
        ];

        let index_info = pragma_vtab.best_index(&constraints).unwrap();

        // Verify arg gets argv_index 1
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // arg
        assert!(index_info.constraint_usages[0].omit);
    }

    #[test]
    fn test_best_index_argv_order_only_schema() {
        let pragma_vtab = PragmaVirtualTable {
            pragma_name: "table_info".to_string(),
            visible_column_count: 1,
            max_arg_count: 1,
            has_pragma_arg: false,
        };

        let constraints = vec![
            usable_constraint(1), // schema (first hidden column after visible columns)
        ];

        let index_info = pragma_vtab.best_index(&constraints).unwrap();

        // Verify schema gets argv_index 1
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(1)); // schema
        assert!(index_info.constraint_usages[0].omit);
    }

    #[test]
    fn test_best_index_argv_order_reverse_constraint_order() {
        // Test when constraints are provided in reverse order (schema first, then arg)
        let pragma_vtab = PragmaVirtualTable {
            pragma_name: "table_info".to_string(),
            visible_column_count: 6,
            max_arg_count: 2,
            has_pragma_arg: true,
        };

        let constraints = vec![
            usable_constraint(7), // schema (second hidden column)
            usable_constraint(6), // arg (first hidden column)
        ];

        let index_info = pragma_vtab.best_index(&constraints).unwrap();

        // Verify arg still gets argv_index 1, schema gets argv_index 2 regardless of constraint order
        assert_eq!(index_info.constraint_usages[0].argv_index, Some(2)); // schema
        assert_eq!(index_info.constraint_usages[1].argv_index, Some(1)); // arg
        assert!(index_info.constraint_usages[0].omit);
        assert!(index_info.constraint_usages[1].omit);
    }

    #[test]
    fn test_best_index_visible_columns_ignored() {
        let pragma_vtab = PragmaVirtualTable {
            pragma_name: "table_info".to_string(),
            visible_column_count: 6,
            max_arg_count: 2,
            has_pragma_arg: true,
        };

        let constraints = vec![
            usable_constraint(0), // visible column (cid)
            usable_constraint(6), // arg (hidden)
        ];

        let index_info = pragma_vtab.best_index(&constraints).unwrap();

        // Verify visible column constraint is ignored, arg gets argv_index 1
        assert_eq!(index_info.constraint_usages[0].argv_index, None); // visible column
        assert_eq!(index_info.constraint_usages[1].argv_index, Some(1)); // arg
        assert!(!index_info.constraint_usages[0].omit); // visible column not omitted
        assert!(index_info.constraint_usages[1].omit); // arg omitted
    }

    #[test]
    fn test_best_index_no_usable_constraints() {
        let pragma_vtab = PragmaVirtualTable {
            pragma_name: "table_info".to_string(),
            visible_column_count: 6,
            max_arg_count: 2,
            has_pragma_arg: true,
        };

        let constraints = vec![ConstraintInfo {
            column_index: 6,
            op: ConstraintOp::Eq,
            usable: false,
            index: 0,
        }];

        let result = pragma_vtab.best_index(&constraints);

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
