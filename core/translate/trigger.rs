use crate::translate::emitter::Resolver;
use crate::translate::schema::{emit_schema_entry, SchemaEntryType, SQLITE_TABLEID};
use crate::translate::ProgramBuilder;
use crate::translate::ProgramBuilderOpts;
use crate::util::{escape_sql_string_literal, normalize_ident};
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{Cookie, Insn};
use crate::{bail_parse_error, Result, MAIN_DB_ID};
use turso_parser::ast::{self, QualifiedName};

/// Reconstruct SQL string from CREATE TRIGGER AST
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_trigger_to_sql(
    temporary: bool,
    if_not_exists: bool,
    trigger_name: &QualifiedName,
    time: Option<ast::TriggerTime>,
    event: &ast::TriggerEvent,
    tbl_name: &QualifiedName,
    for_each_row: bool,
    when_clause: Option<&ast::Expr>,
    commands: &[ast::TriggerCmd],
) -> String {
    let mut sql = String::new();
    sql.push_str("CREATE");
    if temporary {
        sql.push_str(" TEMP");
    }
    sql.push_str(" TRIGGER");
    if if_not_exists {
        sql.push_str(" IF NOT EXISTS");
    }
    sql.push(' ');
    sql.push_str(&trigger_name.name.as_ident());
    sql.push(' ');

    if let Some(t) = time {
        match t {
            ast::TriggerTime::Before => sql.push_str("BEFORE "),
            ast::TriggerTime::After => sql.push_str("AFTER "),
            ast::TriggerTime::InsteadOf => sql.push_str("INSTEAD OF "),
        }
    }

    match event {
        ast::TriggerEvent::Delete => sql.push_str("DELETE"),
        ast::TriggerEvent::Insert => sql.push_str("INSERT"),
        ast::TriggerEvent::Update => sql.push_str("UPDATE"),
        ast::TriggerEvent::UpdateOf(cols) => {
            sql.push_str("UPDATE OF ");
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&col.as_ident());
            }
        }
    }

    sql.push_str(" ON ");
    if let Some(ref db_name) = tbl_name.db_name {
        sql.push_str(&db_name.as_ident());
        sql.push('.');
    }
    sql.push_str(&tbl_name.name.as_ident());
    if for_each_row {
        sql.push_str(" FOR EACH ROW");
    }

    if let Some(when) = when_clause {
        sql.push_str(" WHEN ");
        sql.push_str(&when.to_string());
    }

    sql.push_str(" BEGIN");
    for cmd in commands {
        sql.push(' ');
        sql.push_str(&cmd.to_string());
        sql.push(';');
    }
    sql.push_str(" END");

    sql
}

/// Translate CREATE TRIGGER statement
#[allow(clippy::too_many_arguments)]
pub fn translate_create_trigger(
    trigger_name: QualifiedName,
    resolver: &Resolver,
    temporary: bool,
    if_not_exists: bool,
    time: Option<ast::TriggerTime>,
    tbl_name: QualifiedName,
    program: &mut ProgramBuilder,
    sql: String,
    commands: &[ast::TriggerCmd],
    when_clause: Option<&ast::Expr>,
) -> Result<()> {
    let normalized_trigger_name = normalize_ident(trigger_name.name.as_str());
    let normalized_table_name = normalize_ident(tbl_name.name.as_str());
    let database_id =
        resolve_create_trigger_database_id(resolver, &trigger_name, &tbl_name, temporary)?;
    let target_table_database_id = if temporary {
        // A temp trigger's target table can be in any schema.
        resolver.resolve_existing_table_database_id_qualified(&tbl_name)?
    } else if tbl_name.db_name.is_some() {
        resolver.resolve_database_id(&tbl_name)?
    } else {
        database_id
    };
    // Temp triggers are allowed to reference tables in other databases.
    if target_table_database_id != database_id && database_id != crate::TEMP_DB_ID {
        let table_db_name = tbl_name
            .db_name
            .as_ref()
            .map(|db_name| db_name.as_str().to_string())
            .or_else(|| resolver.get_database_name_by_index(target_table_database_id))
            .unwrap_or_else(|| "main".to_string());
        bail_parse_error!(
            "trigger {} cannot reference objects in database {}",
            normalized_trigger_name,
            table_db_name
        );
    }

    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    program.begin_write_operation()?;

    // Temp-backed triggers follow SQLite's looser name-resolution rules and may
    // access objects across schemas. Ordinary triggers stay schema-local.
    if database_id != crate::TEMP_DB_ID {
        validate_trigger_no_cross_db_refs(
            resolver,
            database_id,
            &normalized_trigger_name,
            commands,
            when_clause,
        )?;
    }

    if crate::schema::is_system_table(&normalized_table_name) {
        bail_parse_error!("cannot create trigger on system table");
    }

    // Check if trigger already exists
    if resolver.with_schema(database_id, |s| {
        s.get_trigger(&normalized_trigger_name).is_some()
    }) {
        if if_not_exists {
            return Ok(());
        }
        bail_parse_error!(
            "trigger {} already exists",
            crate::util::identifier_token_for_error(&trigger_name.name)
        );
    }

    // Verify the table exists (use the table's database, not the trigger's).
    let table = resolver.with_schema(target_table_database_id, |s| {
        s.get_table(&normalized_table_name)
    });
    let Some(table) = table else {
        // SQLite qualifies a non-temp trigger's missing target table with its
        // database ("no such table: main.t1"); temp triggers report the name
        // as the user wrote it.
        if temporary {
            bail_parse_error!(
                "no such table: {}",
                crate::util::table_name_for_error(&tbl_name)
            );
        }
        let db_name = resolver
            .get_database_name_by_index(target_table_database_id)
            .unwrap_or_else(|| "main".to_string());
        bail_parse_error!("no such table: {}.{}", db_name, tbl_name.name.as_str());
    };
    if table.virtual_table().is_some() {
        bail_parse_error!("cannot create triggers on virtual tables");
    }

    if time
        .as_ref()
        .is_some_and(|t| *t == ast::TriggerTime::InsteadOf)
    {
        bail_parse_error!("INSTEAD OF triggers are not supported yet");
    }

    let opts = ProgramBuilderOpts::new(1, 30, 1);
    program.extend(&opts);

    // Open cursor to sqlite_schema table (in the trigger's database)
    let table = resolver
        .with_schema(database_id, |s| s.get_btree_table(SQLITE_TABLEID))
        .unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });

    // Add the trigger entry to sqlite_schema
    emit_schema_entry(
        program,
        resolver,
        sqlite_schema_cursor_id,
        None, // cdc_table_cursor_id, no cdc for triggers
        SchemaEntryType::Trigger,
        &normalized_trigger_name,
        &normalized_table_name,
        0, // triggers don't have a root page
        Some(sql),
    )?;

    // Update schema version
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: (schema_version + 1) as i32,
        p5: 0,
    });

    // Parse schema to load the new trigger
    let escaped_trigger_name = escape_sql_string_literal(&normalized_trigger_name);
    program.emit_insn(Insn::ParseSchema {
        db: database_id,
        where_clause: Some(format!(
            "name = '{escaped_trigger_name}' AND type = 'trigger'"
        )),
        trigger_target_database_id: (temporary && tbl_name.db_name.is_none())
            .then_some(target_table_database_id),
    });

    Ok(())
}

fn resolve_create_trigger_database_id(
    resolver: &Resolver,
    trigger_name: &QualifiedName,
    tbl_name: &QualifiedName,
    temporary: bool,
) -> Result<usize> {
    // TEMP triggers always live in the temp schema.
    if temporary {
        return Ok(crate::TEMP_DB_ID);
    }
    if trigger_name.db_name.is_some() {
        return resolver.resolve_database_id(trigger_name);
    }

    // Unqualified trigger names default to main, except when the trigger is
    // attached to a temp object. SQLite stores those triggers in temp.
    match resolver.resolve_existing_table_database_id_qualified(tbl_name)? {
        crate::TEMP_DB_ID => Ok(crate::TEMP_DB_ID),
        _ => Ok(crate::MAIN_DB_ID),
    }
}

/// Validate that no table or expression reference in a trigger body points to a
/// database other than the trigger's own database. SQLite forbids this with:
///   "trigger X cannot reference objects in database Y"
fn validate_trigger_no_cross_db_refs(
    resolver: &Resolver,
    trigger_db_id: usize,
    trigger_name: &str,
    commands: &[ast::TriggerCmd],
    when_clause: Option<&ast::Expr>,
) -> Result<()> {
    let ctx = CrossDbCheckCtx {
        resolver,
        trigger_db_id,
        trigger_name,
    };

    if let Some(when) = when_clause {
        ctx.check_expr(when)?;
    }

    for cmd in commands {
        match cmd {
            ast::TriggerCmd::Insert { select, .. } => {
                ctx.check_select(select)?;
            }
            ast::TriggerCmd::Update {
                sets,
                from,
                where_clause,
                ..
            } => {
                for set in sets {
                    ctx.check_expr(&set.expr)?;
                }
                if let Some(from) = from {
                    ctx.check_from_clause(from)?;
                }
                if let Some(wc) = where_clause {
                    ctx.check_expr(wc)?;
                }
            }
            ast::TriggerCmd::Delete { where_clause, .. } => {
                if let Some(wc) = where_clause {
                    ctx.check_expr(wc)?;
                }
            }
            ast::TriggerCmd::Select(select) => {
                ctx.check_select(select)?;
            }
        }
    }

    Ok(())
}

struct CrossDbCheckCtx<'a> {
    resolver: &'a Resolver<'a>,
    trigger_db_id: usize,
    trigger_name: &'a str,
}

impl CrossDbCheckCtx<'_> {
    fn check_qname(&self, qn: &QualifiedName) -> Result<()> {
        if let Some(ref db_name) = qn.db_name {
            let resolved = self.resolver.resolve_database_id(qn)?;
            if resolved != self.trigger_db_id {
                bail_parse_error!(
                    "trigger {} cannot reference objects in database {}",
                    self.trigger_name,
                    db_name
                );
            }
        }
        Ok(())
    }

    /// Check an expression tree for cross-database references.
    /// Descends into subqueries that walk_expr skips.
    /// Note: DoublyQualified expressions (e.g. aux.table.column) are NOT checked
    /// here — they are column references, not table references. SQLite allows them
    /// at CREATE TRIGGER time and only rejects them at runtime as "no such column".
    fn check_expr(&self, expr: &ast::Expr) -> Result<()> {
        use crate::translate::expr::WalkControl;
        crate::translate::expr::walk_expr(expr, &mut |e| -> Result<WalkControl> {
            match e {
                // walk_expr doesn't descend into subqueries, so handle them here
                ast::Expr::Exists(select) | ast::Expr::Subquery(select) => {
                    self.check_select(select)?;
                }
                ast::Expr::InSelect { rhs, .. } => {
                    self.check_select(rhs)?;
                }
                _ => {}
            }
            Ok(WalkControl::Continue)
        })?;
        Ok(())
    }

    fn check_select(&self, select: &ast::Select) -> Result<()> {
        check_select_table_refs(select, &|qn| self.check_qname(qn), &|e| self.check_expr(e))
    }

    fn check_from_clause(&self, from: &ast::FromClause) -> Result<()> {
        check_from_clause_refs(from, &|qn| self.check_qname(qn), &|e| self.check_expr(e))
    }
}

fn check_select_table_refs(
    select: &ast::Select,
    check_qname: &dyn Fn(&QualifiedName) -> Result<()>,
    check_expr: &dyn Fn(&ast::Expr) -> Result<()>,
) -> Result<()> {
    // Check CTEs
    if let Some(with) = &select.with {
        for cte in &with.ctes {
            check_select_table_refs(&cte.select, check_qname, check_expr)?;
        }
    }

    check_one_select_refs(&select.body.select, check_qname, check_expr)?;

    for compound in &select.body.compounds {
        check_one_select_refs(&compound.select, check_qname, check_expr)?;
    }

    // Check ORDER BY / LIMIT / OFFSET expressions
    for col in &select.order_by {
        check_expr(&col.expr)?;
    }
    if let Some(limit) = &select.limit {
        check_expr(&limit.expr)?;
        if let Some(offset) = &limit.offset {
            check_expr(offset)?;
        }
    }

    Ok(())
}

fn check_one_select_refs(
    one_select: &ast::OneSelect,
    check_qname: &dyn Fn(&QualifiedName) -> Result<()>,
    check_expr: &dyn Fn(&ast::Expr) -> Result<()>,
) -> Result<()> {
    match one_select {
        ast::OneSelect::Select {
            columns,
            from,
            where_clause,
            group_by,
            ..
        } => {
            for col in columns {
                if let ast::ResultColumn::Expr(expr, _) = col {
                    check_expr(expr)?;
                }
            }
            if let Some(from) = from {
                check_from_clause_refs(from, check_qname, check_expr)?;
            }
            if let Some(wc) = where_clause {
                check_expr(wc)?;
            }
            if let Some(gb) = group_by {
                for expr in &gb.exprs {
                    check_expr(expr)?;
                }
                if let Some(having) = &gb.having {
                    check_expr(having)?;
                }
            }
        }
        ast::OneSelect::Values(rows) => {
            for row in rows {
                for expr in row {
                    check_expr(expr)?;
                }
            }
        }
    }
    Ok(())
}

fn check_from_clause_refs(
    from: &ast::FromClause,
    check_qname: &dyn Fn(&QualifiedName) -> Result<()>,
    check_expr: &dyn Fn(&ast::Expr) -> Result<()>,
) -> Result<()> {
    check_select_table_ref(&from.select, check_qname, check_expr)?;
    for join in &from.joins {
        check_select_table_ref(&join.table, check_qname, check_expr)?;
        if let Some(ast::JoinConstraint::On(expr)) = &join.constraint {
            check_expr(expr)?;
        }
    }
    Ok(())
}

fn check_select_table_ref(
    table: &ast::SelectTable,
    check_qname: &dyn Fn(&QualifiedName) -> Result<()>,
    check_expr: &dyn Fn(&ast::Expr) -> Result<()>,
) -> Result<()> {
    match table {
        ast::SelectTable::Table(qname, ..) => {
            check_qname(qname)?;
        }
        ast::SelectTable::TableCall(qname, args, _) => {
            check_qname(qname)?;
            for arg in args {
                check_expr(arg)?;
            }
        }
        ast::SelectTable::Select(select, _) => {
            check_select_table_refs(select, check_qname, check_expr)?;
        }
        ast::SelectTable::Sub(from, _) => {
            check_from_clause_refs(from, check_qname, check_expr)?;
        }
    }
    Ok(())
}

/// Translate DROP TRIGGER statement
pub fn translate_drop_trigger(
    resolver: &Resolver,
    trigger_name: &ast::QualifiedName,
    if_exists: bool,
    program: &mut ProgramBuilder,
) -> Result<()> {
    let database_id = resolver.resolve_existing_trigger_database_id(trigger_name)?;
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    program.begin_write_operation()?;
    let normalized_trigger_name = normalize_ident(trigger_name.name.as_str());

    // Check if trigger exists
    if resolver.with_schema(database_id, |s| {
        s.get_trigger(&normalized_trigger_name).is_none()
    }) {
        if if_exists {
            return Ok(());
        }
        bail_parse_error!("no such trigger: {}", normalized_trigger_name);
    }

    let opts = ProgramBuilderOpts::new(1, 30, 1);
    program.extend(&opts);

    // Open cursor to sqlite_schema table (structure is the same for all databases)
    let table = resolver.with_schema(MAIN_DB_ID, |s| s.get_btree_table(SQLITE_TABLEID).unwrap());
    let sqlite_schema_cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(table));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1i64.into(),
        db: database_id,
    });

    let search_loop_label = program.allocate_label();
    let skip_non_trigger_label = program.allocate_label();
    let done_label = program.allocate_label();
    let rewind_done_label = program.allocate_label();

    // Find and delete the trigger from sqlite_schema
    program.emit_insn(Insn::Rewind {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: rewind_done_label,
    });

    program.preassign_label_to_next_insn(search_loop_label);

    // Check if this is the trigger we're looking for
    // sqlite_schema columns: type, name, tbl_name, rootpage, sql
    // Column 0: type (should be "trigger")
    // Column 1: name (should match trigger_name)
    let type_reg = program.alloc_register();
    let name_reg = program.alloc_register();
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 0,
        dest: type_reg,
        default: None,
    });
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 1,
        dest: name_reg,
        default: None,
    });

    // Check if type == "trigger"
    let type_str_reg = program.emit_string8_new_reg("trigger".to_string());
    program.emit_insn(Insn::Ne {
        lhs: type_reg,
        rhs: type_str_reg,
        target_pc: skip_non_trigger_label,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: program.curr_collation(),
    });

    // Check if name matches
    let trigger_name_str_reg = program.emit_string8_new_reg(normalized_trigger_name.clone());
    program.emit_insn(Insn::Ne {
        lhs: name_reg,
        rhs: trigger_name_str_reg,
        target_pc: skip_non_trigger_label,
        flags: crate::vdbe::insn::CmpInsFlags::default(),
        collation: program.curr_collation(),
    });

    // Found it! Delete the row
    program.emit_insn(Insn::Delete {
        cursor_id: sqlite_schema_cursor_id,
        table_name: SQLITE_TABLEID.to_string(),
        is_part_of_update: false,
    });
    program.emit_insn(Insn::Goto {
        target_pc: done_label,
    });

    program.preassign_label_to_next_insn(skip_non_trigger_label);
    // Continue to next row
    program.emit_insn(Insn::Next {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: search_loop_label,
        fullscan: false,
        is_index: false,
    });

    program.preassign_label_to_next_insn(done_label);

    program.preassign_label_to_next_insn(rewind_done_label);

    // Update schema version
    let schema_version = resolver.with_schema(database_id, |s| s.schema_version);
    program.emit_insn(Insn::SetCookie {
        db: database_id,
        cookie: Cookie::SchemaVersion,
        value: (schema_version + 1) as i32,
        p5: 0,
    });

    program.emit_insn(Insn::DropTrigger {
        db: database_id,
        trigger_name: normalized_trigger_name,
    });

    Ok(())
}
