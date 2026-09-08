use crate::sync::Arc;
use rustc_hash::FxHashMap as HashMap;

use crate::schema::{EXPR_INDEX_SENTINEL, ROWID_SENTINEL};
use crate::translate::emitter::Resolver;
use crate::translate::expr::{bind_and_rewrite_expr, BindingBehavior};
use crate::translate::expression_index::expression_index_column_usage;
use crate::translate::plan::{ColumnMask, Operation};
use crate::translate::planner::ROWID_STRS;
use crate::{
    bail_parse_error,
    schema::{Schema, Table},
    util::normalize_ident,
    vdbe::builder::{ProgramBuilder, ProgramBuilderOpts},
    CaptureDataChangesExt, Connection,
};
use turso_parser::ast::{self, Expr};

use super::emitter::emit_program;
use super::expr::process_returning_clause;
use super::optimizer::optimize_plan;
use super::plan::{
    ColumnUsedMask, DmlSafety, JoinedTable, Plan, TableReferences, UpdatePlan, UpdateSetClause,
};
use super::planner::{append_vtab_predicates_to_where_clause, parse_from, parse_where};
use super::subquery::{
    mark_shared_cte_materialization_requirements, plan_subqueries_from_returning,
    plan_subqueries_from_update_sets, plan_subqueries_from_where_clause,
};
/*
* Update is simple. By default we scan the table, and for each row, we check the WHERE
* clause. If it evaluates to true, we build the new record with the updated value and insert.
*
* EXAMPLE:
*
sqlite> explain update t set a = 100 where b = 5;
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------------
0     Init           0     16    0                    0   Start at 16
1     Null           0     1     2                    0   r[1..2]=NULL
2     Noop           1     0     1                    0
3     OpenWrite      0     2     0     3              0   root=2 iDb=0; t
4     Rewind         0     15    0                    0
5       Column         0     1     6                    0   r[6]= cursor 0 column 1
6       Ne             7     14    6     BINARY-8       81  if r[6]!=r[7] goto 14
7       Rowid          0     2     0                    0   r[2]= rowid of 0
8       IsNull         2     15    0                    0   if r[2]==NULL goto 15
9       Integer        100   3     0                    0   r[3]=100
10      Column         0     1     4                    0   r[4]= cursor 0 column 1
11      Column         0     2     5                    0   r[5]= cursor 0 column 2
12      MakeRecord     3     3     1                    0   r[1]=mkrec(r[3..5])
13      Insert         0     1     2     t              7   intkey=r[2] data=r[1]
14    Next           0     5     0                    1
15    Halt           0     0     0                    0
16    Transaction    0     1     1     0              1   usesStmtJournal=0
17    Integer        5     7     0                    0   r[7]=5
18    Goto           0     1     0                    0
*/
pub fn translate_update(
    body: ast::Update,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
) -> crate::Result<()> {
    let plan = prepare_and_optimize_update_plan(program, resolver, body, connection, false, None)?;
    let Plan::Update(ref update_plan) = plan else {
        unreachable!("prepare_and_optimize_update_plan must return Plan::Update");
    };
    super::stmt_journal::set_update_stmt_journal_flags(program, update_plan, resolver, connection)?;

    let opts = ProgramBuilderOpts::new(1, 20, 4);
    program.extend(&opts);
    emit_program(connection, resolver, program, plan, |_| {})?;
    Ok(())
}

/// Normalize a planned UPDATE RHS into the per-column expressions consumed by SET.
///
/// Example:
/// UPDATE t SET (a, b) = (SELECT x, y FROM s)
/// After planning, the right-hand side (SELECT x, y FROM s) becomes a
/// SubqueryResult with 2 columns, and this is split into 2 1-column SubqueryResult expressions
/// i.e. one per each SET assignment.
fn split_update_set_values(mut expr: Expr, target_count: usize) -> crate::Result<Vec<Expr>> {
    while let Expr::Parenthesized(mut exprs) = expr {
        match exprs.len() {
            1 => expr = *exprs.pop().expect("single parenthesized expr"),
            _ => {
                expr = Expr::Parenthesized(exprs);
                break;
            }
        }
    }

    match expr {
        Expr::Parenthesized(vals) => {
            if vals.len() != target_count {
                bail_parse_error!("{} columns assigned {} values", target_count, vals.len());
            }
            Ok(vals.into_iter().map(|expr| *expr).collect())
        }
        Expr::SubqueryResult {
            subquery_id,
            lhs,
            not_in,
            query_type:
                ast::SubqueryType::RowValue {
                    result_reg_start,
                    num_regs,
                },
        } => {
            if num_regs != target_count {
                bail_parse_error!("{} columns assigned {} values", target_count, num_regs);
            }
            Ok((0..num_regs)
                .map(|offset| Expr::SubqueryResult {
                    subquery_id,
                    lhs: lhs.clone(),
                    not_in,
                    query_type: ast::SubqueryType::RowValue {
                        result_reg_start: result_reg_start + offset,
                        num_regs: 1,
                    },
                })
                .collect())
        }
        Expr::Subquery(_) => Err(crate::LimboError::InternalError(
            "UPDATE set clause subquery should be planned before normalization".to_string(),
        )),
        expr => {
            if target_count != 1 {
                bail_parse_error!("{} columns assigned 1 values", target_count);
            }
            Ok(vec![expr])
        }
    }
}

pub fn translate_update_for_schema_change(
    body: ast::Update,
    resolver: &Resolver,
    program: &mut ProgramBuilder,
    connection: &Arc<crate::Connection>,
    ddl_query: &str,
    after: impl FnOnce(&mut ProgramBuilder),
) -> crate::Result<()> {
    let plan = prepare_and_optimize_update_plan(
        program,
        resolver,
        body,
        connection,
        true,
        Some(ddl_query),
    )?;
    let opts = ProgramBuilderOpts::new(1, 20, 4);
    program.extend(&opts);
    emit_program(connection, resolver, program, plan, after)?;
    Ok(())
}

fn prepare_and_optimize_update_plan(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    body: ast::Update,
    connection: &Arc<crate::Connection>,
    is_internal_schema_change: bool,
    ddl_query_for_cdc_update: Option<&str>,
) -> crate::Result<Plan> {
    let mut update_plan = prepare_update_plan(
        program,
        resolver,
        body,
        connection,
        is_internal_schema_change,
    )?;

    if let Some(ddl_query_for_cdc_update) = ddl_query_for_cdc_update {
        if program.capture_data_changes_info().has_updates() {
            update_plan.cdc_update_alter_statement = Some(ddl_query_for_cdc_update.to_string());
        }
    }
    let mut read_scope_tables = update_plan.build_read_scope_tables();
    plan_subqueries_from_where_clause(
        program,
        &mut update_plan.non_from_clause_subqueries,
        &mut read_scope_tables,
        &mut update_plan.where_clause,
        resolver,
        connection,
    )?;
    mark_shared_cte_materialization_requirements(
        &mut read_scope_tables,
        &mut update_plan.non_from_clause_subqueries,
    );
    update_plan.target_table = read_scope_tables.joined_tables_mut().remove(0);
    update_plan.from_tables = read_scope_tables;

    let mut plan = Plan::Update(Box::new(update_plan));
    optimize_plan(program, &mut plan, resolver)?;
    Ok(plan)
}

fn validate_update(
    schema: &Schema,
    table_name: &str,
    is_internal_schema_change: bool,
    conn: &Arc<Connection>,
) -> crate::Result<()> {
    // Check if this is a system table that should be protected from direct writes
    if !is_internal_schema_change
        && !conn.is_nested_stmt()
        && !conn.is_mvcc_bootstrap_connection()
        && !crate::schema::allow_user_dml(table_name)
    {
        crate::bail_parse_error!("table {} may not be modified", table_name);
    }
    // Check if this is a materialized view
    if schema.is_materialized_view(table_name) {
        bail_parse_error!("cannot modify materialized view {}", table_name);
    }

    // Check if this table has any incompatible dependent views
    schema.with_incompatible_dependent_views(table_name, |views| {
    if !views.is_empty() {
        use crate::incremental::compiler::DBSP_CIRCUIT_VERSION;
        crate::bail_parse_error!(
            "Cannot UPDATE table '{table_name}' because it has incompatible dependent materialized view(s): {}. \n\
             These views were created with a different DBSP version than the current version ({DBSP_CIRCUIT_VERSION}). \n\
             Please DROP and recreate the view(s) before modifying this table.",
            views.iter().map(|view| view.as_str()).collect::<Vec<_>>().join(", "),
        );
    }
    Ok(())
    })
}

fn prepare_update_plan(
    program: &mut ProgramBuilder,
    resolver: &Resolver,
    mut body: ast::Update,
    connection: &Arc<crate::Connection>,
    is_internal_schema_change: bool,
) -> crate::Result<UpdatePlan> {
    let database_id = resolver.resolve_existing_table_database_id_qualified(&body.tbl_name)?;
    let schema = resolver.schema();
    let target_name = &body.tbl_name.name;
    let table = match resolver.with_schema(database_id, |s| s.get_table(target_name.as_str())) {
        Some(table) => table,
        None => bail_parse_error!(
            "no such table: {}",
            crate::util::table_name_for_error(&body.tbl_name)
        ),
    };
    if program.trigger.is_some() && table.virtual_table().is_some() {
        bail_parse_error!(
            "unsafe use of virtual table \"{}\"",
            body.tbl_name.name.as_str()
        );
    }
    if table.btree().is_some_and(|bt| !bt.has_rowid) {
        bail_parse_error!("UPDATE of WITHOUT ROWID tables is not supported");
    }
    let schema_cookie = resolver.with_schema(database_id, |s| s.schema_version);
    program.begin_write_on_database(database_id, schema_cookie)?;
    validate_update(
        schema,
        target_name.as_str(),
        is_internal_schema_change,
        connection,
    )?;

    // Extract WITH, OR conflict clause, and INDEXED BY before borrowing body mutably
    let with = body.with.take();
    let or_conflict = body.or_conflict.take();
    let indexed = body.indexed.take();

    let table_name = table.get_name();

    let target_table = JoinedTable {
        table: table.as_ref().clone(),
        identifier: body.tbl_name.identifier(),
        internal_id: program.table_reference_counter.next(),
        op: Operation::default_scan_for(&table),
        join_info: None,
        col_used_mask: ColumnUsedMask::default(),
        column_use_counts: Vec::new(),
        expression_index_usages: Vec::new(),
        database_id,
        indexed,
        plan_estimate: None,
    };
    let mut from_tables = TableReferences::new_empty();
    let mut where_clause = vec![];
    let mut vtab_predicates = vec![];
    parse_from(
        body.from.take(),
        resolver,
        program,
        with,
        true,
        &mut where_clause,
        &mut vtab_predicates,
        &mut from_tables,
        connection,
    )?;

    // SQLite rejects UPDATE FROM when a NATURAL JOIN (or explicit USING) introduces
    // a column name that already appears in another FROM-side table without being
    // deduplicated. This proactive check mirrors what SQLite does even when no
    // unqualified column reference appears in the query.
    if !from_tables.joined_tables().is_empty() {
        check_update_from_column_ambiguity(from_tables.joined_tables(), connection.as_ref())?;
    }

    let target_identifier = body.tbl_name.alias.as_ref().map_or_else(
        || normalize_ident(body.tbl_name.name.as_str()),
        |alias| normalize_ident(alias.as_str()),
    );
    let target_table_name = normalize_ident(body.tbl_name.name.as_str());
    let mut non_from_clause_subqueries = vec![];
    // Reject fairly specific cases like UPDATE t SET x=5 FROM t.
    let illegal_target_reference = from_tables.joined_tables().iter().any(|joined| {
        joined.database_id == database_id
            && normalize_ident(joined.identifier.as_str()) == target_identifier
            && normalize_ident(joined.table.get_name()) == target_table_name
    });
    if illegal_target_reference {
        bail_parse_error!(
            "target object/alias may not appear in FROM clause: {}",
            body.tbl_name
                .alias
                .as_ref()
                .map_or(body.tbl_name.name.as_str(), |alias| alias.as_str())
        );
    }

    // At this point where_clause only contains items collected from the FROM clause's JOIN ON conditions.
    // Subqueries within those conditions are not allowed to reference the UPDATE target table, so we do a subquery
    // planning pass here before extending the table scope with the target table.
    plan_subqueries_from_where_clause(
        program,
        &mut non_from_clause_subqueries,
        &mut from_tables,
        &mut where_clause,
        resolver,
        connection,
    )?;

    let mut read_scope_tables = TableReferences::new(vec![target_table], vec![]);
    if from_tables.right_join_swapped() {
        read_scope_tables.set_right_join_swapped();
    }
    read_scope_tables.extend(from_tables);

    for set in &mut body.sets {
        bind_and_rewrite_expr(
            &mut set.expr,
            Some(&mut read_scope_tables),
            None,
            resolver,
            BindingBehavior::ResultColumnsNotAllowed,
        )?;
    }

    plan_subqueries_from_update_sets(
        program,
        &mut non_from_clause_subqueries,
        &mut read_scope_tables,
        &mut body.sets,
        resolver,
        connection,
    )?;

    let set_clauses = collect_update_set_clauses(&mut body.sets, &table, table_name)?;

    let result_columns = if !body.returning.is_empty() {
        // Plan subqueries in RETURNING expressions before processing
        // (so SubqueryResult nodes are cloned into result_columns)
        let mut returning_target = read_scope_tables.joined_tables()[0].clone();
        // SQLite resolves RETURNING columns for an aliased UPDATE target through the
        // base table name, not the UPDATE alias. Keep the target table in scope, but
        // under its schema name so `RETURNING t.col` works while `RETURNING alias.col`
        // still fails.
        returning_target.identifier = table_name.to_string();
        let mut returning_table_references = TableReferences::new(
            vec![returning_target],
            read_scope_tables.outer_query_refs().to_vec(),
        );
        plan_subqueries_from_returning(
            program,
            &mut non_from_clause_subqueries,
            &mut returning_table_references,
            &mut body.returning,
            resolver,
            connection,
        )?;

        process_returning_clause(
            &mut body.returning,
            &mut returning_table_references,
            resolver,
        )?
    } else {
        vec![]
    };

    let columns = table.columns();
    append_vtab_predicates_to_where_clause(
        &mut vtab_predicates,
        &mut read_scope_tables,
        &result_columns,
        &mut where_clause,
        resolver,
    )?;
    parse_where(
        body.where_clause.as_deref(),
        &mut read_scope_tables,
        Some(&result_columns),
        &mut where_clause,
        resolver,
    )?;

    let indexes_to_update = collect_indexes_to_update(
        &table,
        table_name,
        database_id,
        columns,
        &set_clauses,
        &mut read_scope_tables,
        resolver,
    )?;

    // read_scope_tables was only used for visibility in SET clause expressions, so reconstruct
    // target_table and from_tables here.
    let target_table = read_scope_tables.joined_tables_mut().remove(0);
    let from_tables = read_scope_tables;

    Ok(UpdatePlan {
        target_table,
        from_tables,
        or_conflict,
        set_clauses,
        where_clause,
        returning: (!result_columns.is_empty()).then_some(result_columns),
        contains_constant_false_condition: false,
        indexes_to_update,
        write_set_plan: None,
        cdc_update_alter_statement: None,
        non_from_clause_subqueries,
        safety: DmlSafety::default(),
    })
}

fn collect_update_set_clauses(
    sets: &mut [ast::Set],
    table: &Table,
    table_name: &str,
) -> crate::Result<Vec<UpdateSetClause>> {
    let column_lookup: HashMap<String, usize> = table
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, col)| col.name.as_ref().map(|name| (name.to_lowercase(), i)))
        .collect();
    let mut set_clauses: Vec<UpdateSetClause> = Vec::with_capacity(sets.len());

    for set in sets {
        let expr = std::mem::replace(&mut set.expr, Box::new(Expr::Literal(ast::Literal::Null)));
        let values = split_update_set_values(*expr, set.col_names.len())?;

        for (col_name, expr) in set.col_names.iter().zip(values.into_iter()) {
            let expr = Box::new(expr);
            let ident = normalize_ident(col_name.as_str());

            let col_index = match column_lookup.get(&ident) {
                Some(idx) => {
                    table.columns()[*idx].ensure_not_generated("UPDATE", col_name.as_str())?;
                    *idx
                }
                None if ROWID_STRS.iter().any(|s| s.eq_ignore_ascii_case(&ident)) => table
                    .columns()
                    .iter()
                    .enumerate()
                    .find(|(_i, c)| c.is_rowid_alias())
                    .map_or(ROWID_SENTINEL, |(idx, _col)| idx),
                None => crate::bail_parse_error!("no such column: {}.{}", table_name, col_name),
            };

            match set_clauses
                .iter_mut()
                .find(|set| set.column_index == col_index)
            {
                Some(existing) => compose_update_set_clause(existing, expr),
                None => set_clauses.push(UpdateSetClause::new(col_index, expr)),
            }
        }
    }

    Ok(set_clauses)
}

fn compose_update_set_clause(existing: &mut UpdateSetClause, expr: Box<Expr>) {
    if let Expr::FunctionCall {
        name,
        args: new_args,
        ..
    } = expr.as_ref()
    {
        if name.as_str().eq_ignore_ascii_case("array_set_element") && new_args.len() == 3 {
            let mut composed_args = new_args.clone();
            composed_args[0].clone_from(&existing.expr);
            existing.expr = Box::new(Expr::FunctionCall {
                name: name.clone(),
                distinctness: None,
                args: composed_args,
                order_by: vec![],
                within_group: vec![],
                filter_over: turso_parser::ast::FunctionTail {
                    filter_clause: None,
                    over_clause: None,
                },
            });
            return;
        }
    }

    existing.expr = expr;
}

fn collect_indexes_to_update(
    table: &Table,
    table_name: &str,
    database_id: usize,
    columns: &[crate::schema::Column],
    set_clauses: &[UpdateSetClause],
    read_scope_tables: &mut TableReferences,
    resolver: &Resolver,
) -> crate::Result<Vec<Arc<crate::schema::Index>>> {
    use crate::alloc::TursoIteratorExt;
    let indexes: crate::alloc::Vec<_> = resolver.with_schema(database_id, |s| {
        s.get_indices(table_name).cloned().try_collect()
    })?;
    let target_table_ref = read_scope_tables
        .joined_tables()
        .first()
        .expect("UPDATE must have a target table reference");
    let target_table_internal_id = target_table_ref.internal_id;
    let rowid_alias_used = set_clauses.iter().any(|set| {
        set.column_index == ROWID_SENTINEL || columns[set.column_index].is_rowid_alias()
    });
    let updated_cols: Option<ColumnMask> = (!rowid_alias_used)
        .then(|| set_clauses.iter().map(|set| set.column_index).try_collect())
        .transpose()?;
    let affected_cols = match (table.btree(), updated_cols.as_ref()) {
        (Some(bt), Some(updated)) => Some(bt.columns_affected_by_update(updated)?),
        (None, Some(updated)) => Some(updated.clone()),
        _ => None,
    };
    let mut indexes_to_update = Vec::new();
    let mut columns_to_mark_used = Vec::new();

    for idx in indexes {
        let mut must_update = rowid_alias_used;
        let mut expression_cols_used = ColumnUsedMask::default();

        for col in idx.columns.iter() {
            if let Some(expr) = col.expr.as_ref() {
                let cols_used =
                    expression_index_column_usage(expr.as_ref(), target_table_ref, resolver)?;
                expression_cols_used.union_with(&cols_used)?;

                if !must_update
                    && affected_cols.as_ref().is_some_and(|affected_cols| {
                        cols_used.iter().any(|cidx| affected_cols.get(cidx))
                    })
                {
                    must_update = true;
                }
            } else if !must_update
                && affected_cols
                    .as_ref()
                    .is_some_and(|affected_cols| affected_cols.get(col.pos_in_table))
            {
                must_update = true;
            }
        }

        if !must_update {
            if let Some(where_expr) = &idx.where_clause {
                let cols_used =
                    expression_index_column_usage(where_expr.as_ref(), target_table_ref, resolver)?;
                must_update = affected_cols.as_ref().is_some_and(|affected_cols| {
                    cols_used.iter().any(|cidx| affected_cols.get(cidx))
                });
            }
        }

        if must_update {
            columns_to_mark_used.extend(expression_cols_used.iter());
            // mark as used dependencies of virtual columns
            if let Some(btree) = target_table_ref.table.btree() {
                let virtual_cols_in_index = idx
                    .columns
                    .iter()
                    .filter(|c| c.pos_in_table != EXPR_INDEX_SENTINEL)
                    .map(|c| c.pos_in_table)
                    .filter(|&pos| btree.columns()[pos].is_virtual_generated());
                let deps = btree.dependencies_of_columns(virtual_cols_in_index)?;
                columns_to_mark_used.extend(deps.iter());
            }
            indexes_to_update.push(idx);
        }
    }

    for col_idx in columns_to_mark_used {
        read_scope_tables.mark_column_used(target_table_internal_id, col_idx);
    }

    Ok(indexes_to_update)
}

/// Proactive column-ambiguity check for UPDATE FROM.
///
/// SQLite rejects UPDATE FROM when:
/// 1. The same table identifier appears more than once in the FROM clause
///    (duplicate unaliased tables or duplicate aliases).
/// 2. A NATURAL JOIN or USING deduplicates a column, but another table in
///    the FROM graph also exposes that column without deduplication.
///
/// In a regular SELECT these are only caught when an unqualified reference
/// is resolved, but UPDATE FROM checks eagerly regardless of whether the
/// column is actually referenced.
fn check_update_from_column_ambiguity(
    joined_tables: &[JoinedTable],
    connection: &Connection,
) -> crate::Result<()> {
    // Check 1: reject duplicate table identifiers (e.g. FROM t2, t2).
    for (i, table) in joined_tables.iter().enumerate() {
        if joined_tables[..i]
            .iter()
            .any(|preceding| preceding.identifier == table.identifier)
        {
            let db_name = connection
                .get_database_name_by_index(table.database_id)
                .unwrap_or_else(|| "main".to_string());
            bail_parse_error!(
                "ambiguous column name: {db_name}.{}._ROWID_",
                table.identifier
            );
        }
    }

    // Check 2: for each USING/NATURAL-deduplicated column, verify that no
    // other table in the FROM graph (preceding or following) exposes the
    // same column without its own deduplication.
    for (i, table) in joined_tables.iter().enumerate() {
        let using = match &table.join_info {
            Some(info) if !info.using.is_empty() => &info.using,
            _ => continue,
        };
        for using_col in using {
            let col_name = normalize_ident(using_col.as_str());

            // Count how many *other* tables expose this column without it
            // being covered by their own USING clause.
            let mut found_count = 0usize;
            for (j, other) in joined_tables.iter().enumerate() {
                if j == i {
                    continue;
                }
                let has_col = other.columns().iter().any(|c| {
                    c.name
                        .as_ref()
                        .is_some_and(|n| n.eq_ignore_ascii_case(&col_name))
                });
                if !has_col {
                    continue;
                }
                // If this table's own USING already covers the column,
                // it was deduplicated by its own NATURAL/USING JOIN — skip.
                let already_deduped = other.join_info.as_ref().is_some_and(|info| {
                    info.using
                        .iter()
                        .any(|u| u.as_str().eq_ignore_ascii_case(&col_name))
                });
                if !already_deduped {
                    found_count += 1;
                }
            }
            if found_count > 1 {
                bail_parse_error!("ambiguous column name: {}", using_col.as_str());
            }
        }
    }
    Ok(())
}
