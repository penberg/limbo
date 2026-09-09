use super::*;
use crate::alloc::{TryClone, TursoIteratorExt};
use crate::schema::GeneratedType;
use crate::translate::emitter::HashLabels;
use crate::translate::expr::comparison_affinity;
use crate::translate::plan::ColumnUsedMask;
use crate::vdbe::builder::SelfTableContext;

#[derive(Debug, Clone)]
/// Payload layout metadata recorded during hash-build planning or reuse.
pub(super) struct HashBuildPayloadInfo {
    pub payload_columns: Vec<MaterializedColumnRef>,
    pub key_affinities: String,
    pub use_bloom_filter: bool,
    pub bloom_filter_cursor_id: CursorID,
    pub allow_seek: bool,
}

/// Static configuration for a fresh hash-table build.
struct HashBuildConfig {
    payload_columns: Vec<MaterializedColumnRef>,
    payload_signature_columns: ColumnUsedMask,
    key_affinities: String,
    collations: Vec<CollationSeq>,
    use_bloom_filter: bool,
    bloom_filter_cursor_id: CursorID,
    materialized_cursor_id: Option<CursorID>,
    use_materialized_keys: bool,
    allow_seek: bool,
    signature: HashBuildSignature,
}

/// Typestate entry point for hash-build planning.
///
/// Planning decides whether an existing hash build can be reused and, if not,
/// captures all configuration needed to emit a fresh build deterministically.
pub(crate) struct HashBuildPlanner<'a, 'plan> {
    program: &'a mut ProgramBuilder,
    t_ctx: &'a mut TranslateCtx<'plan>,
    table_references: &'a TableReferences,
    non_from_clause_subqueries: &'a [NonFromClauseSubquery],
    predicates: &'a [WhereTerm],
    hash_join_op: &'a HashJoinOp,
    hash_build_cursor_id: CursorID,
    hash_table_id: usize,
}

/// A planned hash build whose signature check has already completed.
pub(super) struct PreparedHashBuild<'a, 'plan> {
    planner: HashBuildPlanner<'a, 'plan>,
    config: HashBuildConfig,
}

/// Result of hash-build planning.
///
/// Reuse means the caller can immediately probe an existing compatible hash
/// table. Build means the caller must execute the prepared build before probing.
pub(super) enum HashBuildPlan<'a, 'plan> {
    Reuse(HashBuildPayloadInfo),
    Build(Box<PreparedHashBuild<'a, 'plan>>),
}

impl<'a, 'plan> HashBuildPlanner<'a, 'plan> {
    #[allow(clippy::too_many_arguments)]
    /// Capture the immutable inputs needed to decide whether to reuse or build.
    pub(super) const fn new(
        program: &'a mut ProgramBuilder,
        t_ctx: &'a mut TranslateCtx<'plan>,
        table_references: &'a TableReferences,
        non_from_clause_subqueries: &'a [NonFromClauseSubquery],
        predicates: &'a [WhereTerm],
        hash_join_op: &'a HashJoinOp,
        hash_build_cursor_id: CursorID,
        hash_table_id: usize,
    ) -> Self {
        Self {
            program,
            t_ctx,
            table_references,
            non_from_clause_subqueries,
            predicates,
            hash_join_op,
            hash_build_cursor_id,
            hash_table_id,
        }
    }

    /// Decide whether the hash table can be reused or must be rebuilt.
    pub(super) fn prepare(self) -> Result<HashBuildPlan<'a, 'plan>> {
        let materialized_input = self
            .t_ctx
            .materialized_build_inputs
            .get(&self.hash_join_op.build_table_idx);
        let materialized_cursor_id = materialized_input.map(|input| input.cursor_id);
        let num_keys = self.hash_join_op.join_keys.len();

        let mut key_affinities = String::new();
        for join_key in &self.hash_join_op.join_keys {
            let build_expr = join_key.get_build_expr(self.predicates);
            let probe_expr = join_key.get_probe_expr(self.predicates);
            let affinity =
                comparison_affinity(build_expr, probe_expr, Some(self.table_references), None);
            key_affinities.push(affinity.aff_mask());
        }

        let collations: Vec<CollationSeq> = self
            .hash_join_op
            .join_keys
            .iter()
            .map(|join_key| {
                let (original_lhs, original_rhs) = match join_key.build_side {
                    BinaryExprSide::Lhs => (
                        join_key.get_build_expr(self.predicates),
                        join_key.get_probe_expr(self.predicates),
                    ),
                    BinaryExprSide::Rhs => (
                        join_key.get_probe_expr(self.predicates),
                        join_key.get_build_expr(self.predicates),
                    ),
                };
                resolve_comparison_collseq_with_symbols(
                    original_lhs,
                    original_rhs,
                    self.table_references,
                    Some(self.t_ctx.resolver.symbol_table),
                )
                .unwrap_or(CollationSeq::Binary)
            })
            .collect();

        let use_bloom_filter = self.hash_join_op.use_bloom_filter
            && collations
                .iter()
                .all(|c| matches!(*c, CollationSeq::Binary | CollationSeq::Unset));

        let build_table = &self.table_references.joined_tables()[self.hash_join_op.build_table_idx];
        let (payload_columns, payload_signature_columns, use_materialized_keys, allow_seek) =
            match materialized_input.map(|input| &input.mode) {
                Some(MaterializedBuildInputMode::KeyPayload {
                    num_keys: payload_num_keys,
                    payload_columns,
                }) => {
                    turso_assert!(
                        *payload_num_keys == num_keys,
                        "materialized hash build input key count mismatch"
                    );
                    let payload_signature_columns: ColumnUsedMask = (0..payload_columns.len())
                        .map(|i| *payload_num_keys + i)
                        .try_collect()?;
                    (
                        payload_columns.clone(),
                        payload_signature_columns,
                        true,
                        false,
                    )
                }
                _ => {
                    let payload_signature_columns: ColumnUsedMask =
                        build_table.col_used_mask.try_clone()?;
                    let payload_columns = payload_signature_columns
                        .iter()
                        .map(|col_idx| {
                            let column = build_table
                                .columns()
                                .get(col_idx)
                                .expect("build table column missing");
                            MaterializedColumnRef::Column {
                                table_id: build_table.internal_id,
                                column_idx: col_idx,
                                is_rowid_alias: column.is_rowid_alias(),
                            }
                        })
                        .collect();
                    (payload_columns, payload_signature_columns, false, true)
                }
            };

        let bloom_filter_cursor_id = if use_materialized_keys {
            materialized_cursor_id.expect("materialized input cursor is required")
        } else {
            self.hash_build_cursor_id
        };

        let join_key_indices = self
            .hash_join_op
            .join_keys
            .iter()
            .map(|key| key.where_clause_idx)
            .collect::<Vec<_>>();
        let signature = HashBuildSignature {
            join_key_indices,
            payload_refs: payload_columns.clone(),
            key_affinities: key_affinities.clone(),
            use_bloom_filter,
            materialized_input_cursor: materialized_cursor_id,
            materialized_mode: materialized_input.as_ref().map(|input| match input.mode {
                MaterializedBuildInputMode::RowidOnly => MaterializedBuildInputModeTag::RowidOnly,
                MaterializedBuildInputMode::KeyPayload { .. } => {
                    MaterializedBuildInputModeTag::Payload
                }
            }),
        };

        if self
            .program
            .hash_build_signature_matches(self.hash_table_id, &signature)
        {
            return Ok(HashBuildPlan::Reuse(HashBuildPayloadInfo {
                payload_columns,
                key_affinities,
                use_bloom_filter,
                bloom_filter_cursor_id,
                allow_seek,
            }));
        }
        if self.program.has_hash_build_signature(self.hash_table_id) {
            self.program.emit_insn(Insn::HashClose {
                hash_table_id: self.hash_table_id,
            });
            self.program.clear_hash_build_signature(self.hash_table_id);
        }

        Ok(HashBuildPlan::Build(Box::new(PreparedHashBuild {
            planner: self,
            config: HashBuildConfig {
                payload_columns,
                payload_signature_columns,
                key_affinities,
                collations,
                use_bloom_filter,
                bloom_filter_cursor_id,
                materialized_cursor_id,
                use_materialized_keys,
                allow_seek,
                signature,
            },
        })))
    }
}

impl<'a, 'plan> PreparedHashBuild<'a, 'plan> {
    /// Emit the fresh hash build after planning has fixed its configuration.
    pub(super) fn emit(self) -> Result<HashBuildPayloadInfo> {
        let Self { planner, config } = self;
        let build_table =
            &planner.table_references.joined_tables()[planner.hash_join_op.build_table_idx];
        let btree = build_table
            .btree()
            .expect("Hash join build table must be a BTree table");
        let num_keys = planner.hash_join_op.join_keys.len();

        let build_key_start_reg = planner.program.alloc_registers(num_keys);
        let mut build_rowid_reg = None;
        let mut build_iter_cursor_id = planner.hash_build_cursor_id;
        let materialized_input = planner
            .t_ctx
            .materialized_build_inputs
            .get(&planner.hash_join_op.build_table_idx);
        if let Some(input) = materialized_input {
            match &input.mode {
                MaterializedBuildInputMode::RowidOnly => {
                    build_rowid_reg = Some(planner.program.alloc_register());
                    build_iter_cursor_id = input.cursor_id;
                }
                MaterializedBuildInputMode::KeyPayload { .. } => {
                    build_iter_cursor_id = input.cursor_id;
                }
            }
        }

        let (key_source_cursor_id, payload_source_cursor_id, hash_build_rowid_cursor_id) =
            if config.use_materialized_keys {
                (
                    build_iter_cursor_id,
                    build_iter_cursor_id,
                    build_iter_cursor_id,
                )
            } else {
                (
                    planner.hash_build_cursor_id,
                    planner.hash_build_cursor_id,
                    planner.hash_build_cursor_id,
                )
            };

        let build_loop_start = planner.program.allocate_label();
        let build_loop_end = planner.program.allocate_label();
        let skip_to_next = planner.program.allocate_label();
        let label_hash_build_end = planner.program.allocate_label();
        planner.program.emit_insn(Insn::Once {
            target_pc_when_reentered: label_hash_build_end,
        });

        if !config.use_materialized_keys {
            planner.program.emit_insn(Insn::OpenRead {
                cursor_id: planner.hash_build_cursor_id,
                root_page: btree.root_page,
                db: build_table.database_id,
            });
        }

        planner.program.emit_insn(Insn::Rewind {
            cursor_id: build_iter_cursor_id,
            pc_if_empty: build_loop_end,
        });

        if !config.use_materialized_keys {
            planner
                .program
                .set_cursor_override(build_table.internal_id, planner.hash_build_cursor_id);
        }

        planner
            .program
            .preassign_label_to_next_insn(build_loop_start);

        if let (Some(rowid_reg), Some(input_cursor_id)) =
            (build_rowid_reg, config.materialized_cursor_id)
        {
            planner
                .program
                .emit_column_or_rowid(input_cursor_id, 0, rowid_reg);
            planner.program.emit_insn(Insn::SeekRowid {
                cursor_id: planner.hash_build_cursor_id,
                src_reg: rowid_reg,
                target_pc: skip_to_next,
            });
        }

        // Pre-filtering build rows with WHERE terms is a pure optimization: the
        // same terms are still evaluated in the probe loop. It is safe for INNER
        // and LEFT OUTER joins because the build side is never null-extended, so
        // a build row rejected here can never appear in the output. For FULL
        // OUTER joins it is wrong: a build row removed from the hash table makes
        // the probe rows that matched it look unmatched, so they would be
        // emitted as spurious null-extended rows.
        let push_where_filters_to_build = !config.use_materialized_keys
            && planner.hash_join_op.join_type != HashJoinType::FullOuter;
        if push_where_filters_to_build {
            let build_only_mask: TableMask = [planner.hash_join_op.build_table_idx]
                .into_iter()
                .try_collect()?;
            for cond in planner.predicates.iter() {
                if cond.from_outer_join.is_some() {
                    // OUTER JOIN predicates must stay on the right-table loop
                    // recorded in `from_outer_join`; applying them while
                    // building the hash table would drop unmatched build rows
                    // before null-extension.
                    continue;
                }
                let mask = table_mask_from_expr(
                    &cond.expr,
                    planner.table_references,
                    planner.non_from_clause_subqueries,
                )?;
                if !mask.get(planner.hash_join_op.build_table_idx)
                    || !build_only_mask.contains_all_set_bits_of(&mask)
                {
                    continue;
                }
                if expr_references_outer_query(&cond.expr, planner.table_references) {
                    continue;
                }
                let jump_target_when_true = planner.program.allocate_label();
                let condition_metadata = ConditionMetadata {
                    jump_if_condition_is_true: false,
                    jump_target_when_true,
                    jump_target_when_false: skip_to_next,
                    jump_target_when_null: skip_to_next,
                };
                translate_condition_expr(
                    planner.program,
                    planner.table_references,
                    &cond.expr,
                    condition_metadata,
                    &planner.t_ctx.resolver,
                )?;
                planner
                    .program
                    .preassign_label_to_next_insn(jump_target_when_true);
            }
        }

        if config.use_materialized_keys {
            for idx in 0..num_keys {
                planner.program.emit_column_or_rowid(
                    key_source_cursor_id,
                    idx,
                    build_key_start_reg + idx,
                );
            }
        } else {
            for (idx, join_key) in planner.hash_join_op.join_keys.iter().enumerate() {
                let build_expr = join_key.get_build_expr(planner.predicates);
                translate_expr(
                    planner.program,
                    Some(planner.table_references),
                    build_expr,
                    build_key_start_reg + idx,
                    &planner.t_ctx.resolver,
                )?;
            }
        }

        if let Some(count) = std::num::NonZeroUsize::new(num_keys) {
            planner.program.emit_insn(Insn::Affinity {
                start_reg: build_key_start_reg,
                count,
                affinities: config.key_affinities.clone(),
            });
        }

        let num_payload = config.payload_columns.len();
        let (payload_start_reg, mut payload_info) = if num_payload > 0 {
            let payload_reg = planner.program.alloc_registers(num_payload);
            for (i, col_idx) in config.payload_signature_columns.iter().enumerate() {
                match build_table
                    .columns()
                    .get(col_idx)
                    .map(|c| c.generated_type())
                {
                    Some(GeneratedType::Virtual { expr, .. }) if !config.use_materialized_keys => {
                        planner.t_ctx.resolver.with_self_table_context(
                            planner.program,
                            Some(&SelfTableContext::ForSelect {
                                table_ref_id: build_table.internal_id,
                                referenced_tables: planner.table_references.clone(),
                            }),
                            |program, _| -> Result<()> {
                                translate_expr(
                                    program,
                                    Some(planner.table_references),
                                    expr,
                                    payload_reg + i,
                                    &planner.t_ctx.resolver,
                                )?;
                                Ok(())
                            },
                        )?;

                        planner.program.emit_column_affinity(
                            payload_reg + i,
                            build_table.columns()[col_idx].affinity(),
                        );
                    }
                    _ => planner.program.emit_column_or_rowid(
                        payload_source_cursor_id,
                        col_idx,
                        payload_reg + i,
                    ),
                };
            }
            (
                Some(payload_reg),
                HashBuildPayloadInfo {
                    payload_columns: config.payload_columns.clone(),
                    key_affinities: config.key_affinities.clone(),
                    use_bloom_filter: false,
                    bloom_filter_cursor_id: config.bloom_filter_cursor_id,
                    allow_seek: config.allow_seek,
                },
            )
        } else {
            (
                None,
                HashBuildPayloadInfo {
                    payload_columns: vec![],
                    key_affinities: config.key_affinities.clone(),
                    use_bloom_filter: false,
                    bloom_filter_cursor_id: config.bloom_filter_cursor_id,
                    allow_seek: config.allow_seek,
                },
            )
        };

        if !config.use_materialized_keys {
            planner
                .program
                .clear_cursor_override(build_table.internal_id);
        }

        planner.program.emit_insn(Insn::HashBuild {
            data: Box::new(HashBuildData {
                cursor_id: hash_build_rowid_cursor_id,
                key_start_reg: build_key_start_reg,
                num_keys,
                hash_table_id: planner.hash_table_id,
                mem_budget: planner.hash_join_op.mem_budget,
                collations: config.collations,
                payload_start_reg,
                num_payload,
                track_matched: matches!(
                    planner.hash_join_op.join_type,
                    HashJoinType::LeftOuter | HashJoinType::FullOuter
                ),
            }),
        });
        if config.use_bloom_filter {
            planner.program.emit_insn(Insn::FilterAdd {
                cursor_id: config.bloom_filter_cursor_id,
                key_reg: build_key_start_reg,
                num_keys,
            });
            payload_info.use_bloom_filter = true;
        }

        planner.program.preassign_label_to_next_insn(skip_to_next);
        planner.program.emit_insn(Insn::Next {
            cursor_id: build_iter_cursor_id,
            pc_if_next: build_loop_start,
            fullscan: false,
            is_index: false,
        });

        planner.program.preassign_label_to_next_insn(build_loop_end);
        planner.program.emit_insn(Insn::HashBuildFinalize {
            hash_table_id: planner.hash_table_id,
        });
        planner
            .program
            .record_hash_build_signature(planner.hash_table_id, config.signature);

        planner
            .program
            .preassign_label_to_next_insn(label_hash_build_end);
        Ok(payload_info)
    }
}

struct PreparedProbeBuild {
    build_cursor_id: CursorID,
    payload_info: HashBuildPayloadInfo,
}

struct ProbeSetupState {
    build_cursor_id: CursorID,
    payload_info: HashBuildPayloadInfo,
    payload_dest_reg: Option<usize>,
    match_reg: usize,
    hash_probe_miss_label: BranchOffset,
    match_found_label: BranchOffset,
    hash_next_label: BranchOffset,
    probe_rowid_reg: Option<usize>,
    key_start_reg: usize,
    num_keys: usize,
    grace_flag_reg: Option<usize>,
}

/// Hash-join probe setup in `open_loop`.
///
/// Setup still runs in three ordered phases, but plain helper methods are enough:
/// build or reuse the hash table, emit probe instructions, then install `HashCtx`.
pub(super) struct HashProbeSetupEmitter<'a, 'plan> {
    program: &'a mut ProgramBuilder,
    t_ctx: &'a mut TranslateCtx<'plan>,
    table_references: &'a TableReferences,
    subqueries: &'a [NonFromClauseSubquery],
    predicates: &'a [WhereTerm],
    hash_join_op: &'a HashJoinOp,
    mode: &'a OperationMode,
    probe_cursor_id: CursorID,
    loop_start: BranchOffset,
    loop_end: BranchOffset,
    next: BranchOffset,
    live_table_ids: &'a HashSet<TableInternalId>,
}

impl<'a, 'plan> HashProbeSetupEmitter<'a, 'plan> {
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn new(
        program: &'a mut ProgramBuilder,
        t_ctx: &'a mut TranslateCtx<'plan>,
        table_references: &'a TableReferences,
        subqueries: &'a [NonFromClauseSubquery],
        predicates: &'a [WhereTerm],
        hash_join_op: &'a HashJoinOp,
        mode: &'a OperationMode,
        probe_cursor_id: CursorID,
        loop_start: BranchOffset,
        loop_end: BranchOffset,
        next: BranchOffset,
        live_table_ids: &'a HashSet<TableInternalId>,
    ) -> Self {
        Self {
            program,
            t_ctx,
            table_references,
            subqueries,
            predicates,
            hash_join_op,
            mode,
            probe_cursor_id,
            loop_start,
            loop_end,
            next,
            live_table_ids,
        }
    }

    /// Ensure the build cursor exists and the hash table is ready for probing.
    fn prepare_build(&mut self) -> Result<PreparedProbeBuild> {
        let build_table = &self.table_references.joined_tables()[self.hash_join_op.build_table_idx];
        let (build_cursor_id, _) = build_table.resolve_cursors(self.program, self.mode.clone())?;
        let build_cursor_id = if let Some(cursor_id) = build_cursor_id {
            cursor_id
        } else {
            let btree = build_table
                .btree()
                .expect("Hash join build table must be a BTree table");
            let cursor_id = self.program.alloc_cursor_id_keyed_if_not_exists(
                CursorKey::table(build_table.internal_id),
                CursorType::BTreeTable(btree.clone()),
            );
            self.program.emit_insn(Insn::OpenRead {
                cursor_id,
                root_page: btree.root_page,
                db: build_table.database_id,
            });
            cursor_id
        };

        let hash_table_id: usize = build_table.internal_id.into();
        let btree = build_table
            .btree()
            .expect("Hash join build table must be a BTree table");
        let hash_build_cursor_id = self.program.alloc_cursor_id_keyed_if_not_exists(
            CursorKey::hash_build(build_table.internal_id),
            CursorType::BTreeTable(btree),
        );
        let payload_info = match HashBuildPlanner::new(
            self.program,
            self.t_ctx,
            self.table_references,
            self.subqueries,
            self.predicates,
            self.hash_join_op,
            hash_build_cursor_id,
            hash_table_id,
        )
        .prepare()?
        {
            HashBuildPlan::Reuse(info) => Ok(info),
            HashBuildPlan::Build(prepared) => prepared.emit(),
        }?;

        Ok(PreparedProbeBuild {
            build_cursor_id,
            payload_info,
        })
    }

    /// Emit the probe-side cursor positioning, key loading, and `HashProbe`. Advance
    /// to the state needed to install the resulting `HashCtx`.
    fn emit_probe(&mut self, prepared: PreparedProbeBuild) -> Result<ProbeSetupState> {
        let PreparedProbeBuild {
            build_cursor_id,
            payload_info,
        } = prepared;
        let build_table = &self.table_references.joined_tables()[self.hash_join_op.build_table_idx];
        let hash_table_id: usize = build_table.internal_id.into();
        let num_keys = self.hash_join_op.join_keys.len();

        // For LEFT/FULL OUTER hash joins, reset matched_bits at the start of
        // each outer-loop iteration so marks from a previous probe pass don't
        // suppress NULL-fill rows in the current one.
        if matches!(
            self.hash_join_op.join_type,
            HashJoinType::LeftOuter | HashJoinType::FullOuter
        ) {
            self.program
                .emit_insn(Insn::HashResetMatched { hash_table_id });
        }

        self.program.emit_insn(Insn::Rewind {
            cursor_id: self.probe_cursor_id,
            pc_if_empty: self.loop_end,
        });
        self.program.preassign_label_to_next_insn(self.loop_start);

        let probe_key_start_reg = self.program.alloc_registers(num_keys);
        for (idx, join_key) in self.hash_join_op.join_keys.iter().enumerate() {
            let probe_expr = join_key.get_probe_expr(self.predicates);
            translate_expr(
                self.program,
                Some(self.table_references),
                probe_expr,
                probe_key_start_reg + idx,
                &self.t_ctx.resolver,
            )?;
        }

        if let Some(count) = std::num::NonZeroUsize::new(num_keys) {
            self.program.emit_insn(Insn::Affinity {
                start_reg: probe_key_start_reg,
                count,
                affinities: payload_info.key_affinities.clone(),
            });
        }

        if payload_info.use_bloom_filter && self.hash_join_op.join_type != HashJoinType::FullOuter {
            self.program.emit_insn(Insn::Filter {
                cursor_id: payload_info.bloom_filter_cursor_id,
                target_pc: self.next,
                key_reg: probe_key_start_reg,
                num_keys,
            });
        }

        let num_payload = payload_info.payload_columns.len();
        let payload_dest_reg = if num_payload > 0 {
            Some(self.program.alloc_registers(num_payload))
        } else {
            None
        };

        if matches!(self.hash_join_op.join_type, HashJoinType::FullOuter) {
            let probe_table_idx = self.hash_join_op.probe_table_idx;
            if let Some(lj_meta) = self.t_ctx.meta_left_joins[probe_table_idx].as_ref() {
                self.program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }

        let hash_probe_miss_label = if self.hash_join_op.join_type == HashJoinType::FullOuter {
            self.program.allocate_label()
        } else {
            self.next
        };

        let (probe_rowid_reg, grace_flag_reg) = {
            let rowid_reg = self.program.alloc_register();
            self.program.emit_insn(Insn::RowId {
                cursor_id: self.probe_cursor_id,
                dest: rowid_reg,
            });
            // grace_flag_reg: 0 during main probe loop, 1 during grace loop
            let flag_reg = self.program.alloc_register();
            self.program.emit_insn(Insn::Integer {
                value: 0,
                dest: flag_reg,
            });
            (Some(rowid_reg), Some(flag_reg))
        };

        let match_reg = self.program.alloc_register();
        self.program.emit_insn(Insn::HashProbe {
            hash_table_id: to_u32(hash_table_id),
            key_start_reg: to_u32(probe_key_start_reg),
            num_keys: to_u32(num_keys),
            dest_reg: to_u32(match_reg),
            target_pc: hash_probe_miss_label,
            payload_dest_reg: payload_dest_reg.map(to_u32),
            num_payload: to_u32(num_payload),
            // Main probe loop always carries the probe rowid so spilled build
            // partitions are deferred to grace processing instead of loaded here.
            probe_rowid_reg: probe_rowid_reg.map(to_u32),
        });

        let match_found_label = self.program.allocate_label();
        self.program.preassign_label_to_next_insn(match_found_label);
        let hash_next_label = self.program.allocate_label();

        Ok(ProbeSetupState {
            build_cursor_id,
            payload_info,
            payload_dest_reg,
            match_reg,
            hash_probe_miss_label,
            match_found_label,
            hash_next_label,
            probe_rowid_reg,
            key_start_reg: probe_key_start_reg,
            num_keys,
            grace_flag_reg,
        })
    }

    /// Install `HashCtx` and cache any payload-backed expressions for later reads.
    fn install_context(&mut self, state: ProbeSetupState) -> Result<()> {
        let ProbeSetupState {
            build_cursor_id,
            payload_info,
            payload_dest_reg,
            match_reg,
            hash_probe_miss_label,
            match_found_label,
            hash_next_label,
            probe_rowid_reg,
            key_start_reg,
            num_keys,
            grace_flag_reg,
        } = state;
        let build_table = &self.table_references.joined_tables()[self.hash_join_op.build_table_idx];
        let hash_table_id: usize = build_table.internal_id.into();
        let payload_columns = payload_info.payload_columns.clone();

        let mut labels = HashLabels::new(match_found_label, hash_next_label);
        if self.hash_join_op.join_type == HashJoinType::FullOuter {
            labels.check_outer = Some(hash_probe_miss_label);
        };
        self.t_ctx.hash_table_contexts.insert(
            self.hash_join_op.build_table_idx,
            HashCtx {
                labels,
                hash_table_reg: hash_table_id,
                match_reg,
                payload_start_reg: payload_dest_reg,
                payload_columns: payload_info.payload_columns,
                build_cursor_id: if payload_info.allow_seek {
                    Some(build_cursor_id)
                } else {
                    None
                },
                join_type: self.hash_join_op.join_type,
                inner_loop_gosub_reg: None,
                probe_rowid_reg,
                key_start_reg,
                num_keys,
                grace_flag_reg,
            },
        );

        self.t_ctx.resolver.enable_expr_to_reg_cache();
        let rowid_expr = Expr::RowId {
            database: None,
            table: build_table.internal_id,
        };
        let payload_has_build_rowid = payload_columns.iter().any(|payload| {
            matches!(
                payload,
                MaterializedColumnRef::RowId { table_id } if *table_id == build_table.internal_id
            )
        });
        let build_table_is_live = self.live_table_ids.contains(&build_table.internal_id);
        if payload_info.allow_seek && !payload_has_build_rowid && !build_table_is_live {
            self.t_ctx
                .resolver
                .cache_expr_reg(Cow::Owned(rowid_expr), match_reg, false, None);
        }
        if let Some(payload_reg) = payload_dest_reg {
            for (i, payload) in payload_columns.iter().enumerate() {
                let (payload_table_id, expr, is_column) = match payload {
                    MaterializedColumnRef::Column {
                        table_id,
                        column_idx,
                        is_rowid_alias,
                    } => (
                        *table_id,
                        Expr::Column {
                            database: None,
                            table: *table_id,
                            column: *column_idx,
                            is_rowid_alias: *is_rowid_alias,
                        },
                        true,
                    ),
                    MaterializedColumnRef::RowId { table_id } => (
                        *table_id,
                        Expr::RowId {
                            database: None,
                            table: *table_id,
                        },
                        false,
                    ),
                };
                if self.live_table_ids.contains(&payload_table_id) {
                    continue;
                }
                if is_column {
                    self.t_ctx.resolver.cache_scalar_expr_reg(
                        Cow::Owned(expr),
                        payload_reg + i,
                        true,
                        self.table_references,
                    )?;
                } else {
                    self.t_ctx.resolver.cache_expr_reg(
                        Cow::Owned(expr),
                        payload_reg + i,
                        false,
                        None,
                    );
                }
            }
        } else if payload_info.allow_seek && !build_table_is_live {
            self.program.emit_insn(Insn::SeekRowid {
                cursor_id: build_cursor_id,
                src_reg: match_reg,
                target_pc: hash_next_label,
            });
        }

        Ok(())
    }

    pub(super) fn emit(mut self) -> Result<()> {
        let prepared = self.prepare_build()?;
        let state = self.emit_probe(prepared)?;
        self.install_context(state)
    }
}

struct ProbeCloseState {
    label_next_probe_row: BranchOffset,
    semi_anti_next_anchor: Option<BranchOffset>,
}

/// Result of emitting hash-join probe teardown.
pub(super) struct HashProbeCloseOutcome {
    /// Preassigned label anchored at the point semi/anti-join `label_next_outer`
    /// labels should target. Callers link their labels to it via
    /// `ProgramBuilder::link_label_to_other_label`.
    pub semi_anti_next_anchor: Option<BranchOffset>,
}

/// Close-loop path of a hash-join probe.
///
/// The teardown remains ordered: emit `HashNext`, optionally emit FULL OUTER
/// unmatched probe rows, then return the probe-row advance state.
pub(super) struct HashProbeCloseEmitter<'a, 'plan> {
    program: &'a mut ProgramBuilder,
    t_ctx: &'a mut TranslateCtx<'plan>,
    hash_join_op: &'a HashJoinOp,
    hash_ctx: HashCtx,
    select_plan: Option<&'plan SelectPlan>,
    table_index: usize,
}

impl<'a, 'plan> HashProbeCloseEmitter<'a, 'plan> {
    /// Capture the mutable close-loop inputs for a single hash probe table.
    pub(super) fn new(
        program: &'a mut ProgramBuilder,
        t_ctx: &'a mut TranslateCtx<'plan>,
        hash_join_op: &'a HashJoinOp,
        hash_ctx: HashCtx,
        select_plan: Option<&'plan SelectPlan>,
        table_index: usize,
    ) -> Self {
        Self {
            program,
            t_ctx,
            hash_join_op,
            hash_ctx,
            select_plan,
            table_index,
        }
    }

    /// Emit `HashNext` and loop back into the existing match-processing path.
    fn emit_matched_iteration(&mut self) -> Result<ProbeCloseState> {
        let hash_table_reg = self.hash_ctx.hash_table_reg;
        let match_reg = self.hash_ctx.match_reg;
        let match_found_label = self.hash_ctx.labels.match_found;
        let hash_next_label = self.hash_ctx.labels.next;
        let payload_dest_reg = self.hash_ctx.payload_start_reg;
        let num_payload = self.hash_ctx.payload_columns.len();
        let check_outer_label = self.hash_ctx.labels.check_outer;
        let join_type = self.hash_ctx.join_type;
        let inner_loop_gosub_reg = self.hash_ctx.inner_loop_gosub_reg;
        let inner_loop_skip_label = self.hash_ctx.labels.inner_loop_skip;
        let label_next_probe_row = self.program.allocate_label();
        let mut semi_anti_next_anchor: Option<BranchOffset> = None;

        if let Some(gosub_reg) = inner_loop_gosub_reg {
            let return_anchor = self.program.allocate_label();
            self.program.preassign_label_to_next_insn(return_anchor);
            semi_anti_next_anchor = Some(return_anchor);
            self.program.emit_insn(Insn::Return {
                return_reg: gosub_reg,
                can_fallthrough: false,
            });
            if let Some(skip_label) = inner_loop_skip_label {
                self.program.preassign_label_to_next_insn(skip_label);
            }
        }

        let hash_next_target = if join_type == HashJoinType::FullOuter {
            check_outer_label.unwrap_or(label_next_probe_row)
        } else {
            label_next_probe_row
        };

        self.program.preassign_label_to_next_insn(hash_next_label);
        if semi_anti_next_anchor.is_none() {
            semi_anti_next_anchor = Some(hash_next_label);
        }

        // Grace dispatch: if grace_flag_reg > 0, jump to the grace loop's own
        // HashNext (which has a different miss target). This lets the inner body
        // be shared between the main probe loop and the grace loop.
        if let Some(grace_flag_reg) = self.hash_ctx.grace_flag_reg {
            let grace_hash_next_label = self.program.allocate_label();
            // Store in hash_ctx for the grace loop emitter to resolve later.
            let build_table_idx = self.hash_join_op.build_table_idx;
            if let Some(ctx) = self.t_ctx.hash_table_contexts.get_mut(&build_table_idx) {
                ctx.labels.grace_hash_next = Some(grace_hash_next_label);
            }
            self.program.emit_insn(Insn::IfPos {
                reg: grace_flag_reg,
                target_pc: grace_hash_next_label,
                decrement_by: 0,
            });
        }

        self.program.emit_insn(Insn::HashNext {
            hash_table_id: hash_table_reg,
            dest_reg: match_reg,
            target_pc: hash_next_target,
            payload_dest_reg,
            num_payload,
        });
        self.program.emit_insn(Insn::Goto {
            target_pc: match_found_label,
        });

        Ok(ProbeCloseState {
            label_next_probe_row,
            semi_anti_next_anchor,
        })
    }

    /// Emit FULL OUTER unmatched probe rows before advancing to the next probe row.
    fn emit_probe_miss_rows(&mut self, state: ProbeCloseState) -> Result<ProbeCloseState> {
        let ProbeCloseState {
            label_next_probe_row,
            semi_anti_next_anchor,
        } = state;

        if matches!(self.hash_ctx.join_type, HashJoinType::FullOuter) {
            let probe_table_idx = self.hash_join_op.probe_table_idx;
            let lj_meta = self.t_ctx.meta_left_joins[probe_table_idx]
                .as_ref()
                .expect("FULL OUTER probe table must have left join metadata");
            let reg_match_flag = lj_meta.reg_match_flag;

            if let Some(check_outer_label) = self.hash_ctx.labels.check_outer {
                self.program.preassign_label_to_next_insn(check_outer_label);
            }
            self.program
                .preassign_label_to_next_insn(lj_meta.label_match_flag_check_value);

            self.program.emit_insn(Insn::IfPos {
                reg: reg_match_flag,
                target_pc: label_next_probe_row,
                decrement_by: 0,
            });

            if let Some(cursor_id) = self.hash_ctx.build_cursor_id {
                self.program.emit_insn(Insn::NullRow { cursor_id });
            }

            if let Some(payload_reg) = self.hash_ctx.payload_start_reg {
                let num_payload = self.hash_ctx.payload_columns.len();
                if num_payload > 0 {
                    self.program.emit_insn(Insn::Null {
                        dest: payload_reg,
                        dest_end: Some(payload_reg + num_payload - 1),
                    });
                }
            }

            if let Some(plan) = self.select_plan {
                emit_unmatched_row_conditions_and_loop(
                    self.program,
                    self.t_ctx,
                    plan,
                    self.hash_join_op.build_table_idx,
                    self.table_index,
                    label_next_probe_row,
                    self.hash_ctx
                        .inner_loop_gosub_reg
                        .zip(self.hash_ctx.labels.inner_loop_gosub),
                    payload_regs(
                        self.hash_ctx.payload_start_reg,
                        self.hash_ctx.payload_columns.len(),
                    ),
                )?;
            }
        }

        Ok(ProbeCloseState {
            label_next_probe_row,
            semi_anti_next_anchor,
        })
    }

    /// Anchor the next probe-row label and return the close-loop control-flow state.
    fn finish(&mut self, state: ProbeCloseState) -> HashProbeCloseOutcome {
        let ProbeCloseState {
            label_next_probe_row,
            semi_anti_next_anchor,
        } = state;

        self.program
            .preassign_label_to_next_insn(label_next_probe_row);

        HashProbeCloseOutcome {
            semi_anti_next_anchor,
        }
    }

    pub(super) fn emit(mut self) -> Result<HashProbeCloseOutcome> {
        let state = self.emit_matched_iteration()?;
        let state = self.emit_probe_miss_rows(state)?;
        Ok(self.finish(state))
    }
}

/// Emit unmatched build rows after the probe cursor has been exhausted.
pub(super) fn emit_hash_join_unmatched_build_rows<'a>(
    program: &mut ProgramBuilder,
    t_ctx: &mut TranslateCtx<'a>,
    hash_join_op: &HashJoinOp,
    hash_ctx: &HashCtx,
    select_plan: Option<&'a SelectPlan>,
    table_index: usize,
    probe_cursor_id: CursorID,
) -> Result<()> {
    if !matches!(
        hash_join_op.join_type,
        HashJoinType::LeftOuter | HashJoinType::FullOuter
    ) {
        return Ok(());
    }
    let Some(plan) = select_plan else {
        return Ok(());
    };

    let hash_table_reg = hash_ctx.hash_table_reg;
    let match_reg = hash_ctx.match_reg;
    let payload_dest_reg = hash_ctx.payload_start_reg;
    let num_payload = hash_ctx.payload_columns.len();
    let build_cursor_id = hash_ctx.build_cursor_id;
    let done_unmatched = program.allocate_label();

    program.emit_insn(Insn::NullRow {
        cursor_id: probe_cursor_id,
    });

    program.emit_insn(Insn::HashScanUnmatched {
        hash_table_id: hash_table_reg,
        dest_reg: match_reg,
        target_pc: done_unmatched,
        payload_dest_reg,
        num_payload,
    });

    let unmatched_loop = program.allocate_label();
    let label_next_unmatched = program.allocate_label();
    program.preassign_label_to_next_insn(unmatched_loop);

    if let Some(cursor_id) = build_cursor_id {
        program.emit_insn(Insn::SeekRowid {
            cursor_id,
            src_reg: match_reg,
            target_pc: done_unmatched,
        });
    }

    emit_unmatched_row_conditions_and_loop(
        program,
        t_ctx,
        plan,
        hash_join_op.build_table_idx,
        table_index,
        label_next_unmatched,
        hash_ctx
            .inner_loop_gosub_reg
            .zip(hash_ctx.labels.inner_loop_gosub),
        payload_regs(payload_dest_reg, num_payload),
    )?;

    program.preassign_label_to_next_insn(label_next_unmatched);
    program.emit_insn(Insn::HashNextUnmatched {
        hash_table_id: hash_table_reg,
        dest_reg: match_reg,
        target_pc: done_unmatched,
        payload_dest_reg,
        num_payload,
    });
    program.emit_insn(Insn::Goto {
        target_pc: unmatched_loop,
    });
    program.preassign_label_to_next_insn(done_unmatched);
    Ok(())
}

/// The registers holding this hash join's payload, which the unmatched-row paths
/// refill for every row they emit. Empty when the join carries no payload.
fn payload_regs(start_reg: Option<usize>, num_payload: usize) -> Range<usize> {
    match start_reg {
        Some(start) => start..start + num_payload,
        None => 0..0,
    }
}

/// Grace Hash Join processing loop after the probe cursor is exhausted.
pub(crate) struct GraceHashLoop;

impl GraceHashLoop {
    /// Emit VDBE-driven grace hash join processing loop.
    /// Uses the shared inner body via `Goto match_found_label` and `grace_flag_reg`
    /// dispatch so that aggregates, LIMIT, ORDER BY, etc. all work naturally.
    /// At runtime, HashGraceInit is a no-op if the build side didn't spill.
    pub fn emit<'a>(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx<'a>,
        hash_join_op: &HashJoinOp,
        hash_ctx: &HashCtx,
        select_plan: Option<&'a SelectPlan>,
        table_index: usize,
        probe_cursor_id: CursorID,
    ) -> Result<()> {
        // Need grace_flag_reg + probe_rowid_reg for grace processing
        let Some(probe_rowid_reg) = hash_ctx.probe_rowid_reg else {
            return Ok(());
        };
        let Some(grace_flag_reg) = hash_ctx.grace_flag_reg else {
            return Ok(());
        };

        let hash_table_reg = hash_ctx.hash_table_reg;
        let match_reg = hash_ctx.match_reg;
        let match_found_label = hash_ctx.labels.match_found;
        let payload_dest_reg = hash_ctx.payload_start_reg;
        let num_payload = hash_ctx.payload_columns.len();
        let is_full_outer = hash_join_op.join_type == HashJoinType::FullOuter;

        let grace_done = program.allocate_label();
        let grace_partition_top = program.allocate_label();
        let grace_probe_top = program.allocate_label();
        let grace_advance = program.allocate_label();
        let grace_cleanup = program.allocate_label();

        // HashGraceInit: finalize probe spill + grace_begin
        program.emit_insn(Insn::HashGraceInit {
            hash_table_id: to_u32(hash_table_reg),
            target_pc: grace_done,
        });

        // Set grace mode flag = 1
        program.emit_insn(Insn::Integer {
            value: 1,
            dest: grace_flag_reg,
        });

        // grace_partition_top: load build partition + first probe chunk
        program.preassign_label_to_next_insn(grace_partition_top);
        program.emit_insn(Insn::HashGraceLoadPartition {
            hash_table_id: to_u32(hash_table_reg),
            target_pc: grace_cleanup,
        });

        // grace_probe_top: get next probe entry (writes keys + rowid to registers)
        program.preassign_label_to_next_insn(grace_probe_top);

        // FULL OUTER: reset match flag before each probe entry so we can detect misses
        if is_full_outer {
            let probe_table_idx = hash_join_op.probe_table_idx;
            if let Some(lj_meta) = t_ctx.meta_left_joins[probe_table_idx].as_ref() {
                program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: lj_meta.reg_match_flag,
                });
            }
        }

        program.emit_insn(Insn::HashGraceNextProbe {
            hash_table_id: to_u32(hash_table_reg),
            key_start_reg: to_u32(hash_ctx.key_start_reg),
            num_keys: to_u32(hash_ctx.num_keys),
            probe_rowid_dest: to_u32(probe_rowid_reg),
            target_pc: grace_advance,
        });

        // Re-position probe cursor via SeekRowid
        program.emit_insn(Insn::SeekRowid {
            cursor_id: probe_cursor_id,
            src_reg: probe_rowid_reg,
            target_pc: grace_probe_top,
        });

        // For FULL OUTER, HashProbe miss needs to go to the outer-check path
        // (emit unmatched probe row with NULL build columns).
        // For INNER/LEFT OUTER, miss just advances to next probe entry.
        let grace_outer_check = if is_full_outer {
            program.allocate_label()
        } else {
            grace_probe_top
        };

        // HashProbe the loaded build partition with the probe keys
        program.emit_insn(Insn::HashProbe {
            hash_table_id: to_u32(hash_table_reg),
            key_start_reg: to_u32(hash_ctx.key_start_reg),
            num_keys: to_u32(hash_ctx.num_keys),
            dest_reg: to_u32(match_reg),
            target_pc: grace_outer_check,
            payload_dest_reg: payload_dest_reg.map(to_u32),
            num_payload: to_u32(num_payload),
            probe_rowid_reg: None, // grace-only: HashGraceLoadPartition already loaded this partition
        });

        // Jump INTO the shared inner body (conditions, result columns, aggregation).
        // The IfPos dispatch before the main loop's HashNext will route back here.
        program.emit_insn(Insn::Goto {
            target_pc: match_found_label,
        });

        // grace_hash_next: the grace loop's own HashNext, reached via IfPos dispatch
        // from the shared body.
        if let Some(grace_hash_next_label) = hash_ctx.labels.grace_hash_next {
            program.preassign_label_to_next_insn(grace_hash_next_label);
        }

        // For FULL OUTER, HashNext miss goes to outer check (unmatched probe row).
        // For INNER/LEFT OUTER, miss advances to next probe entry.
        program.emit_insn(Insn::HashNext {
            hash_table_id: hash_table_reg,
            dest_reg: match_reg,
            target_pc: grace_outer_check,
            payload_dest_reg,
            num_payload,
        });
        // Another match found, loop back to shared body
        program.emit_insn(Insn::Goto {
            target_pc: match_found_label,
        });

        // FULL OUTER: unmatched probe row path.
        // If match_flag is still 0, emit the probe row with NULL build columns.
        if is_full_outer {
            program.preassign_label_to_next_insn(grace_outer_check);

            let probe_table_idx = hash_join_op.probe_table_idx;
            if let Some(lj_meta) = t_ctx.meta_left_joins[probe_table_idx].as_ref() {
                // If match_flag > 0, a match was found, skip to next probe entry
                program.emit_insn(Insn::IfPos {
                    reg: lj_meta.reg_match_flag,
                    target_pc: grace_probe_top,
                    decrement_by: 0,
                });
            }

            // Set build cursor to NULL row
            if let Some(cursor_id) = hash_ctx.build_cursor_id {
                program.emit_insn(Insn::NullRow { cursor_id });
            }

            // NULL out payload registers
            if let Some(payload_reg) = hash_ctx.payload_start_reg {
                if num_payload > 0 {
                    program.emit_insn(Insn::Null {
                        dest: payload_reg,
                        dest_end: Some(payload_reg + num_payload - 1),
                    });
                }
            }

            // Emit the unmatched row through the shared body
            if let Some(plan) = select_plan {
                emit_unmatched_row_conditions_and_loop(
                    program,
                    t_ctx,
                    plan,
                    hash_join_op.build_table_idx,
                    table_index,
                    grace_probe_top,
                    hash_ctx
                        .inner_loop_gosub_reg
                        .zip(hash_ctx.labels.inner_loop_gosub),
                    payload_regs(payload_dest_reg, num_payload),
                )?;
            }

            // Advance to next probe entry
            program.emit_insn(Insn::Goto {
                target_pc: grace_probe_top,
            });
        }

        // grace_advance: probe entries exhausted for this partition.
        program.preassign_label_to_next_insn(grace_advance);

        // LEFT/FULL OUTER: emit unmatched build rows for this partition BEFORE evicting.
        // After eviction, matched_bits are lost, so the global unmatched scan can't
        // see which build rows were matched during grace probing.
        if matches!(
            hash_join_op.join_type,
            HashJoinType::LeftOuter | HashJoinType::FullOuter
        ) {
            if let Some(plan) = select_plan {
                let done_grace_unmatched = program.allocate_label();
                let grace_unmatched_loop = program.allocate_label();
                let grace_next_unmatched = program.allocate_label();

                // Set probe cursor to NULL row (unmatched build rows have no probe match)
                program.emit_insn(Insn::NullRow {
                    cursor_id: probe_cursor_id,
                });

                program.emit_insn(Insn::HashScanUnmatched {
                    hash_table_id: hash_table_reg,
                    dest_reg: match_reg,
                    target_pc: done_grace_unmatched,
                    payload_dest_reg,
                    num_payload,
                });

                program.preassign_label_to_next_insn(grace_unmatched_loop);

                if let Some(cursor_id) = hash_ctx.build_cursor_id {
                    program.emit_insn(Insn::SeekRowid {
                        cursor_id,
                        src_reg: match_reg,
                        target_pc: done_grace_unmatched,
                    });
                }

                emit_unmatched_row_conditions_and_loop(
                    program,
                    t_ctx,
                    plan,
                    hash_join_op.build_table_idx,
                    table_index,
                    grace_next_unmatched,
                    hash_ctx
                        .inner_loop_gosub_reg
                        .zip(hash_ctx.labels.inner_loop_gosub),
                    payload_regs(payload_dest_reg, num_payload),
                )?;

                program.preassign_label_to_next_insn(grace_next_unmatched);
                program.emit_insn(Insn::HashNextUnmatched {
                    hash_table_id: hash_table_reg,
                    dest_reg: match_reg,
                    target_pc: done_grace_unmatched,
                    payload_dest_reg,
                    num_payload,
                });
                program.emit_insn(Insn::Goto {
                    target_pc: grace_unmatched_loop,
                });
                program.preassign_label_to_next_insn(done_grace_unmatched);
            }
        }

        // Evict current partition, advance to next
        program.emit_insn(Insn::HashGraceAdvancePartition {
            hash_table_id: to_u32(hash_table_reg),
            target_pc: grace_cleanup,
        });
        program.emit_insn(Insn::Goto {
            target_pc: grace_partition_top,
        });

        // grace_cleanup: clear grace mode flag
        program.preassign_label_to_next_insn(grace_cleanup);
        program.emit_insn(Insn::Integer {
            value: 0,
            dest: grace_flag_reg,
        });

        // grace_done
        program.preassign_label_to_next_insn(grace_done);
        Ok(())
    }
}
