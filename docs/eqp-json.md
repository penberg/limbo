# Machine-readable execution plan (`EXPLAIN QUERY PLAN FORMAT=JSON`)

Turso can output machine-readable execution plans with `EXPLAIN QUERY PLAN FORMAT=JSON`. For example:

```console
tursodb> EXPLAIN QUERY PLAN FORMAT=JSON SELECT * FROM users WHERE age > 21;
{"version":1,"sql":"EXPLAIN QUERY PLAN FORMAT=JSON SELECT * FROM users WHERE age > 21;","result_columns":["id","name","age"],"nodes":[...]}
```

`FORMAT=TEXT` (or omitting the clause) gives the classic four-column rows. The clause is case-insensitive, and returns
one row with one TEXT column named `plan_json`.

This can be used to monitor the execution plans, or to visualize them. For example,
the [Turso plan visualizer](https://github.com/tursodatabase/turso-explain) generates a graph visualization that can be
used to optimize queries.

## The JSON format

The JSON format is as below, but is not stable yet and may change without warning between minor versions.

```json
{
  "version": 1,
  "sql": "EXPLAIN QUERY PLAN SELECT ...",
  "result_columns": [
    "name",
    "amount"
  ],
  "nodes": [
    {
      "id": 2,
      "parent": null,
      "detail": "SEARCH o USING INDEX idx_orders_user (user_id=?) LEFT-JOIN",
      "op": {
        "type": "search",
        "table": "orders",
        "alias": "o",
        "join": "left",
        "search_kind": "seek",
        "index": {
          "name": "idx_orders_user",
          "covering": false,
          "ephemeral": false
        },
        "constraints": [
          "user_id=?"
        ]
      }
    }
  ],
  "cte_materializations": [
    {
      "cte_id": 1,
      "name": "spenders",
      "nodes": [
        4,
        7
      ]
    }
  ]
}
```

`nodes` lists the same rows `EXPLAIN QUERY PLAN` prints, in the same order:
`id`/`parent` encode the tree (`parent: null` marks a root) and `detail` is
the exact display string. `op` adds the structured fields, discriminated by
`op.type`:

| `op.type`                            | Meaning                                 | Extra fields                                                                                                                                         |
|--------------------------------------|-----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| `scan`                               | iterate a row source                    | `table`, `alias?`, `source` (`table`/`virtual_table`/`subquery`/`recursive_cte_input`), `index?` , `backwards?`, `join?`, `subquery?`                |
| `search`                             | seek by key                             | `table`, `alias?`, `search_kind` (`rowid_eq`/`seek`/`in_seek`), `index?` or `integer_primary_key`, `constraints`, `backwards?`, `join?`, `subquery?` |
| `multi_index`                        | combine rowid sets from several indexes | `set_op` (`or`/`and`), `indexes`                                                                                                                     |
| `index_method`                       | pluggable index method access           | `method`                                                                                                                                             |
| `hash_join`                          | hash side of a hash join                | `table`, `alias?`, `join?`, `subquery?`                                                                                                              |
| `hash_build`                         | materialize a hash join's build input   | `table`, `alias?`                                                                                                                                    |
| `distinct` / `distinct_aggregate`    | hash-table de-duplication               | `function` (aggregate only)                                                                                                                          |
| `order_by` / `group_by`              | sorting stage                           | `method` (`sorter`/`temp_btree`)                                                                                                                     |
| `compound` / `compound_arm`          | compound select and its arms            | `op` (`union_all`/`union`/`intersect`/`except`/`left_most`), `temp_btree`                                                                            |
| `list_subquery` / `scalar_subquery`  | `IN (SELECT ...)` / scalar subquery     | `subquery_id`, `correlated`                                                                                                                          |
| `recursive_setup` / `recursive_step` | recursive CTE phases                    |                                                                                                                                                      |
| `constant_row`                       | query with no FROM clause               |                                                                                                                                                      |

Fields that are absent are simply omitted (e.g. `join` on the first table,
`index` on a rowid search).

Table access operations can include an `estimate` object. This object reports the values that selected the join plan.

| Field | Meaning |
|-------|---------|
| `input_rows` | Rows from the join prefix before this table access. |
| `rows_per_input` | Rows that this access returns for each input row. |
| `output_rows` | Rows from the join prefix after this table access. |
| `access_cost` | Cost of this table access for all input rows. |
| `total_cost` | Cost of the join prefix through this table access. |

These values are estimates, not execution counters. Use statement metrics to compare them with the work from an executed query.
