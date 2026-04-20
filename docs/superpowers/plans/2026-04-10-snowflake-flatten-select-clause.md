# Snowflake FLATTEN: SELECT-Clause UNNEST Fixes

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix three broken UNNEST-in-SELECT patterns so they produce valid Snowflake LATERAL FLATTEN SQL.

**Architecture:** The core FLATTEN logic works for `SELECT * FROM UNNEST(...)` and `SELECT * FROM t, UNNEST(t.col)`. The remaining bugs are all about how the unparser handles aliases, multi-expression projections, and SubqueryAlias nodes when FLATTEN mode is active. Each broken case has a different root cause and needs a targeted fix.

**Tech Stack:** Rust, sqlparser 0.61, DataFusion SQL unparser

---

## Broken Cases

### Case A: `SELECT UNNEST([1,2,3]) as c1`

**Current output:** `SELECT "_unnest"."VALUE" AS "c1"` (no FROM clause)  
**Correct output:** `SELECT _unnest."VALUE" AS "c1" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS _unnest`

**Root cause:** This query has an *outer* Projection with the user alias `c1` wrapping an *inner* Projection+Unnest that is the table source. The outer Projection has `p.expr.len() == 1` and the expression IS an unnest placeholder — so it enters the FLATTEN code path at plan.rs:420. The FLATTEN relation is set on the `relation` builder. But then `reconstruct_select_statement` runs on the outer Projection and builds the SELECT list from the alias expression. The relation builder's FLATTEN gets properly built into the FROM clause.

Wait — let me verify this is actually broken. The explorer said `SELECT UNNEST([1,2,3]) as c1` produces `SELECT "_unnest"."VALUE" AS "c1"` with no FROM, but `SELECT UNNEST([1,2,3])` (no alias) works. Let me check what plan tree the alias version produces.

Actually, looking more carefully: `SELECT UNNEST([1,2,3]) as c1` is parsed differently from `SELECT * FROM UNNEST([1,2,3])`. The first puts UNNEST in the SELECT clause (expression context), the second puts it in FROM (table factor context). These generate completely different plan trees:

- `SELECT * FROM UNNEST([1,2,3])`: `Projection(Unnest(Projection(EmptyRelation)))`
- `SELECT UNNEST([1,2,3]) as c1`: `Projection(Unnest(Projection(EmptyRelation)))` with different expression structure

The key difference is how `reconstruct_select_statement` processes the outer projection — when the UNNEST is in SELECT position, the plan tree looks like:

```
Projection: cols=[__unnest_placeholder(...,depth=1) AS "UNNEST(...)" AS c1]
  Unnest: ...
    Projection: cols=[... AS __unnest_placeholder(...)]
      EmptyRelation
```

The outer Projection has a **double-aliased** expression. The unparser's `reconstruct_select_statement` calls `find_unnest_node_within_select(plan)` which finds the Unnest node, then `unproject_unnest_expr_as_flatten_value` rewrites the placeholder to `_unnest.VALUE`. But the FROM clause is built later — and the question is whether the FLATTEN relation builder gets set.

The issue is that when UNNEST is in SELECT position (not FROM), the code path goes through `reconstruct_select_statement` (line 487 in the Projection handler) which handles the SELECT list, but then recurses into `p.input` (the Unnest node). The Unnest handler at line 1064-1083 skips the inner Projection and recurses to EmptyRelation. At this point, no relation has been set — the FROM clause is empty.

**Fix:** When `find_unnest_node_within_select` finds an Unnest node AND the dialect is FLATTEN, we need to also set the FLATTEN relation on the `relation` builder. This is the missing link — `reconstruct_select_statement` rewrites the SELECT expressions but nobody sets the FROM clause.

### Case B: `SELECT UNNEST([1,2,3]), 1`

**Current output:** `SELECT "_unnest"."VALUE" AS "UNNEST(...)", "Int64(1)"` (no FROM clause)  
**Correct output:** `SELECT _unnest."VALUE", Int64(1) FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS _unnest`

**Root cause:** `p.expr.len() == 2`, so the unnest placeholder check at line 415 returns `None`. The code falls through to `reconstruct_select_statement` which calls `find_unnest_node_within_select`. According to the explorer, this returns `None` because the plan has a CrossJoin (the parser puts the literal `1` in a separate relation joined with CROSS JOIN). `find_unnest_node_within_select` doesn't traverse multi-input nodes.

Same fundamental issue as Case A: SELECT list gets partially rewritten but no FLATTEN relation is set.

### Case C: `SELECT * FROM UNNEST([1,2,3]) AS t1 (c1)`

**Current output:** `SELECT "t1"."c1" FROM (SELECT "_unnest"."VALUE" AS "c1") AS "t1"`  
**Correct output:** `SELECT "t1"."c1" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS "t1"` (or similar — the column rename `c1` maps to `.VALUE`)

**Root cause:** The plan tree has a `SubqueryAlias(t1, Projection(Unnest(...)))`. The SubqueryAlias handler wraps the inner plan in a derived subquery. The inner FLATTEN works, but it gets wrapped in `(SELECT ...) AS t1` instead of directly aliasing the FLATTEN.

---

## File Map

| File | What changes |
|---|---|
| `datafusion/sql/src/unparser/plan.rs` | In `reconstruct_select_statement`: when FLATTEN dialect is active and an Unnest node is found, also set the FLATTEN relation. In the Unnest handler: when FLATTEN dialect and relation not yet set, build FLATTEN relation. |
| `datafusion/sql/src/unparser/utils.rs` | Potentially extend `find_unnest_node_within_select` to handle CrossJoin |
| `datafusion/sql/tests/cases/plan_to_sql.rs` | Add/fix test expectations for the 3 cases |

---

## Task 1: Fix Case A — `SELECT UNNEST([1,2,3]) as c1`

The Unnest handler at plan.rs:1064 needs to set the FLATTEN relation when the dialect requires it and no relation has been set yet.

**Files:**
- Modify: `datafusion/sql/src/unparser/plan.rs:1064-1083`

- [ ] **Step 1: Add a test with correct expected output**

```rust
#[test]
fn snowflake_flatten_select_unnest_with_alias() -> Result<(), DataFusionError> {
    let snowflake = SnowflakeDialect::new();
    roundtrip_statement_with_dialect_helper!(
        sql: "SELECT UNNEST([1,2,3]) as c1",
        parser_dialect: GenericDialect {},
        unparser_dialect: snowflake,
        expected: @"PLACEHOLDER",  // discover, then fix
    );
    Ok(())
}
```

- [ ] **Step 2: In the Unnest handler, add FLATTEN relation setup**

In the `LogicalPlan::Unnest(unnest)` match arm (plan.rs:1064), when the dialect uses FLATTEN and the relation hasn't been set yet, build and set the FLATTEN relation:

```rust
LogicalPlan::Unnest(unnest) => {
    // ... existing struct_type_columns checks ...

    // For Snowflake FLATTEN: if the relation hasn't been set yet
    // (UNNEST was in SELECT clause, not FROM clause), set the FLATTEN
    // relation here so the FROM clause is emitted.
    if self.dialect.unnest_as_lateral_flatten()
        && !relation.has_relation()
        && let Some(flatten_relation) =
            self.try_unnest_to_lateral_flatten_sql(unnest)?
    {
        relation.flatten(flatten_relation);
    }

    if let LogicalPlan::Projection(p) = unnest.input.as_ref() {
        self.select_to_sql_recursively(&p.input, query, select, relation)
    } else {
        internal_err!("Unnest input is not a Projection: {unnest:?}")
    }
}
```

- [ ] **Step 3: Run test, discover actual output, fix expected**
- [ ] **Step 4: Commit**

---

## Task 2: Fix Case B — `SELECT UNNEST([1,2,3]), 1`

This requires extending the UNNEST detection to work when `find_unnest_node_within_select` can't find the Unnest node (because it's behind a CrossJoin).

**Files:**
- Modify: `datafusion/sql/src/unparser/plan.rs` — the `_ =>` branch in `reconstruct_select_statement`
- Possibly modify: `datafusion/sql/src/unparser/utils.rs` — extend `find_unnest_node_within_select`

- [ ] **Step 1: Add test with placeholder expected**
- [ ] **Step 2: Extend `find_unnest_node_within_select` or add fallback handling in `reconstruct_select_statement`**

The simplest approach: in `reconstruct_select_statement`'s `_ =>` branch (line 296-318), when the Snowflake dialect is active, detect UNNEST expressions by pattern matching on Alias/Column names containing `UNNEST_COLUMN_PREFIX`, and also set the FLATTEN relation if not already set.

The existing code at line 307-312 already tries this for bare `Expr::Column` but doesn't handle `Expr::Alias(Expr::Column(...))`. Fix it to unwrap aliases.

- [ ] **Step 3: Run test, fix expected, verify**
- [ ] **Step 4: Commit**

---

## Task 3: Fix Case C — `SELECT * FROM UNNEST([1,2,3]) AS t1 (c1)`

The SubqueryAlias handler wraps the inner FLATTEN in a derived subquery. For FLATTEN dialects, it should pass the alias through to the FLATTEN relation instead.

**Files:**
- Modify: `datafusion/sql/src/unparser/plan.rs` — SubqueryAlias handler

- [ ] **Step 1: Add test with placeholder expected**
- [ ] **Step 2: In SubqueryAlias handler, detect FLATTEN case and pass alias through**

When the SubqueryAlias wraps a Projection+Unnest, and the dialect uses FLATTEN, skip the subquery wrapping and instead recurse into the inner plan with the alias applied to the FLATTEN builder.

- [ ] **Step 3: Run test, fix expected, verify**
- [ ] **Step 4: Commit**

---

## Task 4: Regression check

- [ ] Run all snowflake tests
- [ ] Run all 45+ existing dialect tests
- [ ] Run clippy + fmt
- [ ] Commit any fixes

---

## Notes

These 3 cases (A, B, C) are progressively harder. Case A is likely a small fix in the Unnest handler. Case B requires extending the expression detection logic. Case C requires intercepting the SubqueryAlias handler. If any of these prove too complex, they can be deferred — the core FLATTEN functionality (the 5 working test cases) covers the most common patterns.
