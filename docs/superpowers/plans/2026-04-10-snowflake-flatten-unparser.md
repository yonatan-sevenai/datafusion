# Snowflake LATERAL FLATTEN Unparser — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When the `SnowflakeDialect` is active, unparse `LogicalPlan::Unnest` into valid Snowflake SQL that uses `LATERAL FLATTEN(INPUT => expr) AS alias` and references `alias.VALUE` in the SELECT list.

**Architecture:** The current implementation correctly emits `LATERAL FLATTEN(...)` in the FROM clause but does not rewrite SELECT-list expressions — they still use DataFusion's internal `__unnest_placeholder(...)` names or bare `UNNEST(expr)` calls, which are not valid Snowflake SQL. The fix requires two things: (1) assigning an alias to each FLATTEN table factor, and (2) rewriting any projection expression that references an unnested column to use `<alias>.VALUE` (for arrays) or `<alias>.KEY`/`<alias>.VALUE` (for objects). This rewrite happens in `plan.rs` when the Projection handler encounters FLATTEN-mode unnest.

**Tech Stack:** Rust, sqlparser 0.61, DataFusion SQL unparser

---

## Problem Statement

Current output for `SELECT * FROM UNNEST([1,2,3])`:
```sql
SELECT "UNNEST(make_array(Int64(1),Int64(2),Int64(3)))" FROM LATERAL FLATTEN(INPUT => [1, 2, 3])
```

Correct Snowflake output:
```sql
SELECT _unnest.VALUE FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS _unnest
```

Current output for `SELECT * FROM unnest_table u, UNNEST(u.array_col)`:
```sql
SELECT "u"."array_col", "u"."struct_col", "UNNEST(outer_ref(u.array_col))"
  FROM "unnest_table" AS "u"
  CROSS JOIN LATERAL FLATTEN(INPUT => "u"."array_col")
```

Correct Snowflake output:
```sql
SELECT "u"."array_col", "u"."struct_col", _unnest."VALUE"
  FROM "unnest_table" AS "u"
  CROSS JOIN LATERAL FLATTEN(INPUT => "u"."array_col") AS _unnest
```

There are two bugs:
1. **No alias on the FLATTEN table factor.** FLATTEN needs an alias (e.g. `AS _unnest`) so columns can reference it.
2. **No SELECT-list rewrite.** Expressions referencing the unnested column still use DataFusion internal names (`__unnest_placeholder(...)` or `UNNEST(expr)`). They must be rewritten to `<alias>.VALUE`.

## Key Code Understanding

### How the plan tree looks

For `SELECT * FROM UNNEST([1,2,3])`:
```
Projection: __unnest_placeholder(make_array(1,2,3), depth=1) AS "UNNEST(make_array(...))"
  Unnest: list_type_columns=[__unnest_placeholder(make_array(1,2,3), depth=1)]
    Projection: [1,2,3] AS __unnest_placeholder(make_array(1,2,3))
      EmptyRelation
```

For `SELECT * FROM unnest_table u, UNNEST(u.array_col)`:
```
Projection: u.array_col, u.struct_col, __unnest_placeholder(outer_ref(u.array_col), depth=1)
  CrossJoin
    SubqueryAlias: u
      TableScan: unnest_table
    Subquery
      Projection: __unnest_placeholder(outer_ref(u.array_col), depth=1)
        Unnest: list_type_columns=[__unnest_placeholder(outer_ref(u.array_col), depth=1)]
          Projection: u.array_col AS __unnest_placeholder(outer_ref(u.array_col))
            EmptyRelation
```

### How the unparser visits the tree

The Projection handler in `plan.rs:375` is the key decision point:

1. It checks if this is a single-expression projection with an `__unnest_placeholder(...)` column name.
2. If the dialect returns `unnest_as_lateral_flatten() == true`, it calls `try_unnest_to_lateral_flatten_sql()` and sets `relation.flatten(...)`, then continues recursing.
3. Eventually `reconstruct_select_statement()` runs on the *outer* projection (the one with the actual SELECT columns). It calls `unproject_unnest_expr()` which transforms `__unnest_placeholder(...)` column refs back into `Expr::Unnest(...)`, which then unparses as the SQL function call `UNNEST(...)`.

**The problem:** Step 3 always transforms to `Expr::Unnest(...)` regardless of dialect. For Snowflake, it should instead produce a qualified column reference `<alias>.VALUE`.

### Two distinct code paths

There are two separate scenarios that need different handling:

**Path A: UNNEST-as-table-factor** (`SELECT * FROM UNNEST([1,2,3])`)
- The unnest IS the FROM source.
- The single-expression Projection at `plan.rs:384-401` is detected and the FLATTEN relation is set.
- The recursion continues into the Unnest node, which skips its inner Projection.
- There is no outer Projection that runs `reconstruct_select_statement` — the SELECT list comes from the placeholder projection itself.
- **Fix needed:** When setting `relation.flatten(...)`, also inject the alias into the builder AND rewrite the select item from `__unnest_placeholder(...)` to `<alias>.VALUE`.

**Path B: UNNEST-as-lateral-join** (`SELECT * FROM t, UNNEST(t.col)`)
- The table `t` is the primary FROM source. The unnest is a CROSS JOIN lateral subquery.
- The outer Projection hits `reconstruct_select_statement()` which calls `unproject_unnest_expr()`.
- `unproject_unnest_expr` converts `__unnest_placeholder(...)` columns into `Expr::Unnest(inner_expr)`.
- `Expr::Unnest` then unparses via `unnest_to_sql()` to the SQL function call `UNNEST(inner_expr)`.
- **Fix needed:** For the Snowflake dialect, `unproject_unnest_expr` (or the expression-level unparsing) needs to produce `<alias>.VALUE` instead of `UNNEST(inner_expr)`.

---

## File Map

| File | What changes |
|---|---|
| `datafusion/sql/src/unparser/ast.rs` | `FlattenRelationBuilder`: always assign a default alias; add a method to retrieve the alias name |
| `datafusion/sql/src/unparser/plan.rs` | Path A: rewrite the select item when setting FLATTEN relation. Path B: pass flatten alias context into unproject, or rewrite after unproject |
| `datafusion/sql/src/unparser/utils.rs` | Add `unproject_unnest_expr_as_flatten_value()` — variant of `unproject_unnest_expr` that produces `<alias>.VALUE` |
| `datafusion/sql/tests/cases/plan_to_sql.rs` | Fix existing 3 test expectations; add tests for aliased unnest, `SELECT UNNEST(col)` (implicit FROM) |

---

## Task 1: Fix test expectations to define correct behavior

The 3 existing Snowflake tests currently pass with wrong expected output. Update them first to define the *correct* expected output, then verify they fail.

**Files:**
- Modify: `datafusion/sql/tests/cases/plan_to_sql.rs:2999-3035`

- [ ] **Step 1: Update test expectations to correct Snowflake SQL**

```rust
#[test]
fn snowflake_unnest_to_lateral_flatten_simple() -> Result<(), DataFusionError> {
    let snowflake = SnowflakeDialect::new();
    roundtrip_statement_with_dialect_helper!(
        sql: "SELECT * FROM UNNEST([1,2,3])",
        parser_dialect: GenericDialect {},
        unparser_dialect: snowflake,
        expected: @r#"SELECT _unnest."VALUE" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS _unnest"#,
    );
    Ok(())
}

#[test]
fn snowflake_unnest_to_lateral_flatten_with_cross_join() -> Result<(), DataFusionError>
{
    let snowflake = SnowflakeDialect::new();
    roundtrip_statement_with_dialect_helper!(
        sql: "SELECT * FROM UNNEST([1,2,3]), j1",
        parser_dialect: GenericDialect {},
        unparser_dialect: snowflake,
        expected: @r#"SELECT _unnest."VALUE", "j1"."j1_id", "j1"."j1_string" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS _unnest CROSS JOIN "j1""#,
    );
    Ok(())
}

#[test]
fn snowflake_unnest_to_lateral_flatten_outer_ref() -> Result<(), DataFusionError> {
    let snowflake = SnowflakeDialect::new();
    roundtrip_statement_with_dialect_helper!(
        sql: "SELECT * FROM unnest_table u, UNNEST(u.array_col)",
        parser_dialect: GenericDialect {},
        unparser_dialect: snowflake,
        expected: @r#"SELECT "u"."array_col", "u"."struct_col", _unnest."VALUE" FROM "unnest_table" AS "u" CROSS JOIN LATERAL FLATTEN(INPUT => "u"."array_col") AS _unnest"#,
    );
    Ok(())
}
```

- [ ] **Step 2: Run tests, confirm they fail**

Run: `cargo test -p datafusion-sql snowflake_unnest 2>&1 | tail -20`
Expected: 3 FAILED — the actual output still has `__unnest_placeholder(...)` names and no alias on FLATTEN.

- [ ] **Step 3: Commit the red tests**

```bash
git add datafusion/sql/tests/cases/plan_to_sql.rs
git commit -m "test: define correct expected output for Snowflake FLATTEN unparsing

The existing tests pass with broken SQL output — the SELECT list
still uses DataFusion internal names (__unnest_placeholder) instead
of Snowflake's alias.VALUE convention. Update expectations to the
correct Snowflake SQL so these tests will drive the implementation."
```

---

## Task 2: Auto-assign alias on FlattenRelationBuilder

Every FLATTEN table factor needs an alias so the SELECT list can reference `alias.VALUE`. If the caller doesn't provide one, the builder should assign a default.

**Files:**
- Modify: `datafusion/sql/src/unparser/ast.rs:700-770`

- [ ] **Step 1: Add default alias generation to FlattenRelationBuilder**

Add a constant and update `create_empty` to assign a default alias:

```rust
/// Default table alias for FLATTEN table factors.
/// Snowflake requires an alias to reference output columns (e.g. `_unnest.VALUE`).
const FLATTEN_DEFAULT_ALIAS: &str = "_unnest";
```

Update `FlattenRelationBuilder::create_empty()`:

```rust
fn create_empty() -> Self {
    Self {
        alias: Some(ast::TableAlias {
            name: ast::Ident::new(FLATTEN_DEFAULT_ALIAS),
            columns: vec![],
        }),
        input_expr: None,
        outer: false,
    }
}
```

Add a getter so callers can retrieve the alias name:

```rust
/// Returns the alias name for this FLATTEN relation.
/// Used to build qualified column references like `alias.VALUE`.
pub fn alias_name(&self) -> &str {
    self.alias
        .as_ref()
        .map(|a| a.name.value.as_str())
        .unwrap_or(FLATTEN_DEFAULT_ALIAS)
}
```

- [ ] **Step 2: Verify compilation**

Run: `cargo check -p datafusion-sql`
Expected: compiles cleanly (tests still fail — we haven't rewritten the SELECT list yet).

- [ ] **Step 3: Commit**

```bash
git add datafusion/sql/src/unparser/ast.rs
git commit -m "feat: auto-assign _unnest alias to FlattenRelationBuilder

Every LATERAL FLATTEN needs a table alias so the SELECT list can
reference its output columns (e.g. _unnest.VALUE). Default to
'_unnest' if no alias is explicitly set."
```

---

## Task 3: Rewrite SELECT-list expressions for Path A (UNNEST-as-table-factor)

Path A is `SELECT * FROM UNNEST([1,2,3])`. The single-expression Projection at `plan.rs:389` sets the FLATTEN relation but doesn't rewrite the select item. Fix it.

**Files:**
- Modify: `datafusion/sql/src/unparser/plan.rs:389-401`

- [ ] **Step 1: Rewrite the Projection handler to inject `alias.VALUE` for FLATTEN**

When the FLATTEN branch fires, instead of just setting the relation and recursing blindly, we need to:
1. Get the alias name from the FlattenRelationBuilder.
2. Rewrite the select item to `<alias>."VALUE"`.
3. Set `select.already_projected()` so the downstream doesn't add a duplicate SELECT.

Replace the current FLATTEN block at `plan.rs:389-401`:

```rust
if self.dialect.unnest_as_lateral_flatten()
    && unnest_input_type.is_some()
    && let LogicalPlan::Unnest(unnest) = p.input.as_ref()
    && let Some(flatten_relation) =
        self.try_unnest_to_lateral_flatten_sql(unnest)?
{
    // Build the SELECT item as `alias.VALUE`
    let alias_name = flatten_relation.alias_name().to_string();
    relation.flatten(flatten_relation);

    let value_expr = self.build_flatten_value_select_item(&alias_name);
    select.projection(vec![value_expr]);
    select.already_projected();

    return self.select_to_sql_recursively(
        p.input.as_ref(),
        query,
        select,
        relation,
    );
}
```

- [ ] **Step 2: Add `build_flatten_value_select_item` helper method**

Add this method to `impl Unparser`:

```rust
/// Build a `SELECT alias."VALUE"` item for Snowflake FLATTEN output.
fn build_flatten_value_select_item(&self, alias_name: &str) -> ast::SelectItem {
    let compound = ast::Expr::CompoundIdentifier(vec![
        self.new_ident_quoted_if_needs(alias_name.to_string()),
        ast::Ident::with_quote('"', "VALUE"),
    ]);
    ast::SelectItem::UnnamedExpr(compound)
}
```

- [ ] **Step 3: Run the simple FLATTEN test**

Run: `cargo test -p datafusion-sql snowflake_unnest_to_lateral_flatten_simple`
Expected: PASS — output is now `SELECT _unnest."VALUE" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS _unnest`

- [ ] **Step 4: Run the cross-join test**

Run: `cargo test -p datafusion-sql snowflake_unnest_to_lateral_flatten_with_cross_join`
Check if it passes or what the actual output is. The cross-join case may take a different code path. Adjust if needed.

- [ ] **Step 5: Commit**

```bash
git add datafusion/sql/src/unparser/plan.rs
git commit -m "feat: rewrite SELECT list to alias.VALUE for FLATTEN-as-source

When UNNEST is the FROM source (Path A), the Projection handler
now builds a SELECT _unnest.VALUE item instead of passing through
the __unnest_placeholder column name."
```

---

## Task 4: Rewrite SELECT-list expressions for Path B (UNNEST-as-lateral-join)

Path B is `SELECT * FROM unnest_table u, UNNEST(u.array_col)`. Here the outer Projection runs `reconstruct_select_statement()` which calls `unproject_unnest_expr()`. That function currently always produces `Expr::Unnest(...)`. For Snowflake, it needs to produce `<alias>.VALUE` instead.

**Files:**
- Modify: `datafusion/sql/src/unparser/utils.rs:159-184`
- Modify: `datafusion/sql/src/unparser/plan.rs:231-290` (call site in `reconstruct_select_statement`)

- [ ] **Step 1: Add `unproject_unnest_expr_as_flatten_value` to utils.rs**

This is a variant of `unproject_unnest_expr` that produces a qualified column `alias.VALUE` instead of `Expr::Unnest(...)`:

```rust
/// Like `unproject_unnest_expr`, but for Snowflake FLATTEN:
/// transforms `__unnest_placeholder(...)` column references into
/// `Expr::Column(Column { relation: Some(alias), name: "VALUE" })`.
pub(crate) fn unproject_unnest_expr_as_flatten_value(
    expr: Expr,
    unnest: &Unnest,
    flatten_alias: &str,
) -> Result<Expr> {
    expr.transform(|sub_expr| {
        if let Expr::Column(col_ref) = &sub_expr {
            if unnest
                .list_type_columns
                .iter()
                .any(|e| e.1.output_column.name == col_ref.name)
            {
                let value_col = Expr::Column(Column::new(
                    Some(TableReference::bare(flatten_alias)),
                    "VALUE",
                ));
                return Ok(Transformed::yes(value_col));
            }
        }
        Ok(Transformed::no(sub_expr))
    })
    .map(|e| e.data)
}
```

- [ ] **Step 2: Modify `reconstruct_select_statement` to use the flatten variant**

In `plan.rs:231`, `reconstruct_select_statement` currently does:

```rust
if let Some(unnest) = find_unnest_node_within_select(plan) {
    exprs = exprs
        .into_iter()
        .map(|e| unproject_unnest_expr(e, unnest))
        .collect::<Result<Vec<_>>>()?;
};
```

Add a branch for the flatten dialect. This requires knowing the flatten alias, which we can pass through (either as a field on Unparser or by detecting the dialect at this point):

```rust
if let Some(unnest) = find_unnest_node_within_select(plan) {
    if self.dialect.unnest_as_lateral_flatten() {
        exprs = exprs
            .into_iter()
            .map(|e| {
                unproject_unnest_expr_as_flatten_value(
                    e,
                    unnest,
                    FLATTEN_DEFAULT_ALIAS,
                )
            })
            .collect::<Result<Vec<_>>>()?;
    } else {
        exprs = exprs
            .into_iter()
            .map(|e| unproject_unnest_expr(e, unnest))
            .collect::<Result<Vec<_>>>()?;
    }
};
```

Import `FLATTEN_DEFAULT_ALIAS` from `ast.rs` (make it `pub`).

- [ ] **Step 3: Run the outer-ref test**

Run: `cargo test -p datafusion-sql snowflake_unnest_to_lateral_flatten_outer_ref`
Expected: PASS — output is now `SELECT "u"."array_col", "u"."struct_col", _unnest."VALUE" FROM ...`

- [ ] **Step 4: Run all 3 Snowflake tests**

Run: `cargo test -p datafusion-sql snowflake_unnest`
Expected: all 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add datafusion/sql/src/unparser/utils.rs datafusion/sql/src/unparser/plan.rs
git commit -m "feat: rewrite unnest column refs to alias.VALUE for Snowflake FLATTEN

When the Snowflake dialect is active and reconstruct_select_statement
encounters unnest placeholder columns, they are now rewritten to
qualified column references (_unnest.VALUE) instead of UNNEST(expr)
function calls."
```

---

## Task 5: Run existing unnest tests for regression

**Files:** None — read-only verification.

- [ ] **Step 1: Run all existing unnest-related dialect tests**

Run: `cargo test -p datafusion-sql roundtrip_statement_with_dialect_2 roundtrip_statement_with_dialect_3 roundtrip_statement_with_dialect_4`
Expected: all pass (BigQuery, default, etc. are unchanged).

- [ ] **Step 2: Run full crate test suite**

Run: `cargo test -p datafusion-sql 2>&1 | tail -5`
Expected: all tests pass.

- [ ] **Step 3: Run clippy and fmt**

Run: `cargo fmt --all -- --check && cargo clippy -p datafusion-sql --all-targets --all-features -- -D warnings`
Expected: clean.

---

## Task 6: Add edge case tests

**Files:**
- Modify: `datafusion/sql/tests/cases/plan_to_sql.rs`

- [ ] **Step 1: Add test for `SELECT UNNEST([1,2,3])` (implicit FROM)**

This is the case where UNNEST is in the SELECT clause, not FROM. The plan tree is different — there's an outer Projection with the UNNEST column that feeds into `reconstruct_select_statement`.

```rust
#[test]
fn snowflake_unnest_to_lateral_flatten_implicit_from() -> Result<(), DataFusionError> {
    let snowflake = SnowflakeDialect::new();
    roundtrip_statement_with_dialect_helper!(
        sql: "SELECT UNNEST([1,2,3])",
        parser_dialect: GenericDialect {},
        unparser_dialect: snowflake,
        expected: @"PLACEHOLDER",  // discover actual output, then fix
    );
    Ok(())
}
```

Run with `PLACEHOLDER` first to discover the actual output, then update to the correct value. The correct Snowflake SQL for this case should be:
```sql
SELECT _unnest."VALUE" FROM LATERAL FLATTEN(INPUT => [1, 2, 3]) AS _unnest
```

- [ ] **Step 2: Add test for aliased unnest**

```rust
#[test]
fn snowflake_unnest_to_lateral_flatten_with_alias() -> Result<(), DataFusionError> {
    let snowflake = SnowflakeDialect::new();
    roundtrip_statement_with_dialect_helper!(
        sql: "SELECT * FROM UNNEST([1,2,3]) AS t1 (c1)",
        parser_dialect: GenericDialect {},
        unparser_dialect: snowflake,
        expected: @"PLACEHOLDER",  // discover actual output, then fix
    );
    Ok(())
}
```

The correct Snowflake SQL should reference the user-provided alias. Discover the actual output, evaluate whether the alias override flows through, and fix.

- [ ] **Step 3: Run all tests, verify green**

Run: `cargo test -p datafusion-sql snowflake_unnest`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add datafusion/sql/tests/cases/plan_to_sql.rs
git commit -m "test: add edge case tests for Snowflake FLATTEN unparsing

Add tests for implicit FROM (SELECT UNNEST(...)) and user-provided
aliases to ensure the VALUE column rewriting handles all plan shapes."
```

---

## Notes on What This Plan Does NOT Cover

These are deliberate scope exclusions for now:

1. **Multi-column unnest** — `SELECT unnest(a), unnest(b) FROM t` would need multiple FLATTEN calls with separate aliases. The current code takes only the first expression. This is a separate feature.
2. **Struct unnest / MODE => 'OBJECT'** — The unparser still asserts `struct_type_columns.is_empty()`. Struct flattening with `.KEY`/`.VALUE` output is a separate feature.
3. **Recursive depth / RECURSIVE => TRUE** — Multi-level unnest (`unnest(unnest(col))`) to Snowflake recursive FLATTEN. Separate feature.
4. **User-provided aliases overriding `_unnest`** — Task 6 will discover whether this works. If not, it's a follow-up.
