# Fix: Snowflake Unparser renders VALUE reference but omits LATERAL FLATTEN from FROM

**Status:** Active
**Branch:** Fix on top of SubqueryAlias fix
**Depends on:** 2026-04-11-snowflake-unnest-subquery-alias.md

## Bug

After the SubqueryAlias fix, the Snowflake Unparser generates `"_unnest"."VALUE"` in the SELECT clause but does NOT add `LATERAL FLATTEN(INPUT => ...) AS "_unnest"` to the FROM clause.

## Generated SQL (broken)

```sql
SELECT "_unnest"."VALUE" AS "target_item" 
FROM (
  SELECT "OKTA_LOGS_RAW_T"."_SEVENAI_TENANT_ID", "OKTA_LOGS_RAW_T"."TARGET" 
  FROM "PUBLIC"."OKTA_LOGS_RAW_T" 
  WHERE ("OKTA_LOGS_RAW_T"."_SEVENAI_TENANT_ID" = '...') 
  LIMIT 5 OFFSET 0
)
```

## Expected SQL

```sql
SELECT "_unnest"."VALUE" AS "target_item" 
FROM (
  SELECT "OKTA_LOGS_RAW_T"."_SEVENAI_TENANT_ID", "OKTA_LOGS_RAW_T"."TARGET" 
  FROM "PUBLIC"."OKTA_LOGS_RAW_T" 
  WHERE ("OKTA_LOGS_RAW_T"."_SEVENAI_TENANT_ID" = '...') 
  LIMIT 5 OFFSET 0
), LATERAL FLATTEN(INPUT => PARSE_JSON("TARGET"), OUTER => TRUE) AS "_unnest"
```

## Snowflake error

```
SQL compilation error: invalid identifier '"_unnest".VALUE'
```

Snowflake can't resolve `"_unnest"` because the LATERAL FLATTEN table function was never declared in the FROM clause.

## Input plan

```
Projection: __unnest_placeholder(...,depth=1) AS target_item
  Limit: 5
    Unnest: [__unnest_placeholder(json_get_array(T.TARGET))|depth=1]
      SubqueryAlias: T
        Projection: PARSE_JSON(T.TARGET) AS __unnest_placeholder(...)
          TableScan: T projection=[_SEVENAI_TENANT_ID, TARGET], filters=[tenant=...]
```

Note: `json_get_array(TARGET)` with 1 arg was rewritten to `PARSE_JSON(TARGET)` by the Snowflake expression rewriter (this is a Sigil-side transformation, not a DF issue).

## Diagnosis

The Unparser's Snowflake FLATTEN code path correctly:
1. Identifies the Unnest node
2. Generates `"_unnest"."VALUE"` as the column reference
3. Recursively processes the Unnest's child for the FROM clause

But it does NOT:
4. Append `LATERAL FLATTEN(INPUT => <expr>, OUTER => TRUE) AS "_unnest"` to the FROM clause

The FLATTEN table factor is missing from the generated AST's FROM/JOIN list.

## How to verify the fix

After fixing, the generated SQL should contain:
- `LATERAL FLATTEN(INPUT => ...)` in the FROM clause
- `"_unnest"."VALUE"` in the SELECT resolved against the FLATTEN alias
- The FLATTEN INPUT should be the array expression from the inner Projection (e.g., `PARSE_JSON("TARGET")`)
