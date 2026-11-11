-- Per-index statistics and definition.
SELECT n.nspname AS schemaname, c.relname AS tblname, ic.relname AS idxname, i.indexrelid,
       s.idx_scan,
       pg_relation_size(i.indexrelid) AS index_bytes,
       i.indisvalid, i.indisunique, i.indisprimary, i.indisready,
       i.indkey::text AS indkey, pg_get_expr(i.indpred, i.indrelid) AS indpred,
       pg_get_expr(i.indexprs, i.indrelid) AS indexprs,
       pg_get_indexdef(i.indexrelid) AS indexdef,
       EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid) AS backs_constraint,
       (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
          FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ord)
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum) AS index_columns
FROM pg_index i
JOIN pg_class c  ON c.oid = i.indrelid
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.indexrelid
WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast');
