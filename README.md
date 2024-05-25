# tools

**Générer les options key pour chaque table étrangère**

Source : https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns

```sql
SELECT format('ALTER FOREIGN TABLE fdw.%I ALTER COLUMN %I OPTIONS (ADD key ''true'')', c.relname, a.attname)
FROM pg_catalog.pg_class c
INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
INNER JOIN pg_index i ON c.oid = i.indrelid
INNER JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) AND i.indisprimary
WHERE n.nspname = 'public';
```

**Alimenter la table config depuis les tables étrangères**

```sql
INSERT INTO tools.config (relname, source, pkey)
SELECT format('public.%I', c.relname), format('%I.%I', n.nspname, c.relname), a.attname
FROM pg_catalog.pg_foreign_table ft
INNER JOIN pg_catalog.pg_class c ON c.oid = ft.ftrelid
INNER JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
INNER JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid,
LATERAL pg_catalog.pg_options_to_table(a.attfdwoptions) op
WHERE a.attnum > 0 AND NOT a.attisdropped
AND op.option_name = 'key' AND op.option_value = 'true';
```
