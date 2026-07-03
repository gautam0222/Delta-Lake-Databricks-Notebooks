
# Databricks Delta Lake Practice Notebooks

This repository contains a hands-on Databricks Delta Lake practice project. The notebooks walk through core Delta Lake features using small sample datasets, Unity Catalog tables, and Delta tables stored in Databricks volume paths.

The project is focused on learning and demonstrating Delta Lake concepts such as table creation, Delta transaction logs, schema enforcement, schema evolution, DML operations, time travel, cloning, Change Data Feed, UniForm, and optimization.

## Repository Contents

| File | Description |
| --- | --- |
| `1_deltalake.ipynb` | Creates Delta tables using SQL and the DeltaTable Python API. Demonstrates identity columns and inserting data into a managed Delta table. |
| `2_deltalake.ipynb` | Writes a Spark DataFrame to a Delta path and inspects Delta transaction log JSON files under `_delta_log`. |
| `3_deltalake.ipynb` | Demonstrates schema enforcement, schema evolution using `mergeSchema`, reading Delta data, and schema overwrite using `overwriteSchema`. |
| `4_deltalake.ipynb` | Covers Delta DML operations, including `UPDATE` and `MERGE`/upsert using the DeltaTable API. |
| `5_deltalake.ipynb` | Demonstrates schema-level changes such as column rename with Delta column mapping enabled. |
| `6_deltalake.ipynb` | Uses Delta utility commands, table history, time travel, restore, vacuum, deep clone, and shallow clone. |
| `7_deltalake.ipynb` | Enables and queries Change Data Feed after insert, update, and delete operations. |
| `8_deltalake.ipynb` | Demonstrates Delta UniForm with Iceberg compatibility. |
| `9_deltalake.ipynb` | Covers the small file problem, `OPTIMIZE`, Z-Ordering, and Liquid Clustering. |
| `DeltaLake.dbc` | Databricks archive export of the notebook bundle. |

## Environment Used

These notebooks are designed to run in Databricks with:

- Apache Spark
- Delta Lake
- Unity Catalog enabled
- A catalog named `deltalakegautam`
- A schema/database named `default`
- A Unity Catalog volume path similar to:

```text
/Volumes/deltalakegautam/default/deltavol/
```

Most examples use the following table and path namespace:

```text
deltalakegautam.default
/Volumes/deltalakegautam/default/deltavol/
```

If you run this project in another workspace, update the catalog, schema, and volume names before executing the notebooks.

## Topics Covered

### 1. Delta Table Creation

The first notebook shows two ways to create Delta tables:

- SQL DDL using `CREATE TABLE`
- Python DeltaTable builder API using `DeltaTable.create` and `DeltaTable.createIfNotExists`

It also demonstrates identity columns using `IdentityGenerator`, where the `id` column is generated automatically during inserts.

Example table:

```text
deltalakegautam.default.firstdeltaapi
```

### 2. Path-Based Delta Tables

The notebooks write Spark DataFrames directly to Delta format at volume paths such as:

```text
/Volumes/deltalakegautam/default/deltavol/demosink/
/Volumes/deltalakegautam/default/deltavol/dmlsink/
/Volumes/deltalakegautam/default/deltavol/schemalevel/
/Volumes/deltalakegautam/default/deltavol/optimization/
```

This demonstrates that Delta tables can be used both as registered tables and as path-based datasets.

### 3. Delta Transaction Log

The project inspects files inside `_delta_log`, including JSON commit files such as:

```text
_delta_log/00000000000000000000.json
_delta_log/00000000000000000001.json
```

This helps show how Delta Lake stores transaction metadata, schema information, add/remove file actions, and commit history.

### 4. Schema Enforcement and Schema Evolution

The schema notebooks demonstrate:

- Delta schema enforcement when incoming data does not match the existing table schema
- Appending new columns using `.option("mergeSchema", "true")`
- Replacing the table schema using `.option("overwriteSchema", "true")`

These examples show the difference between protecting an existing schema and intentionally evolving or replacing it.

### 5. Delta DML Operations

The DML notebook covers:

- Reading Delta data using SQL
- Updating rows with `UPDATE`
- Performing upserts using `MERGE`
- Using `DeltaTable.forPath` to reference a path-based Delta table

The merge example updates matching records and inserts non-matching records:

```python
dlt_obj.alias("trg").merge(
    df.alias("src"),
    "trg.id = src.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### 6. Column Mapping and Rename

The schema-level notebook demonstrates enabling Delta column mapping:

```sql
ALTER TABLE delta.`/path/to/table`
SET TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnmapping.mode' = 'name'
);
```

After enabling column mapping, the notebook renames a column without rewriting all data files.

### 7. Delta Utility Commands

The utility notebook demonstrates:

- `DESCRIBE`
- `DESCRIBE DETAIL`
- `DESCRIBE EXTENDED`
- `DESCRIBE HISTORY`
- `SHOW TBLPROPERTIES`

These commands are useful for inspecting Delta table metadata, storage location, table properties, protocol versions, and operation history.

### 8. Time Travel and Restore

The project demonstrates Delta Lake time travel using:

```sql
SELECT * FROM delta.`/path/to/table` VERSION AS OF 1;
SELECT * FROM delta.`/path/to/table` TIMESTAMP AS OF '2026-07-02T06:42:07.000+00:00';
```

It also demonstrates restoring a Delta table to a previous version:

```sql
RESTORE delta.`/path/to/table` TO VERSION AS OF 2;
```

### 9. Vacuum

The notebooks include a `VACUUM` example:

```sql
VACUUM delta.`/path/to/table` RETAIN 0 HOURS;
```

This command removes old data files that are no longer referenced by the Delta transaction log. In a production environment, use retention settings carefully because aggressive vacuuming can break time travel for older versions.

### 10. Delta Clone

The project demonstrates both:

- Deep clone
- Shallow clone

Examples:

```sql
CREATE TABLE deltalakegautam.default.clonetbl
CLONE delta.`/Volumes/deltalakegautam/default/deltavol/dmlsink/` VERSION AS OF 1;

CREATE TABLE deltalakegautam.default.clonetblshallow
SHALLOW CLONE deltalakegautam.default.clonetbl;
```

Deep clone copies table data and metadata. Shallow clone copies metadata and references the original data files.

### 11. Change Data Feed

The Change Data Feed notebook enables row-level change tracking:

```sql
ALTER TABLE deltalakegautam.default.clonetbl
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

It then performs insert, update, and delete operations before querying changes:

```sql
SELECT * FROM table_changes('deltalakegautam.default.clonetbl', 1);
```

Change Data Feed is useful for incremental ETL, downstream synchronization, auditing, and tracking row-level changes between table versions.

### 12. Delta UniForm

The UniForm notebook creates a Delta table with Iceberg compatibility:

```sql
CREATE TABLE deltalakegautam.default.unitbl
USING DELTA
TBLPROPERTIES (
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

Delta UniForm allows compatible clients to read the same data through other table formats such as Iceberg, while Delta remains the source table format.

### 13. Optimization, Z-Ordering, and Liquid Clustering

The optimization notebook demonstrates:

- The small file problem
- File compaction using `OPTIMIZE`
- Data skipping improvement using `ZORDER BY`
- Liquid Clustering using `CLUSTER BY`

Example:

```sql
OPTIMIZE delta.`/Volumes/deltalakegautam/default/deltavol/optimization/`;

OPTIMIZE delta.`/Volumes/deltalakegautam/default/deltavol/optimization/`
ZORDER BY (id);

ALTER TABLE deltalakegautam.default.clonetbl
CLUSTER BY (id);
```

## Suggested Execution Order

Run the notebooks in numeric order:

```text
1_deltalake.ipynb
2_deltalake.ipynb
3_deltalake.ipynb
4_deltalake.ipynb
5_deltalake.ipynb
6_deltalake.ipynb
7_deltalake.ipynb
8_deltalake.ipynb
9_deltalake.ipynb
```

Some notebooks depend on tables or Delta paths created by earlier notebooks. For example, the Change Data Feed and UniForm notebooks use cloned tables created in the utility notebook.

## Important Notes

- These notebooks are intended for learning and experimentation.
- Several notebooks mutate the same tables and paths, so results can differ if cells are run multiple times.
- Some examples intentionally demonstrate failure scenarios, such as schema mismatch before enabling schema evolution.
- The `VACUUM RETAIN 0 HOURS` example is aggressive and should be avoided in production unless you fully understand the impact on time travel and rollback.
- The column name `cutomer_name` appears in the column rename notebook. It likely means `customer_name`, but it has been left as-is to match the current notebook content.
- Update catalog, schema, and volume names before running this project in a different Databricks workspace.

## How to Import into Databricks

You can use either the individual `.ipynb` files or the Databricks archive:

1. Open Databricks Workspace.
2. Go to the target folder.
3. Select **Import**.
4. Upload either:
   - The individual `.ipynb` notebooks, or
   - `DeltaLake.dbc`
5. Attach the notebooks to a compatible Databricks cluster or SQL warehouse.
6. Update catalog/schema/volume names if needed.
7. Run notebooks in numeric order.

## Skills Practiced

By completing these notebooks, the following Delta Lake skills are covered:

- Creating managed Delta tables
- Creating Delta tables with Python APIs
- Writing Spark DataFrames in Delta format
- Reading registered and path-based Delta tables
- Understanding Delta transaction logs
- Handling schema enforcement and schema evolution
- Performing updates and merges
- Renaming columns with column mapping
- Inspecting Delta table metadata and history
- Using Delta time travel
- Restoring older table versions
- Cleaning old files with vacuum
- Creating deep and shallow clones
- Tracking row-level changes with Change Data Feed
- Creating UniForm tables for Iceberg compatibility
- Optimizing table layout with compaction, Z-Ordering, and Liquid Clustering
