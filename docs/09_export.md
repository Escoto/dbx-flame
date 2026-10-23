# 09 — Export

The export task writes the tables and views of one schema, all of them or a named subset,
to one CSV file each. It is its own wheel entry point, `dbx-flame-export`, not a write
verb: the ingest flow (typing, policies, dedup, provenance) doesn't apply, and one run
handles many objects. Objects are exported as they are, so prepare them in Gold first;
the only shaping is leaving columns out and renaming them.

Today a run builds the files in the preparation path. Checksums and the manifest
(gh #44) and delivery to a destination (gh #45) follow; the epic is gh #40.

## 1. A task

```yaml
- task_key: export_cro
  python_wheel_task:
    package_name: dbx-flame
    entry_point: dbx-flame-export
    named_parameters:
      catalog: mdm
      env: "${bundle.target}"
      export.name: CRO
      export.schema_name: gold_cro
      export.preparation_path: /Volumes/mdm_${bundle.target}/gold_cro/exports/
      export.stamp_format: "%Y-%m-%d"
      export.timezone: Europe/Berlin
```

The settings, and what Start rejects, are in
[02_config_schema.md](02_config_schema.md#8-export-task). The run identity parameters
(`__workflow_id` and the rest) work as they do for every task.

## 2. What a run does

1. **Lists the objects.** Every table and view in `{catalog}_{env}.{schema_name}`, or the
   ones `export.tables` names, matched in any case.
2. **Checks every name the settings use** against the schema: the objects in
   `export.tables`, `export.excludes` and `export.renames`, and their columns. A name the
   schema doesn't hold fails the run before anything is written, and the message only
   lists what wasn't found. The settings of an object the schema holds but the run
   doesn't export are skipped, with an `export_settings_skipped` warning against it.
3. **Takes one stamp**, rendered with `export.stamp_format` in `export.timezone`. Every
   stamped name of the run carries it.
4. **Clears `{preparation_path}/{schema_name}`**, only that folder: the preparation path
   may hold other schemas' exports. What a run writes stays there afterwards, for
   inspection.
5. **Writes one CSV per object**, named after the object in uppercase:

   | `export.with_ts_object_names` | File |
   |---|---|
   | `false` (default) | `SITE.csv` |
   | `true` | `SITE_2026-10-20.csv` |

6. **Attempts every object.** One that fails, such as a view whose source is gone, is
   logged against that object (`object_export_failure`) and the run fails once all were
   attempted, keeping the files of the ones that succeeded.

The audit log gets `export_start` and `export_complete` (or `export_failure`) for the run,
and, per object, a `rows_exported` KPI counted from the written file, not from a second
read of the object.

## 3. Columns

Three settings shape the columns of a file without changing the object, applied in this
order:

1. `export.exclude_metadata` (default `true`) leaves out every framework column, the ones
   starting with `__` (`__SOURCE`, `__EXPORT_DATE`, ...), from every object.
2. `export.excludes.<object>` leaves out the columns it lists from that object.
3. `export.renames.<object>.<column>` gives a column its name in that object's file.

```yaml
export.excludes.SITE: "REGION, NOTES"
export.renames.SITE.SITE_ID: ID
```

Because excludes come first, a rename may take the name of a column that was left out,
and renaming a column that was left out is rejected at Start. The run fails before
anything is written when a setting names an unknown object or column, when two columns of
a file would share a name, or when an object would be left without any column. Column
names are matched as written (gh #66).

## 4. CSV format

Fixed, not configurable:

| Rule | Value |
|---|---|
| Standard | RFC 4180: comma separator, `"` quote, a quote inside a value doubled (`""`), header row |
| Encoding | UTF-8, without BOM |
| Line endings | LF, which Spark's writer imposes; a line break inside a value is kept, quoted |
| NULL | empty: `1,,x` |
| Empty string | `""`: `1,"",x` |
| Dates | ISO 8601: `2026-10-20` |
| Timestamps | ISO 8601 in the cluster's session time zone, with its offset: `2026-10-20T08:30:00.000Z` |
| Whitespace | kept as it is; leading and trailing spaces are not trimmed |
