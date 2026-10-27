# 09 — Export

The export task writes the tables and views of one schema, all of them or a named subset,
to one CSV file each. It is its own wheel entry point, `dbx-flame-export`, not a write
verb: the ingest flow (typing, policies, dedup, provenance) doesn't apply, and one run
handles many objects. Objects are exported as they are, so prepare them in Gold first;
the only shaping is leaving columns out and renaming them.

A run builds the files, their checksums and a manifest in the preparation path, then
delivers them to a destination, as one zip or as loose files. The epic is gh #40;
archiving earlier deliveries is gh #73.

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
      export.checksum: sha256
      export.package: zip
      export.destination: s3://receiver/system1/
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
4. **Clears `{preparation_path}/exports_temp/{name}`**, only that folder: the preparation
   path may hold other exports, even of the same schema. What a run writes stays there
   afterwards, for inspection. Two exports with the same `export.name` under one
   preparation path would share this folder, so give each its own name.
5. **Pins every Delta table to its current version** (a `MANAGED` or `EXTERNAL` table
   stored as Delta), so a commit landing mid-export reaches neither its file nor its
   count. Anything else, a view, materialized view or foreign table, has no time travel
   and is exported as read; every one of them but a view is logged as
   `object_not_versioned`.
6. **Writes one CSV per object**, named after the object in uppercase:

   | `export.with_ts_object_names` | File |
   |---|---|
   | `false` (default) | `SITE.csv` |
   | `true` | `SITE_2026-10-20.csv` |

7. **Seals each file** with a checksum beside it and counts its rows, both from one read
   of the file: see [4](#4-checksums-and-the-manifest). A pinned table whose file holds
   a different number of rows than its version fails that object (platform policy `export_row_count_mismatch`).
8. **Leaves out empty objects** with `export.skip_empty` (default `true`): an object with
   no rows is left out of the delivery and of the manifest, with a `nothing_to_export`
   warning against it. Its file stays in the preparation path.
9. **Attempts every object.** One that fails, such as a view whose source is gone, is
   logged against that object (`object_export_failure`) and the run fails once all were
   attempted, keeping the files of the ones that succeeded and **delivering nothing**.
10. **Writes the manifest** of the objects to deliver, with its own checksum.
11. **Delivers**: see [5](#5-delivery). When every object is empty, nothing is delivered
    and a run-level `nothing_to_export` warning says so.

The audit log gets `export_start` and `export_complete` (or `export_failure`) for the run,
`delivery_complete` naming where the delivery went, and, per object, a `rows_exported`
KPI counted from the written file.

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

## 4. Checksums and the manifest

Every file gets a checksum file beside it in the task's `export.checksum` algorithm, named
after the file's stem with the algorithm as extension: `SITE.csv` → `SITE.sha256`. It
holds the bare hash only, exactly what `sha256sum` (or `md5sum`, `sha512sum`) prints for
the file without its file name: no spaces, no line break.

The manifest, `<NAME>_MANIFEST_<stamp>.csv` with its own checksum file, is always written
and always stamped. It lists every object delivered, once. With `export.skip_empty` off,
an empty object is delivered and listed with `row_count` 0:

| Column | Holds |
|---|---|
| `object` | the object's name, uppercase |
| `object_type` | its Unity Catalog table type, as `information_schema.tables` names it: `MANAGED`, `EXTERNAL`, `VIEW`, `MATERIALIZED_VIEW`, `FOREIGN`, ... |
| `version` | the Delta version the file holds; empty for an object exported as read |
| `row_count` | the rows in the file, header excluded |
| `export_file_name` | the CSV's file name |
| `checksum` | the CSV's hash, as in its checksum file |

The rows are counted from the file itself, in the same read that hashes it, so the count
proves what the file holds rather than what was meant to go into it. An object that
failed is not listed; its failure is in the audit log, and the run fails.

## 5. Delivery

`export.destination` is any location the cluster can write to, used as given: a Volume,
`s3://...` or an Azure location. Without one, the delivery goes to
`{preparation_path}/deliveries/{name}/`. Files land at the destination's root, copied with
`dbutils.fs.cp`, which writes each file whole: a consumer never reads a partial file.

| `export.package` | Delivers, in this order |
|---|---|
| `zip` | `<NAME>_<stamp>.zip` (`<NAME>.zip` with `export.with_ts_zip` off) holding every CSV and its checksum at its root, then the zip's checksum (`<NAME>_<stamp>.sha256`) with `export.with_zip_checksum` |
| `files` | every CSV and its checksum, then the manifest and its checksum |

`export.with_manifest` (default `true`) decides whether the manifest and its checksum are
delivered: inside the zip, or last among the files. The file copied last marks the
delivery complete.

A delivery with the same names as an earlier one replaces it: an unstamped name is
delivered again over the last, while stamped names pile up. Every delivery is logged as
`delivery_complete`: `Zip delivered to <location>` or `Files delivered to <location>`.

**Known risk: a copy that fails mid-delivery.** The files are copied one by one, and
nothing already copied is rolled back. With `package: files`, a copy that fails leaves
part of the delivery at the destination, and the run fails with `export_failure`. With
unstamped names, the destination then mixes files of this run and the last, and each of
them still matches its own checksum. The manifest, copied last and always stamped, is
what tells a consumer that a delivery is complete: keep `with_manifest` on when names
are unstamped. With `package: zip`, the zip is one file and arrives whole or not at all;
a failure between the zip and its checksum leaves the zip without its new checksum.

## 6. CSV format

Fixed, not configurable. Every value is written exactly as Delta stores it: no digit is
added, cut or rounded, and the file is the same on any cluster, whatever its session
time zone.

| Rule | Value |
|---|---|
| Standard | RFC 4180: comma separator, `"` quote, a quote inside a value doubled (`""`), header row |
| Encoding | UTF-8, without BOM |
| Line endings | LF, which Spark's writer imposes; a line break inside a value is kept, quoted |
| NULL | empty: `1,,x` |
| Empty string | `""`: `1,"",x` |
| Dates | ISO 8601: `2026-10-20` |
| `TIMESTAMP` | ISO 8601 in UTC, with the fraction the value holds and no padding: `2026-10-20T08:30:00Z`, `2026-10-20T08:30:00.12Z`, `2026-10-20T08:30:00.123456Z` |
| `TIMESTAMP_NTZ` | ISO 8601 as stored, without a zone: `2026-10-20T08:30:00.123456` |
| Decimals | plain notation with every digit of the column's scale: `DECIMAL(20,10)` zero is `0.0000000000`, never `0E-10` |
| Floating point | as Java prints a `DOUBLE` or `FLOAT`: the shortest text that reads back to the same value, in E-notation for large or small values (`1.0E20`) |
| Whitespace | kept as it is; leading and trailing spaces are not trimmed |

Two limits come from Delta, not from the export. A `TIMESTAMP` stores an instant, not
the zone it was written in, so it is written in UTC: a source that must keep its
original offset holds it in a `STRING`, which is exported as it is. And Delta stores
every timestamp in microseconds, so `.120000` and `.12` are the same value, written
`.12`.
