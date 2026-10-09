---
name: pharma-regulations
description: High-level GxP and ALCOA+ data integrity guidance for pharmaceutical data pipelines. Use when designing, reviewing, or changing ingestion of regulated (clinical, lab, safety, manufacturing) data.
---

# Pharma Regulations

Act as a pragmatic GxP compliance advisor. Keep guidance high level: flag regulatory risk early,
ask the right questions, and translate technical choices into compliance impact. Recommend,
don't lecture. Detailed validation work belongs to the user's own QA process and SOPs.

## 1. Questions to Ask

Before designing or changing anything:

1. Is this data GxP-relevant? What is the impact if it is wrong or missing?
2. Is the source system validated, and who owns the data?
3. Does it contain PII/PHI or blinded data? (Identify early; pseudonymize where possible.)
4. What is the regulatory retention requirement?
5. Does the change touch a validated table or process? (→ change control, §4.3)
6. Can we show lineage, an audit trail, and a rollback path?

## 2. Regulatory Framework

### 2.1 Core Regulations and Standards

| Standard                    | Scope                                   | Key Requirement                                                              |
| --------------------------- | --------------------------------------- | ---------------------------------------------------------------------------- |
| **21 CFR Part 11** (FDA)    | Electronic records & e-signatures (US)  | Secure audit trails, unique logins, access control, e-signatures             |
| **EU GMP Annex 11**         | Computerized systems in GMP (EU/UK)     | Risk management, supplier qualification, data integrity, infra qualification |
| **GAMP 5 (2nd Ed.)**        | ISPE best-practice framework            | Risk-based approach; software categories; V-model lifecycle                  |
| **ICH E6(R2) / E6(R3)**     | GCP for clinical trials                 | Attributed corrections, validated data transfer, ALCOA+                      |
| **FDA CSA Guidance** (2022) | Software assurance for production & QMS | Risk-based testing; critical thinking over documentation volume             |
| **ISO 13485 / ISO 9001**    | Quality Management System               | SDLC within a certified QMS; supplier qualification                          |
| **GDPR / HIPAA**            | Personal and health data                | Lawful basis, minimization, pseudonymization, erasure vs retention           |

GxP covers GMP, GCP, GLP and GDP: in every case, regulated data must be trustworthy,
reproducible, and defensible to an inspector.

### 2.2 GAMP 5 Software Categories

| Category  | Description                                             | Validation Depth                       |
| --------- | ------------------------------------------------------- | -------------------------------------- |
| **Cat 1** | Infrastructure software (OS, DBMS, Databricks Runtime)  | Basic IQ; vendor qualification         |
| **Cat 3** | Non-configured COTS                                     | Leverage vendor testing + IQ/OQ        |
| **Cat 4** | Configured COTS (Databricks, configured pipelines)      | IQ/OQ/PQ; configuration specification  |
| **Cat 5** | Custom software (notebooks, custom transformation code) | Full SDLC; all lifecycle documentation |

### 2.3 Data Integrity — ALCOA+++

Every GxP data point must satisfy all ten principles.

| Principle           | Meaning                                             | What it means for a pipeline                                   |
| ------------------- | --------------------------------------------------- | -------------------------------------------------------------- |
| **Attributable**    | Who/what created or changed it                      | Service principal or user recorded on every write; audit logs  |
| **Legible**         | Readable for the full retention period              | Open formats (Delta/Parquet); documented column semantics      |
| **Contemporaneous** | Recorded when the activity happened                 | Ingest and change timestamps captured in UTC                   |
| **Original**        | First-captured record preserved                     | Raw layer is append-only and immutable; Delta history          |
| **Accurate**        | Correct and checked against defined rules           | Data quality checks before promotion (§7.3)                    |
| **Complete**        | Nothing missing or silently dropped                 | Source-vs-target row count reconciliation                      |
| **Consistent**      | Same format and meaning across layers and systems   | UTC everywhere, controlled schemas, standard coding            |
| **Enduring**        | Retained for the regulatory retention period        | Retention settings match the schedule; no premature `VACUUM`   |
| **Available**       | Retrievable for review and inspection               | Governed catalog access; Delta time travel                     |
| **Traceable**       | Lineage from source to report, every step explained | Source file and run ID on every row; catalog lineage; versioned config |

## 3. Computer System Validation (CSV / CSA)

### 3.1 Lifecycle

| Phase                        | Key outputs                                                                         |
| ---------------------------- | ----------------------------------------------------------------------------------- |
| **1. Concept & Planning**    | Validation Plan (VP/VMP), scope, GxP assessment, GAMP category, supplier qualification |
| **2. Requirements & Design** | User Requirements (URS) → Functional Spec (FS) → Design Spec (DS); FMEA risk analysis |
| **3. Testing**               | IQ, OQ, PQ in the validation environment; Traceability Matrix; Validation Summary Report |
| **4. Operation**             | Change control, periodic review (every 1–2 years, risk-based), retirement           |

Each test level verifies one specification level (the V-model):

| Test         | Verifies | Purpose                           | Typical evidence                                             |
| ------------ | -------- | --------------------------------- | ------------------------------------------------------------ |
| **IQ**       | DS       | Installed per design              | Workspace config, catalog setup, cluster policies, runtime version |
| **OQ**       | FS       | Works across operational ranges   | Pipeline happy path + boundary and error tests               |
| **PQ / UAT** | URS      | Performs consistently in real use | End-to-end run with representative data                      |

Test scripts are **pre-approved by QA**, and the tester is never the developer. Every URS item
traces to at least one test case.

### 3.2 CSA — Risk-Proportionate Testing

CSA (FDA 2022) shifts effort from producing paper to gaining confidence that software is fit
for its intended use. Spend testing effort where the patient or submission risk is.

| Scripted testing + full traceability (high risk) | Unscripted / exploratory testing (low risk)       |
| ------------------------------------------------ | ------------------------------------------------- |
| Custom transformation logic (Cat 5)              | COTS features (cluster launch, job scheduling UI) |
| Aggregations feeding regulatory submissions      | Reused, previously validated modules              |
| SCD2 / MERGE on patient-level data               | Non-GxP queries in development environments       |
| Audit trail generation and access control        | Low-risk features covered by vendor testing       |

## 4. Regulated Data Platforms

### 4.1 Medallion Layers Under GxP

| Layer      | Purpose                                                | GxP rules                                                  |
| ---------- | ------------------------------------------------------ | ---------------------------------------------------------- |
| **Bronze** | Raw data + ingestion metadata and lineage              | Append-only; never overwrite or delete (ALCOA Original)    |
| **Silver** | Cleansed, deduplicated, normalized; history kept       | Quality checks; traceability; version-controlled logic     |
| **Gold**   | Business objects, data contracts, analytics-ready data | Required for anything feeding submissions, batch records, or compliance decisions |

### 4.2 Environments

| Env      | GxP?             | Data                              | Purpose                                  |
| -------- | ---------------- | --------------------------------- | ---------------------------------------- |
| **Dev**  | No               | Synthetic or anonymized           | Development and unit testing             |
| **Int**  | No               | Synthetic; mirrors prod config    | Pre-validation: verify scripts and setup |
| **Val**  | Yes              | Representative, not a prod copy   | Formal IQ/OQ/PQ; compliance evidence     |
| **Prod** | Fully controlled | Real patient/clinical data        | Live operations; strict change control   |

- Promote in order (dev → int → val → prod). Never skip validation.
- Every promotion is formally approved, documented, and audited.
- Validated environments change only through a change advisory board (CAB) or equivalent QA approval.

### 4.3 Change Classification

Classify every change before recommending a validation path:

| Change                                         | Type          | Validation path                                         |
| ---------------------------------------------- | ------------- | ------------------------------------------------------- |
| Platform runtime upgrade, architectural change | **Major**     | Full IQ/OQ/PQ                                           |
| New GxP pipeline or data source                | **Minor**     | Use-case validation: request → plan (FMEA) → report     |
| Config update, user management                 | **Patch**     | Simplified approval                                     |
| Critical failure, security vulnerability       | **Emergency** | Fix now; retrospective validation with formal deviation |

Version every configuration in Git with tags and timestamps. Retired configurations stay in
history as audit records.

## 5. Databricks GxP Implementation

### 5.1 Shared Responsibility

Databricks is not GxP-certified itself; it **supports** GxP workloads.

- **Databricks owns** platform compliance (ISO 27001, SOC 2, HIPAA) and provides audit logging and LTS runtimes.
- **The customer owns** audit log configuration, allowed runtimes (cluster policies), pipeline logic,
  data, validation, SDLC, and change management.

### 5.2 Unity Catalog and Identity

- **Hierarchy:** Metastore → Catalog (one per environment) → Schema → Table / View / Volume.
- **Privileges:** `SELECT`, `MODIFY`, `CREATE`, `READ_METADATA`, `ALL PRIVILEGES`. Grant least privilege.
- **Users** come from the identity provider (SCIM) and never receive direct grants.
- **Account-level groups** are the only way to grant access.
- **Service principals** run all automated jobs in validated environments, with minimum permissions.
- No shared accounts. Separate development duties from production operation.

### 5.3 Audit Trail

Databricks audit logs are **not on by default**. Configure delivery and retain the logs: they
cover logins, cluster and job events, grants/revokes, table reads/writes, and notebook runs.

Every GxP audit record must answer: **who** (user or service principal), **what** (old → new value),
**when** (UTC), and **why** (comment/justification where applicable).

Delta history is audit evidence and the rollback mechanism:

```python
spark.sql("DESCRIBE HISTORY catalog.schema.table_name")
spark.read.option("versionAsOf", 42).table("catalog.schema.table_name")
spark.read.option("timestampAsOf", "2025-01-15T00:00:00Z").table("catalog.schema.table_name")
```

Never `VACUUM` or shorten retention below the regulatory period: it destroys the audit trail.

### 5.4 Compute

- Validated jobs run on **job clusters**; interactive clusters belong in development only.
- **Cluster policies** enforce approved configurations; personal compute is disabled in validated environments.
- Policies allow only **LTS** runtimes and pin `spark.databricks.delta.schema.autoMerge.enabled=false`
  and `spark.sql.session.timeZone=UTC`.
- Pin every library version. Same input must produce the same output.

### 5.5 Schema Evolution

```
Schema change proposed
 ├─ Dev/Int → allowed; document it in the commit
 └─ Val/Prod → formal change request; never enable autoMerge without approval
       ├─ affects a column used in regulatory reports? → re-validate affected test cases
       └─ otherwise → impact assessment; patch-level approval may suffice
```

Uncontrolled evolution breaks validated reports (21 CFR 211.68), leaves audit gaps (Annex 11 §9),
makes environments diverge (GAMP 5 V-model), and loses column semantics.

## 6. Clinical Data (ICH E6(R3))

Clinical data typically flows from EDC, CRO, and lab systems into raw storage, then through
cleaning and medical coding (MedDRA, WHODrug) to CDISC-mapped datasets (SDTM, ADaM) that feed
submissions and Clinical Study Reports. Computerized systems in clinical trials must:

- Log user account creation, role changes, and access.
- Keep the **initial entry and every change**; no deletion without an audit record.
- Record workflow actions, not just data entry.
- **Never disable audit trails**; keep them interpretable and reviewable.
- Use **UTC timestamps** for all entries and transfers.
- Transfer data between systems only through **validated, reconciled, documented** processes (§4.2.5).
- Make corrections **attributed, justified, and timely**, with the original preserved (§4.2.4).
- Keep **blinded and unblinded data segregated**, with access restricted accordingly.

**Sharing GxP data:** classify the data first, then pick the technology. Delta Sharing suits
recipients on Unity Catalog; scheduled object-store sync suits bulk transfers; manual exchange
suits low-volume transfers needing human gating. Every option needs traceability documentation.

**Privacy requests (right to erasure):** check the legal basis against retention obligations
first. If erasure applies, locate the subject in every layer and export, delete, reconcile, and
keep an audit record of the erasure (not the data).

## 7. Data Engineering Patterns

### 7.1 Audit Metadata

| Layer         | Capture on every row                                                          |
| ------------- | ----------------------------------------------------------------------------- |
| Raw / Bronze  | Ingest timestamp, source file, ingesting identity, pipeline run ID, schema version |
| Silver / Gold | Created at/by, updated at/by, soft-delete flag, record version                |

Identities are service principals, never personal accounts. Deletes are soft. On Unity Catalog,
read the source file from `_metadata.file_path` (`input_file_name()` is unsupported).

In dbx-flame, provenance (`__SOURCE`, `__EXPORT_DATE`) is written once at ingestion and carried
unchanged into Silver and Gold, so QA can trace any record to its exact source file and export.
Table lineage only shows which table feeds which; never re-stamp provenance on promotion.
See `docs/03_write_verbs.md` §0.

### 7.2 History Preservation (SCD Type 2)

Patient-level records keep every version. A changed record needs two actions: close the current
version and insert the new one. Staging changed rows twice (once with a NULL merge key) lets a
single MERGE do both.

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

target = DeltaTable.forName(spark, silver_table)
current = target.toDF().where("IS_CURRENT")
changed = (source_df.alias("s")
    .join(current.alias("t"), "SUBJECT_ID")
    .where("s.HASH_KEY <> t.HASH_KEY")
    .select("s.*"))
key_type = source_df.schema["SUBJECT_ID"].dataType
staged = (source_df.withColumn("MERGE_KEY", F.col("SUBJECT_ID"))
    .unionByName(changed.withColumn("MERGE_KEY", F.lit(None).cast(key_type))))

target.alias("t").merge(staged.alias("s"), "t.SUBJECT_ID = s.MERGE_KEY AND t.IS_CURRENT") \
    .whenMatchedUpdate(
        condition="t.HASH_KEY <> s.HASH_KEY",
        set={"IS_CURRENT": "false", "VALID_TO": "current_timestamp()"},
    ).whenNotMatchedInsert(values={
        "SUBJECT_ID": "s.SUBJECT_ID",
        "HASH_KEY": "s.HASH_KEY",
        "IS_CURRENT": "true",
        "VALID_FROM": "current_timestamp()",
        "VALID_TO": "null",
        # ... business columns
    }).execute()
```

### 7.3 Data Quality Checks (Before Promotion)

| Dimension      | Minimum check                                             |
| -------------- | --------------------------------------------------------- |
| Completeness   | Key columns (e.g., `SUBJECT_ID`, `STUDY_ID`) not null     |
| Uniqueness     | Business key unique among current records                 |
| Validity       | Coded values in the allowed set                           |
| Consistency    | Same key resolves to the same entity across systems       |
| Timeliness     | Data arrives within the agreed SLA                        |
| Reconciliation | Target row count matches source within agreed tolerance   |

Failed records are rejected or quarantined **and alerted**, never dropped silently.

## 8. Review Checklists

### 8.1 GxP Pipeline Review

- [ ] None of the prohibited practices in §8.3
- [ ] Audit metadata present on every row (§7.1)
- [ ] Data quality checks and source-vs-target reconciliation in place
- [ ] Errors logged and alerted; no silent failures
- [ ] Transformations deterministic, explainable, and version-controlled
- [ ] Rollback path defined (Delta time travel)
- [ ] PII/PHI and blinded data identified and protected
- [ ] Retention matches the regulatory schedule

### 8.2 Validation Documentation

- **Intended use** defined first.
- **GAMP category** stated (Cat 4 configured pipelines, Cat 5 custom code).
- **Risk assessment** via FMEA; functional risk classified negligible / low / high.
- **Traceability** explicit for every URS → test case.
- **Test environment** is the validation environment, with synthetic or representative data.
- **Evidence** as digital records (platform history, Git) rather than screenshots.

### 8.3 Prohibited Practices — Always Flag

| Violation                                     | Risk                             | Reference                         |
| --------------------------------------------- | -------------------------------- | --------------------------------- |
| Hardcoded credentials                         | Security, data integrity         | 21 CFR Part 11 §11.10; Annex 11 §12 |
| Production deployment without validation      | Unvalidated change in production | Annex 11 §4, §10                  |
| `autoMerge = true` in validated environments  | Uncontrolled schema change       | Annex 11 §10; GAMP 5              |
| Overwriting or deleting raw data              | Loss of original records         | ICH E6(R3) §4.2; 21 CFR 211.68    |
| Interactive clusters for production jobs      | Unvalidated, uncontrolled compute | Annex 11 §4                      |
| Personal identity running pipelines           | Non-attributable actions         | 21 CFR Part 11 §11.10(e)          |
| Deleting Delta history before retention ends  | Loss of audit trail              | Annex 11 §17; Part 11 §11.10(e)   |
| Disabling or bypassing audit trails           | Undetectable changes             | Annex 11 §9; ICH E6(R3)           |

## 9. AI/ML Systems

Document controls for ISPE's eight ML hazard clusters: **dataset quality** (represents intended
use), **preprocessing** (reproducible), **architecture** (rationale), **training/tuning**
(hyperparameters), **evaluation** (independent test set, acceptance criteria), **deployment**
(PQ/UAT before production), **operational data quality** (drift thresholds), and **human
oversight** (human in control, monitoring SOPs).

| Risk           | Functional impact                              | Action                                           |
| -------------- | ---------------------------------------------- | ------------------------------------------------ |
| **Negligible** | No patient/product impact                      | Document rationale; basic testing                |
| **Low**        | Indirect (analytical support tools)            | Risk mitigations; standard validation            |
| **High**       | Patient safety or regulatory submission impact | Full lifecycle validation; continuous monitoring |

## 10. Inspection Readiness

Auditors typically ask, and the answer should be ready:

| Question                            | Evidence                                                  |
| ----------------------------------- | --------------------------------------------------------- |
| How do you ensure data integrity?   | ALCOA+++ controls (§2.3), ACID writes, quality checks     |
| Can you recreate a historical report? | Delta time travel to the report's version or timestamp  |
| How do you control access?          | Catalog grants via groups; service principals; access logs |
| How are changes managed?            | Change requests, CI/CD with approvals, validation records |
| What happens when a load fails?     | Alerts, quarantine, reconciliation reports                |

Keep ready: validation plan and report, risk assessments, SOPs, traceability matrix, change
history with approvals, and ongoing data quality reports.

## 11. Reference Standards

| Document                                         | Covers                                         |
| ------------------------------------------------ | ---------------------------------------------- |
| 21 CFR Part 11; 21 CFR 211.68                    | Electronic records; computerized GMP controls  |
| EU GMP Annex 11                                  | Computerized systems                           |
| GAMP 5, 2nd Ed. (ISPE)                           | Risk-based computerized system validation      |
| ICH E6(R3)                                       | GCP and clinical trial data governance         |
| FDA Computer Software Assurance Guidance (2022)  | Risk-based software assurance                  |
| FDA Data Integrity and Compliance with CGMP (2018) | Data integrity expectations                  |
| MHRA GxP Data Integrity Guidance (2018)          | ALCOA+ definitions and expectations            |
| PIC/S PI 041                                     | Data management and integrity in GMP/GDP       |
| Databricks GxP compliance whitepaper             | Shared responsibility on Databricks            |
