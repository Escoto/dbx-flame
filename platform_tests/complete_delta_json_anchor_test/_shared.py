"""Constants and assertions shared by the complete_delta_json_anchor_test steps.

Deliberately dependency-free: these run as spark_python_task on a cluster, not
under pytest, so a failed assert is what fails the task.

The production shape of a full + delta source after a pause (GH #25). The full feed
resyncs silver from the newest full export alone, read on __EXPORT_DATE; the delta feed
anchors on each record's last_modified_dt, so it applies only the changes made after
what that export already carries, and dates history by when the record changed.
"""

CATALOG = "testing_dev_01"
SCHEMA = "functional_testing"

BRONZE_FULL_TABLE = "ADDRESS_ANCHOR_BRONZE_FULL_1"
BRONZE_DELTA_TABLE = "ADDRESS_ANCHOR_BRONZE_DELTA_1"
SILVER_TABLE = "ADDRESS_ANCHOR_SILVER_1"

VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/source_data"
INBOUND = f"{VOLUME}/inbound"
METADATA = f"{VOLUME}/metadata"

FULL_DIRECTORY = "ADDRESS_ANCHOR_FULL_SOURCE_1"
DELTA_DIRECTORY = "ADDRESS_ANCHOR_DELTA_SOURCE_1"

# The exports under sample_data/, in the order the upstream system sent them.
STEADY_FULL = "ADDRESS_FULL_20251101050000Z.json"  # 001-019
STEADY_INSERT = "ADDRESS_DELTA_20251115050000Z.json"  # inserts 020
FULL_STALE = "ADDRESS_FULL_20260101050000Z.json"  # 20 records, superseded by the resync
DELTA_INSERT = "ADDRESS_DELTA_20260102050000Z.json"  # inserts 021
DELTA_UPDATE = "ADDRESS_DELTA_20260103050000Z.json"  # updates 001
FULL_RESYNC = "ADDRESS_FULL_20260104050000Z.json"  # 18 records: carries both, drops 018-020
DELTA_LATE = "ADDRESS_DELTA_20260105050000Z.json"  # inserts 022

# Round one is the pipeline working as usual; round two is everything the upstream
# system sent while it was paused.
ROUNDS = {
    "steady": [(STEADY_FULL, FULL_DIRECTORY), (STEADY_INSERT, DELTA_DIRECTORY)],
    "backlog": [
        (FULL_STALE, FULL_DIRECTORY),
        (DELTA_INSERT, DELTA_DIRECTORY),
        (DELTA_UPDATE, DELTA_DIRECTORY),
        (FULL_RESYNC, FULL_DIRECTORY),
        (DELTA_LATE, DELTA_DIRECTORY),
    ],
}

# Everything round two sent before the resync: the resync supersedes all of it.
SUPERSEDED = (FULL_STALE, DELTA_INSERT, DELTA_UPDATE)


def uri(n: int) -> str:
    return f"https://r.testing.com/{n:03d}"


STEADY_URIS = {uri(n) for n in range(1, 21)}
STEADY_INSERTED = uri(20)
RESYNC_URIS = {uri(n) for n in range(1, 18)} | {uri(21)}
UPDATED = uri(1)
UPDATED_STREET = "1 Main Street, Unit 5"
LATE_INSERT = uri(22)


def qualified(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def current(df):
    """Only the live version of each address."""
    return df.filter(df["__CURRENT_FLAG"] == "Y")


def expect_current(df, expected: set) -> None:
    """The current set is exactly these addresses, one live row each."""
    live = current(df)
    uris = [row["URI"] for row in live.select("URI").collect()]
    assert len(uris) == len(set(uris)), f"an address has more than one current row: {uris}"
    actual = set(uris)
    assert actual == expected, (
        f"expected {len(expected)} current addresses, found {len(actual)}: "
        f"missing={sorted(expected - actual)} extra={sorted(actual - expected)}"
    )


def expect_absent(df, exports) -> None:
    """None of these exports wrote a single row to silver."""
    for export in exports:
        written = df.filter(df["__FILEPATH"].endswith(export)).count()
        assert written == 0, f"{export} is superseded and should not reach silver"


def opened_at(df, address: str) -> str:
    """When the address's current version opened."""
    row = current(df).filter(df["URI"] == address).select("__START_DATE").first()
    return str(row[0])


def expect_anchor(df, table: str) -> None:
    """Bronze stamped __ANCHOR_DT as a timestamp on every row, and Silver kept it."""
    kind = dict(df.dtypes).get("__ANCHOR_DT")
    assert kind == "timestamp", f"{table}: __ANCHOR_DT is {kind}, expected timestamp"
    nulls = df.filter(df["__ANCHOR_DT"].isNull()).count()
    assert nulls == 0, f"{table}: {nulls} rows have no __ANCHOR_DT"


def expect_stats_columns(spark, table: str, *required: str) -> None:
    """The table keeps data-skipping statistics on the columns the framework filters on."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {qualified(table)}").collect()
    listed = {row.key: row.value for row in rows}.get("delta.dataSkippingStatsColumns", "")
    missing = [column for column in required if column not in listed.split(",")]
    assert not missing, f"{table}: delta.dataSkippingStatsColumns lacks {missing} ({listed!r})"
