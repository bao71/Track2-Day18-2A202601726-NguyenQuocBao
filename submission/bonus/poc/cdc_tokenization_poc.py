"""Bonus PoC: tokenized CDC landing + late-event-safe merge.

Run from repo root:
    .venv\\Scripts\\python.exe submission\\bonus\\poc\\cdc_tokenization_poc.py

The script demonstrates the hard part of topic C in miniature:
- PII is HMAC-tokenized before Bronze is written.
- Silver current state accepts only newer source timestamps.
- Older late events are retained in Bronze but cannot overwrite current truth.
- Any break-glass PII lookup writes an audit row.
"""
from __future__ import annotations

import hashlib
import hmac
import sys
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lakehouse import path, reset  # noqa: E402


SECRET = b"day18-demo-key"


def token(value: str, namespace: str) -> str:
    digest = hmac.new(SECRET, f"{namespace}:{value}".encode(), hashlib.sha256).hexdigest()
    return digest[:24]


def bronze_batch() -> pl.DataFrame:
    raw = pl.DataFrame({
        "trip_id": ["T1", "T2", "T1", "T3", "T1"],
        "op": ["c", "c", "u", "c", "u"],
        "rider_phone": ["0901000001", "0901000002", "0901000001", "0901000003", "0901000001"],
        "driver_citizen_id": ["0791", "0792", "0791", "0793", "0791"],
        "city_id": ["HCM", "HN", "HCM", "DN", "HCM"],
        "status": ["requested", "requested", "completed", "requested", "driver_assigned"],
        "fare_vnd": [0, 0, 128000, 0, 0],
        "source_ts": [
            "2026-08-18T09:00:00",
            "2026-08-18T09:00:05",
            "2026-08-18T09:03:00",
            "2026-08-18T09:03:30",
            "2026-08-18T09:01:00",  # late stale update for T1
        ],
        "source_lsn": [101, 102, 110, 120, 105],
    }).with_columns(
        pl.col("source_ts").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S"),
        pl.col("rider_phone").map_elements(lambda v: token(v, "phone"), return_dtype=pl.String).alias("rider_token"),
        pl.col("driver_citizen_id").map_elements(lambda v: token(v, "citizen_id"), return_dtype=pl.String).alias("driver_token"),
        pl.lit("vault://pii/trips/").add(pl.col("trip_id")).alias("encrypted_pii_pointer"),
    )
    return raw.drop(["rider_phone", "driver_citizen_id"])


def merge_current(silver_path: str, changes: pl.DataFrame) -> None:
    incoming = (
        changes
        .sort(["trip_id", "source_ts", "source_lsn"])
        .group_by("trip_id", maintain_order=True)
        .last()
        .select([
            "trip_id",
            "city_id",
            "status",
            "fare_vnd",
            "source_ts",
            "source_lsn",
            "rider_token",
            "driver_token",
        ])
    )
    dt = DeltaTable(silver_path)
    (
        dt.merge(
            source=incoming.to_arrow(),
            predicate=(
                "t.trip_id = s.trip_id AND "
                "(s.source_ts > t.source_ts OR "
                "(s.source_ts = t.source_ts AND s.source_lsn > t.source_lsn))"
            ),
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


def main() -> None:
    bronze_path = path("bronze", "bonus_cdc_raw_tokenized")
    silver_path = path("silver", "bonus_trips_current")
    audit_path = path("gold", "bonus_pii_access_audit")
    reset(bronze_path, silver_path, audit_path)

    bronze = bronze_batch()
    write_deltalake(bronze_path, bronze.to_arrow(), mode="overwrite", partition_by=["city_id"])

    initial = bronze.filter(pl.col("source_lsn") <= 102)
    write_deltalake(
        silver_path,
        initial.select([
            "trip_id",
            "city_id",
            "status",
            "fare_vnd",
            "source_ts",
            "source_lsn",
            "rider_token",
            "driver_token",
        ]).to_arrow(),
        mode="overwrite",
        partition_by=["city_id"],
    )

    merge_current(silver_path, bronze.filter(pl.col("source_lsn") > 102))

    current = pl.from_arrow(DeltaTable(silver_path).to_pyarrow_table()).sort("trip_id")
    t1 = current.filter(pl.col("trip_id") == "T1").row(0, named=True)
    assert t1["status"] == "completed", "stale late update overwrote newer trip state"
    assert t1["fare_vnd"] == 128000
    assert "0901000001" not in str(bronze.to_dicts()), "raw phone leaked into Bronze"

    audit = pl.DataFrame({
        "requester": ["support.oncall@company.vn"],
        "ticket": ["INC-2026-0818-42"],
        "reason": ["rider dispute break-glass lookup"],
        "table_name": ["bonus_cdc_raw_tokenized"],
        "columns": ["encrypted_pii_pointer"],
        "row_count": [1],
        "access_ts": ["2026-08-18T09:10:00"],
    }).with_columns(pl.col("access_ts").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S"))
    write_deltalake(audit_path, audit.to_arrow(), mode="overwrite")

    print("Bronze rows:", DeltaTable(bronze_path).count())
    print("Silver current rows:", DeltaTable(silver_path).count())
    print("T1 final state:", {"status": t1["status"], "fare_vnd": t1["fare_vnd"], "source_lsn": t1["source_lsn"]})
    print("Audit rows:", DeltaTable(audit_path).count())
    print("PASS: PII tokenized, late stale CDC rejected, audit written.")


if __name__ == "__main__":
    main()
