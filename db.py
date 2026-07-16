# ══════════════════════════════════════════════════════════════════════════════
# db.py — Central SQL data access layer
# ══════════════════════════════════════════════════════════════════════════════

import psycopg2
import psycopg2.extras
import pandas as pd
import streamlit as st
import json
from datetime import datetime
from pathlib import PurePosixPath
from uuid import uuid4
import mimetypes
import re
import urllib.parse
import requests


# ══════════════════════════════════════════════════════════════════════════════
# CONNECTION
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_connection():
    return psycopg2.connect(
        st.secrets["connections"]["supabase"]["url"],
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )

def _get_conn():
    """
    Returns a healthy connection, rolling back any broken transaction first.
    Always use this instead of get_connection() directly inside functions.
    """
    try:
        conn = get_connection()
        if conn.closed:
            st.cache_resource.clear()
            return get_connection()
        # If stuck in a failed transaction, roll it back
        if conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
            conn.rollback()
        return conn
    except Exception:
        st.cache_resource.clear()
        return get_connection()
    
def _run(query: str, params: tuple = (), fetch: str = None):
    """
    Internal helper. fetch = None | "one" | "df"
    """
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch == "one":
                result = cur.fetchone()
                conn.commit()
                return dict(result) if result else {}
            elif fetch == "df":
                rows = cur.fetchall()
                conn.commit()
                return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
            else:
                conn.commit()
                return None
    except Exception:
        conn.rollback()
        st.cache_resource.clear()
        raise


# ══════════════════════════════════════════════════════════════════════════════
# REAGENTS
# ══════════════════════════════════════════════════════════════════════════════

def load_reagents(division: str = None) -> pd.DataFrame:
    # NOTE: qc1reagents has no division column (only QC1 exists in Supabase
    # right now, so nothing is filtered). The `division` param is accepted
    # for call-signature compatibility with the rest of the app but ignored.
    conn = _get_conn()
    df = pd.read_sql("""
        SELECT
            id::text            AS "ID",
            reagent_name        AS "Reagent Name",
            management_code     AS "Management Code",
            manufacturer        AS "Manufacturer",
            catalog_number      AS "Catalog Number",
            lot_number          AS "Lot Number",
            storage             AS "Storage",
            arrival_qty         AS "Arrival Qty",
            arrival_date        AS "Arrival Date",
            expiration_date     AS "Expiration Date",
            usage_info          AS "Usage",
            notes               AS "Notes",
            ghs                 AS "GHS",
            coa_link            AS "CoA Link",
            msds_link           AS "MSDS Link",
            pic                 AS "PIC",
            solid_liquid        AS "Solid/Liquid",
            created             AS "Created",
            updated             AS "Updated",
            COALESCE(is_voided, false) AS "Is Voided",
            void_event_id       AS "Void Event ID",
            void_reason         AS "Void Reason",
            voided_by           AS "Voided By",
            void_timestamp      AS "Void Timestamp",
            superseded_by       AS "Superseded By"
        FROM qc1reagents
        ORDER BY id ASC
    """, conn)
    if not df.empty and "ID" in df.columns:
        df["ID"] = df["ID"].astype(str).str.strip()
    return df


def insert_reagent(
    division, reagent_name, management_code, manufacturer="",
    catalog_number="", lot_number="", storage="", arrival_qty=1,
    arrival_date=None, expiration_date=None, usage_info="",
    notes="", ghs="", coa_link="", msds_link="", pic="", solid_liquid=""
) -> str:
    # NOTE: division is accepted for call-signature compatibility but
    # qc1reagents has no division column, so it isn't written.
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1reagents (
                    reagent_name, management_code, manufacturer,
                    catalog_number, lot_number, storage, arrival_qty,
                    arrival_date, expiration_date, usage_info, notes,
                    ghs, coa_link, msds_link, pic, solid_liquid,
                    created, is_voided
                ) VALUES (
                    %s,%s,%s, %s,%s,%s,%s,
                    %s,%s,%s,%s, %s,%s,%s,%s,%s,
                    %s, FALSE
                ) RETURNING id
            """, (
                reagent_name, management_code, manufacturer,
                catalog_number, lot_number, storage, arrival_qty,
                arrival_date, expiration_date, usage_info, notes,
                ghs, coa_link, msds_link, pic, solid_liquid,
                datetime.now(),
            ))
            new_id = cur.fetchone()[0]
            conn.commit()
        return str(new_id)
    except Exception:
        _rollback_quietly(conn)
        raise


def void_reagent(reagent_id, void_reason, voided_by,
                 void_event_id="", superseded_by=""):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1reagents SET
                    is_voided=TRUE, void_reason=%s, voided_by=%s,
                    void_timestamp=%s, void_event_id=%s,
                    superseded_by=%s, updated=%s
                WHERE id=%s
            """, (void_reason, voided_by, datetime.now(),
                  void_event_id, superseded_by, datetime.now(), int(reagent_id)))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_reagent_field(reagent_id, field, value):
    ALLOWED = {
        "lot_number","expiration_date","storage","pic","ghs",
        "manufacturer","catalog_number","notes","coa_link",
        "msds_link","solid_liquid","usage_info",
    }
    if field not in ALLOWED:
        raise ValueError(f"Field '{field}' not updatable.")
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE qc1reagents SET {field}=%s, updated=%s WHERE id=%s",
                (value, datetime.now(), int(reagent_id))
            )
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# UNITS
# ══════════════════════════════════════════════════════════════════════════════

def load_units(division: str) -> pd.DataFrame:
    conn = _get_conn()
    df = pd.read_sql("""
        SELECT
            reagent_id      AS "Reagent ID",
            unit_label      AS "Unit Label",
            status          AS "Status",
            opened_date     AS "Opened Date",
            disposed_date   AS "Disposed Date",
            disposed_by     AS "Disposed By",
            notes           AS "Notes",
            pao             AS "PAO",
            opened_by       AS "Opened By",
            reason          AS "Reason",
            COALESCE(is_voided, false) AS "Is Voided",
            void_event_id   AS "Void Event ID",
            void_reason     AS "Void Reason",
            voided_by       AS "Voided By",
            void_timestamp  AS "Void Timestamp",
            superseded_by   AS "Superseded By",
            eao             AS "EAO",
            id              AS "_unit_db_id"
        FROM qc1units
        WHERE division = %s
        ORDER BY reagent_id ASC, id ASC
    """, conn, params=(division,))
    if not df.empty:
        df["Reagent ID"] = df["Reagent ID"].astype(str).str.strip()
        df["Status"] = df["Status"].str.strip().str.lower()
        df["Status"] = df["Status"].replace("unused", "unopened")
    return df


# ── Label print tracking ──────────────────────────────────────────────────────
# Persisted flags so a label that has been generated/printed stays blocked from
# reprinting even after a page reload / new session (a session-only flag reset
# on every reload and did not actually prevent reprints).

def mark_reagent_label_printed(reagent_id):
    """Mark a reagent's registration label set as printed (per-reagent)."""
    _execute("UPDATE qc1reagents SET label_printed=TRUE WHERE id=%s", (int(reagent_id),))


def reset_reagent_labels_printed():
    """Clear the printed flag on all reagents (the 'Reset Printed' escape hatch)."""
    _execute("UPDATE qc1reagents SET label_printed=FALSE WHERE COALESCE(label_printed, FALSE)=TRUE")


def mark_cell_culture_label_printed(cell_id):
    """Mark a cell culture entry's label as printed (per-entry — qc1cellculture
    has no separate units table, so this is the only flag needed, mirroring
    mark_reagent_label_printed rather than the per-unit reagent pattern)."""
    _execute("UPDATE qc1cellculture SET label_printed=TRUE WHERE cell_id=%s", (int(cell_id),))


def reset_cell_culture_labels_printed():
    """Clear the printed flag on all cell culture entries (the 'Reset Printed' escape hatch)."""
    _execute("UPDATE qc1cellculture SET label_printed=FALSE WHERE COALESCE(label_printed, FALSE)=TRUE")


def mark_unit_label_printed(division, reagent_id, unit_label):
    """Mark one specific unit's opened-label as printed (per-unit).
    Keyed by reagent_id + unit_label only — qc1units rows are inserted without a
    division value (matches update_unit_opened), so filtering on it matches none.
    The `division` param is kept for call-signature consistency but not used."""
    _execute(
        "UPDATE qc1units SET label_printed=TRUE "
        "WHERE reagent_id=%s AND unit_label=%s",
        (str(reagent_id), unit_label),
    )


def mark_opened_unit_labels_printed(division, reagent_id):
    """Mark every currently-opened unit of a reagent as printed (batch reprint)."""
    _execute(
        "UPDATE qc1units SET label_printed=TRUE "
        "WHERE reagent_id=%s AND LOWER(status)='opened'",
        (str(reagent_id),),
    )


def reset_unit_labels_printed(division=None):
    """Clear the printed flag on all units ('Reset Printed')."""
    _execute(
        "UPDATE qc1units SET label_printed=FALSE "
        "WHERE COALESCE(label_printed, FALSE)=TRUE"
    )


def insert_units_bulk(division: str, reagent_id: str, count: int):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            for i in range(1, count + 1):
                cur.execute("""
                    INSERT INTO qc1units (division, reagent_id, unit_label, status, created_at)
                    VALUES (%s, %s, %s, 'unused', %s)
                """, (division, reagent_id, f"Unit {i}", datetime.now()))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_unit_opened(division, reagent_id, unit_label,
                       opened_date, opened_by, pao="", eao=""):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1units SET
                    status=%s, opened_date=%s, opened_by=%s, pao=%s, eao=%s
                WHERE division=%s AND reagent_id=%s AND unit_label=%s
            """, ("opened", opened_date, opened_by, pao, eao,
                  division, str(reagent_id), unit_label))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_unit_disposed(division, reagent_id, unit_label,
                         disposed_date, disposed_by, reason):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1units SET
                    status=%s, disposed_date=%s, disposed_by=%s, reason=%s
                WHERE division=%s AND reagent_id=%s AND unit_label=%s
            """, ("disposed", disposed_date, disposed_by, reason,
                  division, str(reagent_id), unit_label))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def void_unit(division, reagent_id, unit_label, void_reason,
              voided_by, void_event_id="", superseded_by=""):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1units SET
                    is_voided=TRUE, void_reason=%s, voided_by=%s,
                    void_timestamp=%s, void_event_id=%s, superseded_by=%s
                WHERE division=%s AND reagent_id=%s AND unit_label=%s
            """, (void_reason, voided_by, datetime.now(),
                  void_event_id, superseded_by,
                  division, str(reagent_id), unit_label))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# REAGENT USAGES
# ══════════════════════════════════════════════════════════════════════════════

def load_reagent_usages(division: str) -> pd.DataFrame:
    conn = _get_conn()
    df = pd.read_sql("""
        SELECT
            box_id      AS "Box ID",
            usage       AS "Usage",
            quantity    AS "Quantity",
            bottle_key  AS "Bottle Key",
            unit        AS "Unit"
        FROM qc1reagentusages
        WHERE division = %s
        ORDER BY id ASC
    """, conn, params=(division,))
    return df


def load_usage_mapping(division: str) -> pd.DataFrame:
    """
    Returns reagent usages grouped by box_id — used as the usage mapping
    (replaces QC1_UsageMapping sheet lookups).
    """
    return load_reagent_usages(division)


def save_reagent_usages(division: str, box_id: str, usages_list: list):
    """
    Replaces all usage rows for a given box_id then inserts fresh ones.
    usages_list: [{"usage": "...", "quantity": "..."}, ...]
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM qc1reagentusages WHERE division=%s AND box_id=%s",
                (division, str(box_id))
            )
            for u in usages_list:
                cur.execute("""
                    INSERT INTO qc1reagentusages
                        (division, box_id, usage, quantity, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (division, str(box_id),
                      u.get("usage",""), u.get("quantity",""),
                      datetime.now()))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise

# ══════════════════════════════════════════════════════════════════════════════
# CELL CULTURE
# ══════════════════════════════════════════════════════════════════════════════

def load_cell_culture(division: str = None) -> pd.DataFrame:
    # QC1CellCulture uses cell_id as PK, not id
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    cell_id::text       AS "Cell ID",
                    cell_name           AS "Cell Name",
                    control_no          AS "Control No.",
                    manufacturer        AS "Manufacturer",
                    lot_number          AS "Lot Number",
                    passage_number      AS "Passage Number",
                    num_cells           AS "Num Cells",
                    ln2_tank            AS "LN2 Tank",
                    storage             AS "Storage",
                    entry_date          AS "Entry Date",
                    coa_link            AS "CoA Link",
                    remarks             AS "Remarks",
                    pic                 AS "PIC",
                    status              AS "Status",
                    registered_at       AS "Registered At",
                    COALESCE(is_voided, false) AS "Is Voided",
                    void_event_id       AS "Void Event ID",
                    void_reason         AS "Void Reason",
                    voided_by           AS "Voided By",
                    void_timestamp      AS "Void Timestamp",
                    superseded_by       AS "Superseded By"
                FROM qc1cellculture
                ORDER BY cell_id ASC
            """)
            rows = cur.fetchall()
        conn.commit()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception:
        _rollback_quietly(conn)
        raise


def insert_cell_culture(
    division, cell_name, control_no="", manufacturer="",
    lot_number="", passage_number="", num_cells="", ln2_tank="",
    storage="", entry_date=None, coa_link="", remarks="", pic="",
    status="unavailable"
) -> str:
    # cell_id is SERIAL — auto-generated, don't pass it in
    return _execute_returning_id("""
        INSERT INTO qc1cellculture (
            cell_name, control_no, manufacturer, lot_number,
            passage_number, num_cells, ln2_tank, storage,
            entry_date, coa_link, remarks, pic, status,
            registered_at, is_voided
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE)
        RETURNING cell_id
    """, (
        cell_name, control_no, manufacturer, lot_number,
        passage_number, num_cells, ln2_tank, storage,
        entry_date, coa_link, remarks, pic, status,
        datetime.now(),
    ))


def update_cell_culture_status(cell_id, status):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE qc1cellculture SET status=%s WHERE cell_id=%s",
                (status, cell_id),
            )
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def void_cell_culture(cell_id, void_reason, voided_by,
                      void_event_id="", superseded_by=""):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1cellculture SET
                    is_voided=TRUE, void_reason=%s, voided_by=%s,
                    void_timestamp=%s, void_event_id=%s, superseded_by=%s
                WHERE cell_id=%s
            """, (void_reason, voided_by, datetime.now(),
                  void_event_id, superseded_by, cell_id))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def load_cell_usage(division: str = None) -> pd.DataFrame:
    # QC1CellUsages uses usage_id as PK
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    usage_id::text      AS "Usage ID",
                    cell_id             AS "Cell ID",
                    cell_name           AS "Cell Name",
                    control_no          AS "Control No.",
                    date_of_use         AS "Date of Use",
                    time_of_use         AS "Time of Use",
                    used_by             AS "User",
                    remarks             AS "Remarks",
                    outcome             AS "Outcome",
                    timestamp           AS "Timestamp"
                FROM qc1cellusages
                ORDER BY usage_id ASC
            """)
            rows = cur.fetchall()
        conn.commit()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception:
        _rollback_quietly(conn)
        raise


def insert_cell_usage(
    division, cell_id, cell_name, control_no="",
    date_of_use=None, time_of_use="", used_by="",
    remarks="", outcome=""
) -> str:
    # qc1cellusages has no division column and its PK is usage_id (SERIAL),
    # not id — confirmed against the live schema.
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1cellusages (
                    cell_id, cell_name, control_no,
                    date_of_use, time_of_use, used_by,
                    remarks, outcome, timestamp, created_at
                ) VALUES (%s,%s,%s, %s,%s,%s, %s,%s,%s,%s)
                RETURNING usage_id
            """, (
                cell_id, cell_name, control_no,
                date_of_use, time_of_use, used_by,
                remarks, outcome, datetime.now(), datetime.now()
            ))
            new_id = cur.fetchone()[0]
            conn.commit()
        return str(new_id)
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# STANDARDS
# ══════════════════════════════════════════════════════════════════════════════



def insert_standard(
    division, standard_id, item_name, management_code="",
    manufacturer="", catalog_number="", lot_number="", storage="",
    expiry_date=None, arrival_date=None, ghs="", assay="",
    capacity_per_unit="", preparation_procedure="", coa_url="",
    msds_url="", note="", pic="", registration_date=None,
    serial_number="", aliquot_serial="", status=""
) -> str:
    # NOTE: division is accepted for call-signature compatibility but
    # qc1standards has no division column, so it isn't written.
    # standard_id is the SERIAL PK — auto-generated on insert, returned for
    # the inserted row's unique identifier.
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1standards (
                    standard_id, item_name, management_code,
                    manufacturer, catalog_number, lot_number, storage,
                    expiry_date, arrival_date, ghs, assay,
                    capacity_per_unit, preparation_procedure, coa_url,
                    msds_url, note, pic, registration_date,
                    serial_number, aliquot_serial, status,
                    timestamp, is_voided
                ) VALUES (
                    %s,%s,%s, %s,%s,%s,%s,
                    %s,%s,%s,%s, %s,%s,%s,
                    %s,%s,%s,%s, %s,%s,%s,
                    %s, FALSE
                ) RETURNING standard_id
            """, (
                standard_id, item_name, management_code,
                manufacturer, catalog_number, lot_number, storage,
                expiry_date, arrival_date, ghs, assay,
                capacity_per_unit, preparation_procedure, coa_url,
                msds_url, note, pic, registration_date,
                serial_number, aliquot_serial, status,
                datetime.now(),
            ))
            new_id = cur.fetchone()[0]
            conn.commit()
        return str(new_id)
    except Exception:
        _rollback_quietly(conn)
        raise


def update_standard_field(record_id, field, value):
    """
    Updates ONE unit row, identified by its unique `id` (primary key) —
    NOT by `standard_id`, since standard_id is now shared across every unit
    in a registration batch (see insert_standards_bulk). Filtering by
    standard_id here would incorrectly update every sibling unit at once.
    """
    ALLOWED = {
        "lot_number","expiry_date","storage","pic","ghs","status",
        "manufacturer","catalog_number","note","coa_url","msds_url",
        "opened_date","pao","eao","opened_by",
        "disposed_date","disposed_by","dispose_reason",
    }
    if field not in ALLOWED:
        raise ValueError(f"Field '{field}' not updatable.")
    try:
        row_id = int(record_id) if record_id else None
    except (ValueError, TypeError):
        raise ValueError(f"Invalid id: {record_id}")

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE qc1standards SET {field}=%s, timestamp=%s WHERE id=%s",
                (value, datetime.now(), row_id)
            )
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def void_standard(record_id, void_reason, voided_by,
                  void_event_id="", superseded_by=""):
    """
    Voids ONE unit row, identified by its unique `id` (primary key) —
    NOT by `standard_id` (see note in update_standard_field above).
    """
    try:
        row_id = int(record_id) if record_id else None
    except (ValueError, TypeError):
        raise ValueError(f"Invalid id: {record_id}")

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1standards SET
                    is_voided=TRUE, void_reason=%s, voided_by=%s,
                    void_timestamp=%s, void_event_id=%s, superseded_by=%s
                WHERE id=%s
            """, (void_reason, voided_by, datetime.now(),
                  void_event_id, superseded_by, row_id))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def insert_standard_usage(
    division, standard_id, aliquot_serial="", item_name="",
    management_code="", lot_number="", unit_label="",
    date_of_use=None, purpose="", amount="", pao="", eao="",
    used_by="", note="", aliquot_id="", is_aliquot="",
    aliquot_name="", aliquot_status="",
    conn=None
) -> str:
    # NOTE: division param kept for call-signature compat but qc1standardusages
    # has no division column — do not include it in the INSERT.
    # If a caller passes in its own `conn` (e.g. to batch several aliquot rows
    # into one atomic transaction), reuse it and let the caller commit/rollback.
    # Otherwise fall back to the original standalone-commit-per-call behavior.
    _external_conn = conn is not None
    conn = conn if _external_conn else _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1standardusages (
                    standard_id, aliquot_serial, item_name,
                    management_code, lot_number, unit_label,
                    date_of_use, purpose, amount, pao, eao,
                    used_by, note, timestamp,
                    aliquot_id, is_aliquot, aliquot_name, aliquot_status,
                    created_at
                ) VALUES (
                    %s,%s,%s, %s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s,%s,%s,
                    %s,%s,%s,%s, %s
                ) RETURNING usage_id
            """, (
                standard_id, aliquot_serial, item_name,
                management_code, lot_number, unit_label,
                date_of_use, purpose, amount, pao, eao,
                used_by, note, datetime.now(),
                aliquot_id, is_aliquot, aliquot_name, aliquot_status,
                datetime.now()
            ))
            new_id = cur.fetchone()[0]
            if not _external_conn:
                conn.commit()
        return str(new_id)
    except Exception:
        if not _external_conn:
            _rollback_quietly(conn)
        raise


def load_standard_disposal(division: str = None) -> pd.DataFrame:
    # QC1StandardDisposal uses disposal_id as PK
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    disposal_id::text   AS "Disposal ID",
                    standard_id         AS "Standard ID",
                    aliquot_serial      AS "Aliquot Serial",
                    item_name           AS "Item Name",
                    management_code     AS "Management Code",
                    lot_number          AS "Lot Number",
                    unit_label          AS "Unit Label",
                    disposal_date       AS "Disposal Date",
                    reason              AS "Reason",
                    pic                 AS "PIC",
                    timestamp           AS "Timestamp"
                FROM qc1standarddisposal
                ORDER BY disposal_id ASC
            """)
            rows = cur.fetchall()
        conn.commit()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception:
        _rollback_quietly(conn)
        raise


def insert_standard_disposal(
    division, standard_id, aliquot_serial="", item_name="",
    management_code="", lot_number="", unit_label="",
    disposal_date=None, reason="", pic=""
) -> str:
    # NOTE: division param kept for call-signature compat but qc1standarddisposal
    # has no division column — do not include it in the INSERT.
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1standarddisposal (
                    standard_id, aliquot_serial, item_name,
                    management_code, lot_number, unit_label,
                    disposal_date, reason, pic, timestamp, created_at
                ) VALUES (
                    %s,%s,%s, %s,%s,%s,
                    %s,%s,%s,%s,%s
                ) RETURNING disposal_id
            """, (
                standard_id, aliquot_serial, item_name,
                management_code, lot_number, unit_label,
                disposal_date, reason, pic,
                datetime.now(), datetime.now()
            ))
            new_id = cur.fetchone()[0]
            conn.commit()
        return str(new_id)
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# AUDIT TRAIL
# ══════════════════════════════════════════════════════════════════════════════

def log_audit(division: str, username: str, role: str,
              action: str, category: str, detail: str):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1audittrail
                    (division, username, role, action, category, detail, timestamp)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (division, username, role, action, category, detail, datetime.now()))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def load_audit_trail(division: str, limit: int = 500) -> pd.DataFrame:
    conn = _get_conn()
    return pd.read_sql("""
        SELECT
            timestamp   AS "Timestamp",
            username    AS "User",
            role        AS "Role",
            action      AS "Action",
            category    AS "Category",
            detail      AS "Detail"
        FROM qc1audittrail
        WHERE division = %s
        ORDER BY timestamp DESC
        LIMIT %s
    """, conn, params=(division, limit))


# ══════════════════════════════════════════════════════════════════════════════
# USERS
# ══════════════════════════════════════════════════════════════════════════════

def load_users(division: str = None) -> pd.DataFrame:
    conn = _get_conn()
    if division:
        df = pd.read_sql("""
            SELECT
                username        AS "Username",
                full_name       AS "Full Name",
                password        AS "Password",
                role            AS "Role",
                division        AS "Division",
                status          AS "Status",
                registered_at   AS "Registered At",
                approved_by     AS "Approved By",
                approved_at     AS "Approved At",
                last_active_at  AS "Last Activated At"
            FROM users WHERE division=%s ORDER BY registered_at DESC
        """, conn, params=(division,))
    else:
        df = pd.read_sql("""
            SELECT
                username        AS "Username",
                full_name       AS "Full Name",
                password        AS "Password",
                role            AS "Role",
                division        AS "Division",
                status          AS "Status",
                registered_at   AS "Registered At",
                approved_by     AS "Approved By",
                approved_at     AS "Approved At",
                last_active_at  AS "Last Activated At"
            FROM users ORDER BY registered_at DESC
        """, conn)
    return df


def get_user(username: str) -> dict:
    return _run(
        "SELECT * FROM users WHERE username=%s",
        (username,), fetch="one"
    )


def insert_user(username, full_name, password_hash, role, division):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users
                    (username, full_name, password, role, division,
                     status, registered_at)
                VALUES (%s,%s,%s,%s,%s,'Pending',%s)
            """, (username, full_name, password_hash, role, division,
                  datetime.now()))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_user_status(username: str, status: str, approved_by: str = ""):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users SET status=%s, approved_by=%s, approved_at=%s
                WHERE username=%s
            """, (status, approved_by, datetime.now(), username))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_user_role(username: str, role: str):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET role=%s WHERE username=%s",
                        (role, username))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_user_last_active(username: str):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET last_active_at=%s WHERE username=%s",
                        (datetime.now(), username))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def get_usernames_by_role(role: str, division: str = None) -> list:
    """
    Returns usernames of all Active users with the given role (matched
    case-insensitively), optionally restricted to one division. Backs
    "notify all Supervisors"-style features — role/division/status are plain
    columns on `users`, so this needs no join or separate roles table.
    """
    if division:
        df = _read_df(
            "SELECT username FROM users WHERE LOWER(role)=LOWER(%s) AND division=%s AND LOWER(status)='active'",
            (role, division),
        )
    else:
        df = _read_df(
            "SELECT username FROM users WHERE LOWER(role)=LOWER(%s) AND LOWER(status)='active'",
            (role,),
        )
    return df["username"].dropna().tolist() if not df.empty else []


# ══════════════════════════════════════════════════════════════════════════════
# NOTIFICATIONS
# ══════════════════════════════════════════════════════════════════════════════

_NOTIFICATIONS_TABLE_ENSURED = False


def _ensure_notifications_table():
    """
    Idempotently creates the notifications table backing the in-app
    notification badge/panel. One row per recipient is written at creation
    time (fan-out on write) rather than joined against live role membership
    at read time — matching the denormalize-at-write-time convention already
    used by qc1audittrail/gmpvoidevents. Safe to call repeatedly; runs once
    per process.
    """
    global _NOTIFICATIONS_TABLE_ENSURED
    if _NOTIFICATIONS_TABLE_ENSURED:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    notification_id     SERIAL PRIMARY KEY,
                    recipient_username   TEXT,
                    notification_type    TEXT,
                    message              TEXT,
                    source_table         TEXT,
                    source_record_id     TEXT,
                    is_read              BOOLEAN DEFAULT FALSE,
                    read_at              TIMESTAMP,
                    created_at           TIMESTAMP
                )
            """)
        conn.commit()
        _NOTIFICATIONS_TABLE_ENSURED = True
    except Exception:
        _rollback_quietly(conn)
        raise


def insert_notification(recipient_username, notification_type, message,
                         source_table="", source_record_id=""):
    _ensure_notifications_table()
    _execute("""
        INSERT INTO notifications (
            recipient_username, notification_type, message,
            source_table, source_record_id, is_read, created_at
        ) VALUES (%s,%s,%s, %s,%s, FALSE,%s)
    """, (recipient_username, notification_type, message,
          source_table, source_record_id, datetime.now()))


def insert_notifications_bulk(recipient_usernames: list, notification_type, message,
                               source_table="", source_record_id=""):
    """Fan out one identical notification to each recipient in a single call."""
    if not recipient_usernames:
        return
    _ensure_notifications_table()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            now = datetime.now()
            cur.executemany("""
                INSERT INTO notifications (
                    recipient_username, notification_type, message,
                    source_table, source_record_id, is_read, created_at
                ) VALUES (%s,%s,%s, %s,%s, FALSE,%s)
            """, [
                (recipient, notification_type, message, source_table, source_record_id, now)
                for recipient in recipient_usernames
            ])
        conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def load_notifications(username: str, unread_only: bool = False) -> pd.DataFrame:
    _ensure_notifications_table()
    query = """
        SELECT
            notification_id   AS "ID",
            notification_type AS "Type",
            message           AS "Message",
            source_table      AS "Source Table",
            source_record_id  AS "Source Record ID",
            is_read           AS "Is Read",
            read_at           AS "Read At",
            created_at        AS "Created At"
        FROM notifications
        WHERE recipient_username = %s
    """
    if unread_only:
        query += " AND is_read = FALSE"
    query += " ORDER BY created_at DESC"
    return _read_df(query, (username,))


def count_unread_notifications(username: str) -> int:
    _ensure_notifications_table()
    df = _read_df(
        "SELECT COUNT(*) AS cnt FROM notifications WHERE recipient_username=%s AND is_read=FALSE",
        (username,),
    )
    return int(df.iloc[0]["cnt"]) if not df.empty else 0


def mark_notification_read(notification_id):
    _ensure_notifications_table()
    _execute("""
        UPDATE notifications SET is_read = TRUE, read_at = %s
        WHERE notification_id = %s
    """, (datetime.now(), notification_id))


def mark_all_notifications_read(username: str):
    _ensure_notifications_table()
    _execute("""
        UPDATE notifications SET is_read = TRUE, read_at = %s
        WHERE recipient_username = %s AND is_read = FALSE
    """, (datetime.now(), username))


# ══════════════════════════════════════════════════════════════════════════════
# GMP CORRECTIONS
# ══════════════════════════════════════════════════════════════════════════════

def load_gmp_corrections(division: str = None) -> pd.DataFrame:
    # NOTE: the live gmpcorrections table has NO `id`, `status`, `updated_at`,
    # or `division` columns (the PK is `correction_id`, the mod time is
    # `timestamp`). Selecting the old sheet-era names crashed this function,
    # so corrections silently fell back to empty and the ✏️ correction-hover
    # never rendered. Ordered ASC so the last row per (sheet,record,field) is
    # the newest — _build_corrections_lookup / iloc[-1] treat last as latest.
    conn = _get_conn()
    return pd.read_sql("""
        SELECT
            correction_id::text AS "ID",
            sheet_name          AS "Sheet Name",
            record_id           AS "Record ID",
            field_name          AS "Field Name",
            old_value           AS "Old Value",
            new_value           AS "New Value",
            reason              AS "Reason",
            reason_detail       AS "Reason Detail",
            corrected_by        AS "Corrected By",
            corrected_at        AS "Corrected At",
            record_label        AS "Record Label",
            timestamp           AS "Updated At"
        FROM gmpcorrections
        ORDER BY corrected_at ASC
    """, conn)


def insert_gmp_correction(
    sheet_name, record_id, field_name, old_value, new_value,
    reason, corrected_by, record_label="", division=None
):
    # NOTE: `division` is accepted for call-signature compatibility but the live
    # gmpcorrections table has no `division` or `updated_at` column — writing them
    # made every correction INSERT fail (silently swallowed), so corrections never
    # persisted. Real columns only; `timestamp` is the mod-time, `created_at`
    # defaults to NOW().
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gmpcorrections (
                    sheet_name, record_id, field_name,
                    old_value, new_value, reason, corrected_by,
                    corrected_at, record_label, timestamp
                ) VALUES (%s,%s,%s, %s,%s,%s,%s, %s,%s,%s)
            """, (
                sheet_name, str(record_id), field_name,
                str(old_value), str(new_value), reason, corrected_by,
                datetime.now(), record_label, datetime.now()
            ))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# GMP VOID EVENTS
# ══════════════════════════════════════════════════════════════════════════════

def insert_void_event(
    void_event_id, sheet_name, record_id, record_label,
    void_reason, voided_by, superseded_by="", division="", notes=""
):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gmpvoidevents (
                    event_code, sheet_name, record_id, record_label,
                    void_reason, voided_by, void_timestamp,
                    superseded_by, division, notes, created_at
                ) VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s)
            """, (
                void_event_id, sheet_name, str(record_id), record_label,
                void_reason, voided_by, datetime.now(),
                superseded_by, division, notes, datetime.now()
            ))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# STORAGE MAP
# ══════════════════════════════════════════════════════════════════════════════

def insert_storage_slot(division, rack_id, row_num, col_num,
                        reagent_id, reagent_name, management_code) -> str:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1storageslots
                    (division, rack_id, row_num, col_num, reagent_id, reagent_name, management_code, created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (division, rack_id, row_num, col_num, str(reagent_id), reagent_name, management_code, datetime.now()))
            new_id = cur.fetchone()[0]
            conn.commit()
        return str(new_id)
    except Exception:
        _rollback_quietly(conn)
        raise


def delete_storage_slot(slot_id: str):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM qc1storageslots WHERE id = %s", (int(slot_id),))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# STOCK OPNAME
# ══════════════════════════════════════════════════════════════════════════════

def load_stock_opname(division: str) -> pd.DataFrame:
    conn = _get_conn()
    return pd.read_sql("""
        SELECT
            opname_date     AS "Date",
            management_code AS "Management Code",
            reagent_name    AS "Reagent Name",
            system_qty      AS "System Qty",
            actual_qty      AS "Actual Qty",
            difference      AS "Difference",
            remark          AS "Remark",
            pic             AS "PIC",
            timestamp       AS "Timestamp"
        FROM qc1stockopname WHERE division = %s ORDER BY timestamp DESC
    """, conn, params=(division,))


def insert_stock_opname(division, opname_date, management_code,
                        reagent_name, system_qty, actual_qty, remark, pic):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1stockopname
                    (division, opname_date, management_code, reagent_name,
                     system_qty, actual_qty, difference, remark, pic, timestamp)
                VALUES (%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)
            """, (
                division, opname_date, management_code, reagent_name,
                system_qty, actual_qty, actual_qty - system_qty,
                remark, pic, datetime.now()
            ))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# LOW STOCK POINTS (replaces QC1_LowStockPoints / QC1_ReorderPoints)
# ══════════════════════════════════════════════════════════════════════════════

def load_low_stock_points(division: str) -> dict:
    conn = _get_conn()
    df = pd.read_sql("""
        SELECT management_code, warning_threshold, critical_threshold
        FROM qc1lowstockpoints WHERE division = %s
    """, conn, params=(division,))
    result = {}
    for _, row in df.iterrows():
        result[row["management_code"]] = {
            "warning":  int(row["warning_threshold"]),
            "critical": int(row["critical_threshold"]),
        }
    return result


def upsert_low_stock_point(division, management_code, warning, critical):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1lowstockpoints
                    (division, management_code, warning_threshold, critical_threshold, created_at)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (management_code) DO UPDATE SET
                    warning_threshold  = EXCLUDED.warning_threshold,
                    critical_threshold = EXCLUDED.critical_threshold
            """, (division, management_code, warning, critical, datetime.now()))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# EMAIL SETTINGS & RECIPIENTS
# ══════════════════════════════════════════════════════════════════════════════

def load_email_settings() -> dict:
    conn = _get_conn()
    df = pd.read_sql("SELECT setting_name, setting_value FROM emailsettings", conn)
    return dict(zip(df["setting_name"], df["setting_value"])) if not df.empty else {}


def save_email_setting(setting_name: str, setting_value: str):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO emailsettings (setting_name, setting_value, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (setting_name) DO UPDATE SET
                    setting_value = EXCLUDED.setting_value,
                    updated_at = EXCLUDED.updated_at
            """, (setting_name, setting_value, datetime.now()))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def load_email_recipients(division: str) -> pd.DataFrame:
    conn = _get_conn()
    return pd.read_sql("""
        SELECT email, categories, COALESCE(active, TRUE) AS active
        FROM emailrecipients
        WHERE division = %s
        ORDER BY email ASC
    """, conn, params=(division,))


def insert_email_recipient(division: str, email: str, categories: str, active: bool = True):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO emailrecipients (division, email, categories, active, created_at)
                VALUES (%s,%s,%s,%s,%s)
            """, (division, email, categories, active, datetime.now()))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_email_recipient(division: str, email: str, categories: str = None, active: bool = None):
    """Partial update: only fields that are not None get changed."""
    sets, params = [], []
    if categories is not None:
        sets.append("categories=%s"); params.append(categories)
    if active is not None:
        sets.append("active=%s"); params.append(active)
    if not sets:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            params += [division, email]
            cur.execute(
                f"UPDATE emailrecipients SET {', '.join(sets)} WHERE division=%s AND email=%s",
                tuple(params),
            )
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def delete_email_recipient(division: str, email: str):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM emailrecipients WHERE division=%s AND email=%s",
                        (division, email))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# PURCHASES
# ══════════════════════════════════════════════════════════════════════════════

def load_purchases(division: str) -> pd.DataFrame:
    """
    Returns purchases with column names matching purchase_tracking_page's
    EXPECTED_COLUMNS so app.py logic (df["Status"], df["ID"], etc.) works
    unchanged.
    """
    conn = _get_conn()
    df = pd.read_sql("""
        SELECT
            id::text                AS "ID",
            purchase_type            AS "Purchase Type",
            mdvan_date                AS "MDVAN Date",
            pre_order_date            AS "Pre-Order Date",
            estimated_arrival         AS "Estimated Arrival Date",
            reagent_name              AS "Reagent Name",
            management_code           AS "Management Code",
            catalog_number            AS "Catalog Number",
            quantity                  AS "Quantity",
            manufacturer              AS "Manufacturer",
            pic                       AS "PIC",
            status                    AS "Status",
            received_date             AS "Received Date",
            receipt_history           AS "Receipt History",
            cancellation_reason       AS "Cancellation Reason",
            notes                     AS "Notes"
        FROM qc1purchases
        WHERE division = %s
        ORDER BY id ASC
    """, conn, params=(division,))
    if not df.empty:
        df["ID"] = df["ID"].astype(str).str.strip()
        df["Receipt History"] = df["Receipt History"].fillna("[]").astype(str)
    return df


def insert_purchase(
    division, management_code, reagent_name, manufacturer="",
    catalog_number="", quantity=1, pre_order_date=None,
    estimated_arrival=None, pic="", notes="", purchase_type="",
    mdvan_date=None, status="Ordered"
) -> str:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1purchases (
                    division, management_code, reagent_name, manufacturer,
                    catalog_number, quantity, pre_order_date, estimated_arrival,
                    status, pic, notes, receipt_history, purchase_type,
                    mdvan_date, created_at
                ) VALUES (
                    %s,%s,%s,%s, %s,%s,%s,%s,
                    %s,%s,%s,'[]',%s,
                    %s,%s
                ) RETURNING id
            """, (
                division, management_code, reagent_name, manufacturer,
                catalog_number, quantity, pre_order_date, estimated_arrival,
                status, pic, notes, purchase_type,
                mdvan_date, datetime.now(),
            ))
            new_id = cur.fetchone()[0]
            conn.commit()
        return str(new_id)
    except Exception:
        _rollback_quietly(conn)
        raise


def update_purchase(
    purchase_id, new_status, received_date=None, receipt_history=None,
    cancellation_reason=None, mdvan_date=None, pre_order_date=None,
    estimated_arrival=None
):
    """
    Partial update — only overwrites fields that were actually passed in
    (mirrors the old "only if user picked something" sheet logic).
    """
    fields, values = ["status=%s"], [new_status]
    if received_date is not None:
        fields.append("received_date=%s"); values.append(received_date)
    if receipt_history is not None:
        fields.append("receipt_history=%s"); values.append(receipt_history)
    if cancellation_reason is not None:
        fields.append("cancellation_reason=%s"); values.append(cancellation_reason)
    if mdvan_date is not None:
        fields.append("mdvan_date=%s"); values.append(mdvan_date)
    if pre_order_date is not None:
        fields.append("pre_order_date=%s"); values.append(pre_order_date)
    if estimated_arrival is not None:
        fields.append("estimated_arrival=%s"); values.append(estimated_arrival)
    values.append(int(purchase_id))
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE qc1purchases SET {', '.join(fields)} WHERE id=%s",
                tuple(values)
            )
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def load_active_purchase_codes(division: str) -> set:
    conn = _get_conn()
    df = pd.read_sql("""
        SELECT DISTINCT management_code FROM qc1purchases
        WHERE division = %s
          AND status IN ('Ordered','Shipped','Partially Received')
    """, conn, params=(division,))
    if df.empty or "management_code" not in df.columns:
        return set()
    return set(df["management_code"].astype(str).str.strip().unique())


# ══════════════════════════════════════════════════════════════════════════════
# QC2 UNIFIED TABLE ACCESS
# These two functions replace ALL load_data() and save_data() calls for QC2.
# The table_map routes sheet names to SQL table names.
# ══════════════════════════════════════════════════════════════════════════════

QC2_TABLE_MAP = {
    "QC2_Media":                "qc2_media",
    "QC2_Media_Usage":          "qc2_media_usage",
    "QC2_Reagent":              "qc2_reagent",
    "QC2_Reagent_Bottles":      "qc2_reagent_bottles",
    "QC2_Reagent_Usage":        "qc2_reagent_usage",
    "QC2_BI_Inbound":           "qc2_bi_inbound",
    "QC2_BI_Usage":             "qc2_bi_usage",
    "QC2_Culture_Inbound":      "qc2_culture_inbound",
    "QC2_Culture_Usage":        "qc2_culture_usage",
    "QC2_Master_Inventory":     "qc2_master_inventory",
    "QC2_Master_SO":            "qc2_master_so",
    "QC2_Media_GPT":            "qc2_media_gpt",
    "QC2_Master_Barcode":       "qc2_master_barcode",
    "QC2_Master_Barcode_Arrival": "qc2_master_barcode_arrival",
    "QC2_Transaction_Log":      "qc2_transaction_log",
    "QC2_Signatures":           "qc2_signatures",
    "QC2_Master_Media_Limit":   "qc2_media_limit",
    "QC2_Audit_Log":            "audit_trail",
    "AuditTrail":               "audit_trail",
    "Email_Settings":           "email_settings",
    "QC2_EmailRecipients":      "emailrecipients",
}

# Column name mappings: SQL column → Sheet column name
# Only needed where SQL column names differ from original sheet headers
QC2_COLUMN_MAP = {
    "qc2_media": {
        "nama": "Nama", "management_code": "Management_Code",
        "batch": "Batch", "catalog_number": "Catalog_Number",
        "arrival_date": "Arrival_Date",
        "tgl_buat": "Tgl_Buat", "exp": "Exp",
        "qty_box_in": "Qty_Box_In", "isi_per_box": "Isi_per_Box",
        "isi_per_pack": "Isi_per_Pack", "total_pack_stock": "Total_Pack_Stock",
        "total_pcs_stock": "Total_Pcs_Stock", "status": "Status",
        "ghs": "GHS", "tipe_entry": "Tipe_Entry", "pic": "PIC",
        "disposal_reason": "Disposal_Reason", "disposal_date": "Disposal_Date",
        "pic_disposal": "PIC_Disposal", "coa": "CoA", "storage_temp": "Storage_Temp",
        "manufacturer": "Manufacturer", "gpt_required": "GPT_Required",
        "void_reason": "Void_Reason", "void_by": "Void_By", "void_date": "Void_Date",
    },
    "qc2_media_usage": {
        "batch": "Batch", "management_code": "Management_Code", "nama": "Nama",
        "pack_released": "Pack_Released", "pcs_used": "Pcs_Used", "pcs_disposed": "Pcs_Disposed",
        "jumlah_pakai": "Jumlah_Pakai", "keterangan": "Keterangan", "pic": "PIC",
        "timestamp": "Timestamp",
    },
    "qc2_reagent": {
        "reagent_name": "Reagent_Name", "management_code": "Management_Code",
        "manufacturer": "Manufacturer", "catalog_number": "Catalog_Number",
        # NOTE: "catalog_number"/"Catalog_Number" above is a pre-existing,
        # mislabeled field that actually stores the Lot/Vendor Batch No.
        # (see the Register Arrival save block in appqc2.py). "catalog_no"
        # is the genuine, separate Catalog Number field — kept under a
        # different name deliberately so the legacy field is untouched.
        "catalog_no": "Catalog_No",
        # "reagent_type"/"unit" were never mapped here, so save_qc2_table()
        # silently dropped them before every INSERT (its column list comes
        # straight from this dict) and load_qc2_table() never renamed them
        # back — "Reagent_Type"/"Unit" were therefore always absent from the
        # app-side dataframe, which is the root cause of the Inventory grid
        # showing Item Name in the Type/Unit columns (see appqc2.py's
        # reagent groupby — a missing column made its "column exists?"
        # fallback alias Reagent_Name into those output columns).
        "reagent_type": "Reagent_Type", "unit": "Unit",
        "arrival_date": "Arrival_Date", "disposal_date": "Disposal_Date",
        "quantity": "Quantity", "qty_in": "Qty_In", "qty_out": "Qty_Out",
        "status": "Status", "expiration_date": "Expiration_Date",
        "open_date": "Open_Date", "storage": "Storage", "pic": "PIC",
        "function_desc": "Function", "ghs": "GHS", "coa": "CoA", "msds": "MSDS",
        "disposal_reason": "Disposal_Reason", "pao_days": "PAO_days",
        "void_reason": "Void_Reason", "void_by": "Void_By", "void_date": "Void_Date",
    },
    "qc2_reagent_bottles": {
        "management_code": "Management_Code", "bottle_id": "Bottle_ID",
        "status": "Status", "opened_date": "Opened_Date",
        "disposed_date": "Disposed_Date", "pic": "PIC", "notes": "Notes",
    },
    "qc2_bi_inbound": {
        "management_code": "Management Code", "item_name": "Item Name",
        "lot": "Lot", "catalog_number": "Catalog_Number",
        "qty_in_pcs": "Qty In (Pcs)", "exp": "Exp",
        "cfu": "CFU", "pic": "PIC", "timestamp": "Timestamp",
        "storage_temp": "Storage_Temp", "manufacturer": "Manufacturer",
        "coa": "CoA", "ghs": "GHS", "status": "Status",
        "void_reason": "Void_Reason", "void_by": "Void_By", "void_date": "Void_Date",
    },
    "qc2_bi_usage": {
        "management_code": "Management Code", "item_name": "Item Name",
        "qty_out_pcs": "Qty Out (Pcs)", "purpose": "Purpose",
        "pic": "PIC", "timestamp": "Timestamp",
    },
    "qc2_culture_inbound": {
        "management_code": "Management Code", "item_name": "Item Name",
        "lot": "Lot", "catalog_number": "Catalog_Number",
        "qty_in_pcs": "Qty In (Pcs)", "exp": "Exp",
        "arrival": "Arrival", "pic": "PIC", "timestamp": "Timestamp",
        "storage_temp": "Storage_Temp", "manufacturer": "Manufacturer",
        "vendor_passage": "Vendor_Passage", "atcc_no": "ATCC_No",
        "cfu": "CFU", "coa": "CoA", "ghs": "GHS", "status": "Status",
        "function": "Function",
        "void_reason": "Void_Reason", "void_by": "Void_By", "void_date": "Void_Date",
    },
    "qc2_culture_usage": {
        "management_code": "Management Code", "item_name": "Item Name",
        "qty_out_pcs": "Qty Out (Pcs)", "purpose": "Purpose",
        "pic": "PIC", "timestamp": "Timestamp",
    },
    "qc2_master_inventory": {
        "inventory_id": "Inventory_ID", "category": "Category",
        "management_code": "Management_Code", "item_name": "Item_Name",
        "batch_lot": "Batch_Lot", "arrival_date": "Arrival_Date",
        "exp_date": "Exp_Date", "qty_in": "Qty_In", "qty_out": "Qty_Out",
        "stock": "Stock", "status": "Status", "pic": "PIC", "ghs": "GHS",
        "storage_temp": "Storage_Temp", "manufacturer": "Manufacturer",
        "coa": "CoA", "msds": "MSDS",
    },
    "qc2_master_so": {
        "timestamp": "Timestamp", "so_date": "Date", "pic": "PIC",
        "category": "Category", "management_code": "Management_Code",
        "item_name": "Item_Name", "system_stock": "System_Stock",
        "actual_stock": "Actual_Stock", "difference": "Difference",
        "status": "Status", "remark": "Notes",
        "coa_msds": "COA_MSDS", "ghs_category": "GHS_Category",
    },
    "qc2_media_gpt": {
        "batch": "Batch", "nama_media": "Nama_Media", "strain": "Strain",
        "cfu_kontrol": "CFU_Kontrol", "cfu_uji": "CFU_Uji",
        "recovery": "Recovery", "hasil": "Hasil", "pic": "PIC", "tanggal": "Tanggal",
    },
    "qc2_master_barcode": {
        "gtin_barcode": "GTIN_Barcode", "management_code": "Management_Code",
        "nama_item": "Nama_Item", "kategori": "Kategori",
        "manufacturer": "Manufacturer", "storage_temp": "Storage_Temp",
    },
    "qc2_master_barcode_arrival": {
        "barcode_unique": "Barcode_Unique", "management_code": "Management_Code",
        "nama_item": "Nama_Item", "batch": "Batch", "expired": "Expired",
        "tgl_datang": "Tgl_Datang", "status": "Status",
    },
    "qc2_media_limit": {
        "management_code": "Management Code", "media_name": "Media_Name",
        "min_pack_limit": "Min_Pack_Limit", "set_by": "Set_By",
        "set_date": "Set_Date", "notes": "Notes",
    },
    "qc2_transaction_log": {
        "timestamp": "Timestamp", "inventory_id": "Inventory_ID",
        "action": "Action", "qty_change": "Qty_Change",
        "new_stock": "New_Stock", "pic": "PIC", "detail": "Detail",
    },
    "audit_trail": {
        "username": "User_ID", "role": "User_Role",
        "action": "Aksi", "category": "Kategori",
        "detail": "Detail", "timestamp": "Timestamp",
        "acting_for_username": "Acting_For", "delegation_status": "Delegation_Status",
        "module": "Module",
    },
    "emailrecipients": {
        "email": "Email", "categories": "Categories", "active": "Active",
    },
}


def load_qc2_table(sheet_name: str) -> pd.DataFrame:
    sql_table = QC2_TABLE_MAP.get(sheet_name)
    if not sql_table:
        return pd.DataFrame()

    if sql_table == "email_settings":
        return _read_df('SELECT setting_name AS "Setting_Name", setting_value AS "Setting_Value" FROM emailsettings')

    if sql_table == "audit_trail":
        return _select_table("qc1audittrail", AUDIT_COLS, order_by="id ASC", division="QC2")

    # ---- Safely check if 'id' column exists ----
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = %s AND column_name = 'id'
                )
            """, (sql_table,))
            has_id = cur.fetchone()[0]
        conn.commit()
    except Exception:
        _rollback_quietly(conn)
        has_id = False
    finally:
        conn.close()

    # ---- Build query with or without ORDER BY ----
    order_clause = "ORDER BY id ASC" if has_id else ""
    query = f"SELECT * FROM {sql_table} {order_clause}".strip()
    df = _read_df(query)

    # ---- Drop internal columns ----
    for col in ["id", "created_at"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # ---- Rename SQL columns to sheet headers ----
    col_map = QC2_COLUMN_MAP.get(sql_table, {})
    if col_map:
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    return df


def save_qc2_table(sheet_name: str, df: pd.DataFrame):
    """
    Replaces save_data(df, sheet_name) for QC2.
    Clears the SQL table and re-inserts all rows from the DataFrame.
    This mirrors the Google Sheets ws.clear() + ws.update() pattern exactly.
    """
    sql_table = QC2_TABLE_MAP.get(sheet_name)
    if not sql_table:
        return  # Unknown sheet — silently skip

    col_map = QC2_COLUMN_MAP.get(sql_table, {})
    # Reverse the column map: sheet name → SQL name
    reverse_map = {v: k for k, v in col_map.items()}

    # Rename DataFrame columns from sheet names back to SQL names
    df_sql = df.copy()
    if reverse_map:
        rename = {k: v for k, v in reverse_map.items() if k in df_sql.columns}
        df_sql = df_sql.rename(columns=rename)

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            # Clear existing rows
            cur.execute(f"DELETE FROM {sql_table}")

            # Insert all rows from DataFrame
            if not df_sql.empty:
                cols = [c for c in df_sql.columns if c not in ["id", "created_at"]]
                placeholders = ", ".join(["%s"] * len(cols))
                col_names    = ", ".join(cols)
                for _, row in df_sql.iterrows():
                    values = [
                        None if pd.isna(row.get(c, None)) or str(row.get(c, "")) in ["nan", "None", "NaT"]
                        else str(row.get(c, ""))
                        for c in cols
                    ]
                    try:
                        cur.execute(
                            f"INSERT INTO {sql_table} ({col_names}) VALUES ({placeholders})",
                            values
                        )
                    except Exception:
                        pass  # Skip rows that fail rather than aborting the whole save
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


# =============================================================================
# SCHEMA-COMPATIBLE SQL OVERRIDES
# =============================================================================
# The Supabase schema supplied for this migration has a single QC1 table set.
# Most QC1 tables do not have a division column, so these definitions override
# the earlier partial migration functions that still queried non-existent
# division columns. They also centralize rollback handling for read failures so
# a cached psycopg2 connection is never left in PostgreSQL's aborted transaction
# state.

from psycopg2 import sql


def _clean_sql_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if text in {"", "-", "nan", "NaN", "None", "NaT"}:
        return None
    return value


def _rollback_quietly(conn):
    try:
        conn.rollback()
    except Exception:
        pass


def _read_df(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = _get_conn()
    try:
        df = pd.read_sql(query, conn, params=params)
        conn.commit()
        return df
    except Exception:
        _rollback_quietly(conn)
        raise


def _execute(query: str, params: tuple = ()) -> None:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def _execute_returning_id(query: str, params: tuple = ()) -> str:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            new_id = cur.fetchone()[0]
        conn.commit()
        return str(new_id)
    except Exception:
        _rollback_quietly(conn)
        raise


def _get_table_columns(table: str) -> set:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s
            """, (table,))
            cols = {r[0] for r in cur.fetchall()}
        conn.commit()
        return cols
    except Exception:
        _rollback_quietly(conn)
        return set()


_QC2_CULTURE_FUNCTION_COLUMN_ENSURED = False


def _ensure_qc2_culture_function_column():
    """
    Idempotently adds the nullable 'function' column to qc2_culture_inbound
    so Standard Strain arrivals can save the Function/Tujuan text entered in
    Register Arrival (previously captured on the form but never persisted).
    Purely additive (ADD COLUMN IF NOT EXISTS) — safe to call repeatedly;
    existing rows simply get NULL. Runs once per process.
    """
    global _QC2_CULTURE_FUNCTION_COLUMN_ENSURED
    if _QC2_CULTURE_FUNCTION_COLUMN_ENSURED:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE qc2_culture_inbound ADD COLUMN IF NOT EXISTS function TEXT")
        conn.commit()
        _QC2_CULTURE_FUNCTION_COLUMN_ENSURED = True
    except Exception:
        _rollback_quietly(conn)
        raise


_STD_TRACKING_COLUMNS_ENSURED = False


def _ensure_std_tracking_columns():
    """
    Idempotently adds the nullable columns backing the opt-in "Per Use"
    cumulative-volume tracking feature on Standards. Purely additive
    (ADD COLUMN IF NOT EXISTS) — safe to call repeatedly, and existing rows
    simply get NULL, which the app treats as "Per Bottle" (the prior
    default behavior). Runs once per process.
    """
    global _STD_TRACKING_COLUMNS_ENSURED
    if _STD_TRACKING_COLUMNS_ENSURED:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            for table in ("qc1standards", "qc1standardsquarantine"):
                cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS usage_tracking_mode TEXT")
                cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS tracking_unit TEXT")
                cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS low_stock_threshold TEXT")
            # qty_received on qc1standards only — quarantine already has it from
            # the original schema. Deferred-quantity workflow: Register Usage
            # captures it on the placeholder row at first use instead of
            # registration ever writing it to qc1standards.
            cur.execute("ALTER TABLE qc1standards ADD COLUMN IF NOT EXISTS qty_received TEXT")
        conn.commit()
        _STD_TRACKING_COLUMNS_ENSURED = True
    except Exception:
        _rollback_quietly(conn)
        raise


_REAGENT_ALIQUOT_TABLE_ENSURED = False


def _ensure_reagent_aliquot_table():
    """
    Idempotently creates qc1reagentaliquots — the reagent-side equivalent of
    qc1standardusages' aliquot rows. A reagent unit ("Unit 1") can be split
    into N aliquots ("MGMT/25-07 Unit 1-1", "...-1-2", ...) when it's opened;
    each aliquot is one row here, tracked independently until disposed.
    Unlike Standards (which logs disposal in a separate qc1standarddisposal
    table), disposal is recorded inline on the row itself — matching how
    qc1units already records reagent disposal inline rather than in a log
    table. Safe to call repeatedly; runs once per process.
    """
    global _REAGENT_ALIQUOT_TABLE_ENSURED
    if _REAGENT_ALIQUOT_TABLE_ENSURED:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS qc1reagentaliquots (
                    aliquot_usage_id SERIAL PRIMARY KEY,
                    reagent_id       TEXT,
                    unit_label       TEXT,
                    management_code  TEXT,
                    item_name        TEXT,
                    lot_number       TEXT,
                    aliquot_serial   TEXT,
                    aliquot_id       TEXT,
                    aliquot_name     TEXT,
                    aliquot_status   TEXT,
                    opened_date      DATE,
                    pao              TEXT,
                    eao              TEXT,
                    opened_by        TEXT,
                    note             TEXT,
                    disposed_date    DATE,
                    disposed_by      TEXT,
                    dispose_reason   TEXT,
                    timestamp        TIMESTAMP,
                    created_at       TIMESTAMP
                )
            """)
        conn.commit()
        _REAGENT_ALIQUOT_TABLE_ENSURED = True
    except Exception:
        _rollback_quietly(conn)
        raise


_STD_REGISTERED_BY_COLUMN_ENSURED = False


def _ensure_std_registered_by_column():
    """
    Idempotently adds an immutable 'registered_by' column to
    qc1standardsquarantine, capturing the session username at registration
    time. Distinct from the existing 'pic' column, which is free-text and
    user-editable, so it can't reliably answer "who actually submitted this"
    when resolving who to notify on approve/reject.
    """
    global _STD_REGISTERED_BY_COLUMN_ENSURED
    if _STD_REGISTERED_BY_COLUMN_ENSURED:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE qc1standardsquarantine ADD COLUMN IF NOT EXISTS registered_by TEXT")
        conn.commit()
        _STD_REGISTERED_BY_COLUMN_ENSURED = True
    except Exception:
        _rollback_quietly(conn)
        raise


def _select_table(table: str, sheet_to_sql: dict, order_by: str = "id ASC", division: str = None) -> pd.DataFrame:
    live_cols = _get_table_columns(table)

    # Only select columns that actually exist on the live table
    usable = {sheet_col: db_col for sheet_col, db_col in sheet_to_sql.items() if db_col in live_cols}
    missing = {sheet_col: db_col for sheet_col, db_col in sheet_to_sql.items() if db_col not in live_cols}

    if not usable:
        # Nothing maps to a real column — return an empty frame with expected headers
        return pd.DataFrame(columns=list(sheet_to_sql.keys()))

    select_parts = [
        sql.SQL("{} AS {}").format(sql.Identifier(db_col), sql.Identifier(sheet_col))
        for sheet_col, db_col in usable.items()
    ]

    has_division_filter = bool(division) and "division" in live_cols
    where_clause = sql.SQL(" WHERE {} = %s").format(sql.Identifier("division")) if has_division_filter else sql.SQL("")
    params = (division,) if has_division_filter else None

    order_col = order_by.split()[0].strip('"')
    if order_col in live_cols:
        query = sql.SQL("SELECT {} FROM {}{} ORDER BY {}").format(
            sql.SQL(", ").join(select_parts),
            sql.Identifier(table),
            where_clause,
            sql.SQL(order_by),
        )
    else:
        query = sql.SQL("SELECT {} FROM {}{}").format(
            sql.SQL(", ").join(select_parts),
            sql.Identifier(table),
            where_clause,
        )

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        conn.commit()
        df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame(columns=list(usable.keys()))
    except Exception:
        _rollback_quietly(conn)
        raise

    # Add back any missing expected columns as empty, so downstream code
    # that expects them (e.g. df["ID"]) doesn't KeyError
    for sheet_col in missing:
        df[sheet_col] = ""

    # Postgres BOOLEAN columns (e.g. is_voided) come back as a real bool dtype.
    # The gspread-compat shim writes text like "TRUE" into these cells, and
    # pandas refuses to put a string into a bool-dtype column
    # ("Invalid value 'TRUE' for dtype 'bool'"). Normalise any bool column to
    # "TRUE"/"FALSE" strings so both the shim and the app's
    # str(...).upper() == "TRUE" checks work. On write-back, Postgres coerces
    # 'TRUE'/'FALSE' back into the boolean column.
    for _c in df.columns:
        if df[_c].dtype == bool:
            df[_c] = df[_c].map(lambda v: "TRUE" if v else "FALSE")

    # Preserve original column order
    ordered_cols = [c for c in sheet_to_sql.keys() if c in df.columns]
    return df[ordered_cols]


def _replace_table_from_df(table: str, df: pd.DataFrame, sheet_to_sql: dict, preserve_id: bool = True):
    if df is None:
        df = pd.DataFrame(columns=list(sheet_to_sql.keys()))
    df_sql = df.copy()
    cols = [c for c in df_sql.columns if c in sheet_to_sql]
    db_cols = [sheet_to_sql[c] for c in cols]

    # Two sheet columns can map to the same db column in some *_COLS dicts.
    # Writing both would list the db column twice in the INSERT and fail
    # with "specified more than once" — keep only the first sheet column
    # for each underlying db column.
    seen_db_cols = set()
    deduped = [(c, db_c) for c, db_c in zip(cols, db_cols) if not (db_c in seen_db_cols or seen_db_cols.add(db_c))]
    cols = [c for c, _ in deduped]
    db_cols = [db_c for _, db_c in deduped]

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(table)))
            for _, row in df_sql[cols].iterrows():
                values = [_clean_sql_value(row.get(c)) for c in cols]
                if not preserve_id:
                    filtered = [(c, v) for c, v in zip(db_cols, values) if c != "id"]
                    db_cols_use = [c for c, _ in filtered]
                    values = [v for _, v in filtered]
                else:
                    db_cols_use = db_cols
                if not db_cols_use:
                    continue
                cur.execute(
                    sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.Identifier(table),
                        sql.SQL(", ").join(sql.Identifier(c) for c in db_cols_use),
                        sql.SQL(", ").join(sql.Placeholder() for _ in db_cols_use),
                    ),
                    values,
                )
            if "id" in db_cols and preserve_id:
                cur.execute(
                    "SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE((SELECT MAX(id) FROM "
                    + table + "), 1), true)",
                    (table,),
                )
        conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


QC1_REAGENT_COLS = {
    "ID": "id", "Reagent Name": "reagent_name", "Management Code": "management_code",
    "Manufacturer": "manufacturer", "Catalog Number": "catalog_number", "Lot Number": "lot_number",
    "Storage": "storage", "Arrival Qty": "arrival_qty", "Arrival Date": "arrival_date",
    "Expiration Date": "expiration_date", "Usage": "usage_info", "Notes": "notes",
    "GHS": "ghs", "CoA Link": "coa_link", "MSDS Link": "msds_link", "PIC": "pic",
    "Solid/Liquid": "solid_liquid", "Created": "created", "Updated": "updated",
    "Is Voided": "is_voided", "Void Event ID": "void_event_id", "Void Reason": "void_reason",
    "Voided By": "voided_by", "Void Timestamp": "void_timestamp", "Superseded By": "superseded_by",
    "Label Printed": "label_printed",
}

QC1_UNIT_COLS = {
    "_unit_db_id": "id", "Reagent ID": "reagent_id", "Unit Label": "unit_label", "Status": "status",
    "Opened Date": "opened_date", "Disposed Date": "disposed_date", "Disposed By": "disposed_by",
    "Notes": "notes", "PAO": "pao", "Opened By": "opened_by", "Reason": "reason",
    "Is Voided": "is_voided", "Void Event ID": "void_event_id", "Void Reason": "void_reason",
    "Voided By": "voided_by", "Void Timestamp": "void_timestamp", "Superseded By": "superseded_by",
    "EAO": "eao", "Created At": "created_at",
}

QC1_USAGE_COLS = {
    "_usage_db_id": "id", "Box ID": "box_id", "Usage": "usage", "Quantity": "quantity",
    "Bottle Key": "bottle_key", "Unit": "unit", "Created At": "created_at",
}

QC1_CELL_COLS = {
    "Cell ID": "cell_id", "Cell Name": "cell_name", "Control No.": "control_no",
    "Manufacturer": "manufacturer", "Lot Number": "lot_number", "Passage Number": "passage_number",
    "Num Cells": "num_cells", "LN2 Tank": "ln2_tank", "Storage": "storage",
    "Entry Date": "entry_date", "CoA Link": "coa_link", "Remarks": "remarks", "PIC": "pic",
    "Status": "status", "Registered At": "registered_at", "Is Voided": "is_voided",
    "Void Event ID": "void_event_id", "Void Reason": "void_reason", "Voided By": "voided_by",
    "Void Timestamp": "void_timestamp", "Superseded By": "superseded_by", "Created At": "created_at",
    "Label Printed": "label_printed",
}

QC1_CELL_USAGE_COLS = {
    "Usage ID": "usage_id", "Cell ID": "cell_id", "Cell Name": "cell_name",
    "Control No.": "control_no", "Date of Use": "date_of_use", "Time of Use": "time_of_use",
    "User": "used_by", "Remarks": "remarks", "Outcome": "outcome", "Timestamp": "timestamp",
    "Created At": "created_at",
}

QC1_STANDARD_COLS = {
    "ID": "standard_id", "Standard ID": "standard_id", "Unit ID": "id", "Item Name": "item_name",
    "Management Code": "management_code", "Manufacturer": "manufacturer",
    "Catalog Number": "catalog_number", "Lot Number": "lot_number", "Storage": "storage",
    "Expiry Date": "expiry_date", "Arrival Date": "arrival_date", "GHS": "ghs",
    "Assay": "assay", "Qty Received": "qty_received", "Capacity Per Unit": "capacity_per_unit",
    "Preparation Procedure": "preparation_procedure", "CoA URL": "coa_url",
    "MSDS URL": "msds_url", "Note": "note", "PIC": "pic",
    "Registration Date": "registration_date", "Serial Number": "serial_number",
    "Aliquot Serial": "aliquot_serial", "Status": "status", "Opened Date": "opened_date",
    "PAO": "pao", "EAO": "eao", "Opened By": "opened_by", "Disposed Date": "disposed_date",
    "Disposed By": "disposed_by", "Dispose Reason": "dispose_reason", "Timestamp": "timestamp",
    "Is Voided": "is_voided", "Void Event ID": "void_event_id", "Void Reason": "void_reason",
    "Voided By": "voided_by", "Void Timestamp": "void_timestamp", "Superseded By": "superseded_by",
    "Usage Tracking Mode": "usage_tracking_mode", "Tracking Unit": "tracking_unit",
    "Low Stock Threshold": "low_stock_threshold",
}
# Note: unlike other QC1_*_COLS maps, this one deliberately has no "Created At"
# key. STD_COLS in standards_page() (app_sql.py) has no "Created At" field —
# the page compares live headers against STD_COLS exactly to decide whether
# to auto-migrate, so any extra mapped key here causes a permanent mismatch
# (and a pandas column-count crash once row data no longer matches headers).

# Quarantine is a distinct pre-approval intake record, not a filtered view of
# qc1standards — it has its own columns (QID, Qty Received, Approved By/Date)
# and lacks several Standards-only fields (Standard ID, Registration Date,
# Serial Number, Opened/Disposed fields), so it needs its own table.
QC1_STANDARD_QUARANTINE_COLS = {
    "QID": "qid", "Item Name": "item_name",
    "Management Code": "management_code", "Manufacturer": "manufacturer",
    "Catalog Number": "catalog_number", "Lot Number": "lot_number", "Storage": "storage",
    "Qty Received": "qty_received", "Arrival Date": "arrival_date", "Expiry Date": "expiry_date",
    "GHS": "ghs", "Assay": "assay", "Capacity Per Unit": "capacity_per_unit",
    "Preparation Procedure": "preparation_procedure", "CoA URL": "coa_url",
    "MSDS URL": "msds_url", "Note": "note", "PIC": "pic", "Status": "status",
    "Registered By": "registered_by",
    "Approved By": "approved_by", "Approved Date": "approved_date", "Timestamp": "timestamp",
    "Is Voided": "is_voided", "Void Event ID": "void_event_id", "Void Reason": "void_reason",
    "Voided By": "voided_by", "Void Timestamp": "void_timestamp", "Superseded By": "superseded_by",
    "Usage Tracking Mode": "usage_tracking_mode", "Tracking Unit": "tracking_unit",
    "Low Stock Threshold": "low_stock_threshold",
}

QC1_STANDARD_USAGE_COLS = {
    "Usage ID": "usage_id", "Standard ID": "standard_id",
    "Aliquot Serial": "aliquot_serial", "Item Name": "item_name",
    "Management Code": "management_code", "Lot Number": "lot_number", "Unit Label": "unit_label",
    "Date of Use": "date_of_use", "Purpose": "purpose", "Amount": "amount", "PAO": "pao",
    "EAO": "eao", "User": "used_by", "Note": "note", "Timestamp": "timestamp",
    "Aliquot ID": "aliquot_id", "Is Aliquot": "is_aliquot", "Aliquot Name": "aliquot_name",
    "Aliquot Status": "aliquot_status",
    # "Is Voided" is read-only here (not written by this module) so it's safe to
    # surface without affecting the header-count invariant _load() relies on for
    # the other QC1_*_COLS maps above — it lets the per-use remaining-volume calc
    # (and the existing void banner in the Usage tab) see real void status instead
    # of always defaulting to "" when the column happens to exist on the live table.
    "Is Voided": "is_voided",
}
# Note: deliberately no "Created At" key — USAGE_COLS in standards_page()
# (app_sql.py) has no "Created At" field, and _load() compares live headers
# against USAGE_COLS exactly to decide whether to auto-migrate (see the
# same note on QC1_STANDARD_COLS above).

QC1_STANDARD_DISPOSAL_COLS = {
    "Disposal ID": "disposal_id", "Standard ID": "standard_id",
    "Aliquot Serial": "aliquot_serial", "Item Name": "item_name",
    "Management Code": "management_code", "Lot Number": "lot_number", "Unit Label": "unit_label",
    "Disposal Date": "disposal_date", "Reason": "reason", "PIC": "pic", "Timestamp": "timestamp",
}
# Note: deliberately no "Created At" key — same reason as QC1_STANDARD_USAGE_COLS above.

# Reagent aliquots — mirrors QC1_STANDARD_USAGE_COLS' aliquot fields, adapted
# to qc1units (Reagent ID + Unit Label identify the parent bottle instead of
# Standard ID). Disposal is recorded inline on the row itself (disposed_date/
# disposed_by/dispose_reason) rather than in a separate disposal-log table,
# matching how qc1units itself records reagent disposal inline.
QC1_REAGENT_ALIQUOT_COLS = {
    "Aliquot Usage ID": "aliquot_usage_id", "Reagent ID": "reagent_id",
    "Unit Label": "unit_label", "Management Code": "management_code",
    "Item Name": "item_name", "Lot Number": "lot_number",
    "Aliquot Serial": "aliquot_serial", "Aliquot ID": "aliquot_id",
    "Aliquot Name": "aliquot_name", "Aliquot Status": "aliquot_status",
    "Opened Date": "opened_date", "PAO": "pao", "EAO": "eao", "Opened By": "opened_by",
    "Note": "note", "Disposed Date": "disposed_date", "Disposed By": "disposed_by",
    "Dispose Reason": "dispose_reason", "Timestamp": "timestamp",
}

QC1_PURCHASE_COLS = {
    "ID": "id", "Management Code": "management_code", "Reagent Name": "reagent_name",
    "Manufacturer": "manufacturer", "Catalog Number": "catalog_number", "Quantity": "quantity",
    "Pre-Order Date": "pre_order_date", "Estimated Arrival Date": "estimated_arrival",
    "Received Date": "received_date", "Status": "status", "PIC": "pic", "Notes": "notes",
    "Receipt History": "receipt_history", "Cancellation Reason": "cancellation_reason",
    "Purchase Type": "purchase_type", "MDVAN Date": "mdvan_date", "Created At": "created_at",
}

QC1_STORAGE_MAP_COLS = {
    "ID": "id", "Rack ID": "rack_id", "Rack Name": "rack_name", "Rows": "rows",
    "Cols": "cols", "Storage Type": "storage_type", "Form": "form",
    "Status Type": "status_type", "Layout Type": "layout_type", "Created At": "created_at",
}

QC1_STORAGE_SLOT_COLS = {
    "Rack ID": "rack_id", "Row": "rows", "Col": "cols", "Zone": "zone",
    "Status Type": "status_type", "Reagent Name": "reagent_name",
    "Management Code": "management_code", "Reagent Status": "reagent_status",
}

QC1_STOCK_OPNAME_COLS = {
    "SO ID": "so_id", "SO Date": "opname_date", "SO Period": "so_period", "Reagent ID": "reagent_id",
    "Reagent Name": "reagent_name", "Management Code": "management_code",
    "Unopened System": "unopened_system", "Unopened Actual": "unopened_actual",
    "Unopened Diff": "unopened_diff", "Opened System": "opened_system",
    "Opened Actual": "opened_actual", "Opened Diff": "opened_diff",
    "Status": "status", "Notes": "remark", "PIC": "pic", "Timestamp": "timestamp",
}

QC1_LOW_STOCK_COLS = {
    "ID": "id", "Management Code": "management_code",
    "Warning Threshold": "warning_threshold", "Critical Threshold": "critical_threshold",
    "Created At": "created_at",
}

USER_COLS = {
    "ID": "id", "Division": "division", "Username": "username", "Full Name": "full_name",
    "Password": "password", "Role": "role", "Status": "status", "Registered At": "registered_at",
    "Approved By": "approved_by", "Approved At": "approved_at", "Last Activated At": "last_active_at",
}

AUDIT_COLS = {
    "ID": "id", "Division": "division", "User_ID": "username", "User_Name": "username",
    "User_Role": "role", "Aksi": "action", "Kategori": "category", "Detail": "detail",
    "Timestamp": "timestamp", "Username": "username", "Action": "action", "Category": "category",
    "Role": "role", "Acting_For": "acting_for_username", "Delegation_Status": "delegation_status",
    "Module": "module",
}

SHEET_TABLES = {
    "Users": ("users", USER_COLS),
    "AuditTrail": ("qc1audittrail", AUDIT_COLS),
    "QC2_Audit_Log": ("qc1audittrail", AUDIT_COLS),
    "QC1_Reagents": ("qc1reagents", QC1_REAGENT_COLS),
    "QC1_Units": ("qc1units", QC1_UNIT_COLS),
    "QC1_ReagentUsages": ("qc1reagentusages", QC1_USAGE_COLS),
    "QC1_UsageMapping": ("qc1reagentusages", QC1_USAGE_COLS),
    "QC1_CellCulture": ("qc1cellculture", QC1_CELL_COLS),
    "QC1_CellUsage": ("qc1cellusages", QC1_CELL_USAGE_COLS),
    "QC1_CellUsages": ("qc1cellusages", QC1_CELL_USAGE_COLS),
    "QC1_Standards": ("qc1standards", QC1_STANDARD_COLS),
    "QC1_StandardsQuarantine": ("qc1standardsquarantine", QC1_STANDARD_QUARANTINE_COLS),
    "QC1_StandardUsage": ("qc1standardusages", QC1_STANDARD_USAGE_COLS),
    "QC1_StandardUsages": ("qc1standardusages", QC1_STANDARD_USAGE_COLS),
    "QC1_StandardDisposal": ("qc1standarddisposal", QC1_STANDARD_DISPOSAL_COLS),
    "QC1_ReagentAliquots": ("qc1reagentaliquots", QC1_REAGENT_ALIQUOT_COLS),
    "QC1_Purchases": ("qc1purchases", QC1_PURCHASE_COLS),
    "QC1_StorageMap": ("qc1storagemap", QC1_STORAGE_MAP_COLS),
    "QC1_StorageSlots": ("qc1storageslot", QC1_STORAGE_SLOT_COLS),
    "QC1_StockOpname": ("qc1stockopname", QC1_STOCK_OPNAME_COLS),
    "QC1_LowStockPoints": ("qc1lowstockpoints", QC1_LOW_STOCK_COLS),
    "QC1_ReorderPoints": ("qc1lowstockpoints", QC1_LOW_STOCK_COLS),
    "GMP_Corrections": ("gmpcorrections", {
        "ID": "id", "Division": "division", "Sheet Name": "sheet_name", "Record ID": "record_id",
        "Field Name": "field_name", "Old Value": "old_value", "New Value": "new_value",
        "Reason": "reason", "Status": "status", "Corrected By": "corrected_by",
        "Corrected At": "corrected_at", "Record Label": "record_label", "Updated At": "updated_at",
        "Created At": "created_at",
    }),
    "GMP_VoidEvents": ("gmpvoidevents", {
        "ID": "id", "Void Event ID": "void_event_id", "Sheet Name": "sheet_name",
        "Record ID": "record_id", "Record Label": "record_label", "Void Reason": "void_reason",
        "Voided By": "voided_by", "Void Timestamp": "void_timestamp",
        "Superseded By": "superseded_by", "Division": "division", "Notes": "notes",
        "Created At": "created_at",
    }),
}

# _select_table() defaults to "ORDER BY id ASC", but a few tables use a
# differently-named primary key (e.g. cell_id, usage_id) instead of "id".
# Since that default order_col isn't a real column on those tables,
# _select_table() silently drops ORDER BY entirely for them, which means
# read order is whatever Postgres happens to return (not guaranteed stable,
# especially since every write goes through a full DELETE+re-INSERT of the
# table — see _replace_table_from_df). Any app code that reads rows once to
# resolve a positional row index and later writes via that index (e.g. the
# gspread-compat update_cell shim) depends on this order being stable across
# reads, so these need an explicit, deterministic ORDER BY.
SHEET_TABLE_ORDER_BY = {
    "QC1_CellCulture": "cell_id ASC",
    "QC1_CellUsage": "usage_id ASC",
    "QC1_CellUsages": "usage_id ASC",
    "QC1_StandardsQuarantine": "qid ASC",
    "QC1_StandardUsage": "usage_id ASC",
    "QC1_StandardUsages": "usage_id ASC",
    "QC1_StandardDisposal": "disposal_id ASC",
    "QC1_ReagentAliquots": "aliquot_usage_id ASC",
}

def _normalize_sheet_name(sheet_name: str) -> str:
    return str(sheet_name or "").strip()


def load_reagents(division: str = None) -> pd.DataFrame:
    df = _select_table("qc1reagents", QC1_REAGENT_COLS)
    if not df.empty:
        df["ID"] = df["ID"].astype(str).str.strip()
    return df


def insert_reagent(
    division, reagent_name, management_code, manufacturer="",
    catalog_number="", lot_number="", storage="", arrival_qty=1,
    arrival_date=None, expiration_date=None, usage_info="",
    notes="", ghs="", coa_link="", msds_link="", pic="", solid_liquid=""
) -> str:
    return _execute_returning_id("""
        INSERT INTO qc1reagents (
            reagent_name, management_code, manufacturer, catalog_number, lot_number,
            storage, arrival_qty, arrival_date, expiration_date, usage_info, notes,
            ghs, coa_link, msds_link, pic, solid_liquid, created, is_voided
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE)
        RETURNING id
    """, (
        reagent_name, management_code, manufacturer, catalog_number, lot_number,
        storage, arrival_qty, arrival_date, expiration_date, usage_info, notes,
        ghs, coa_link, msds_link, pic, solid_liquid, datetime.now(),
    ))


def load_units(division: str = None) -> pd.DataFrame:
    df = _select_table("qc1units", QC1_UNIT_COLS)
    if not df.empty:
        df["Reagent ID"] = df["Reagent ID"].astype(str).str.strip()
        df["Status"] = df["Status"].fillna("").astype(str).str.strip().str.lower().replace("unused", "unopened")
    return df


def insert_units_bulk(division: str, reagent_id: str, count: int):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            for i in range(1, int(count) + 1):
                cur.execute(
                    "INSERT INTO qc1units (reagent_id, unit_label, status, created_at) VALUES (%s,%s,'unused',%s)",
                    (str(reagent_id), f"Unit {i}", datetime.now()),
                )
        conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_unit_opened(division, reagent_id, unit_label,
                       opened_date, opened_by, pao="", eao=""):
    _execute("""
        UPDATE qc1units
        SET status='opened', opened_date=%s, opened_by=%s, pao=%s, eao=%s
        WHERE reagent_id=%s AND unit_label=%s
    """, (opened_date, opened_by, pao, eao, str(reagent_id), unit_label))


def update_unit_disposed(division, reagent_id, unit_label,
                         disposed_date, disposed_by, reason):
    _execute("""
        UPDATE qc1units
        SET status='disposed', disposed_date=%s, disposed_by=%s, reason=%s
        WHERE reagent_id=%s AND unit_label=%s
    """, (disposed_date, disposed_by, reason, str(reagent_id), unit_label))


def load_reagent_usages(division: str = None) -> pd.DataFrame:
    return _select_table("qc1reagentusages", QC1_USAGE_COLS)


def load_usage_mapping(division: str = None) -> pd.DataFrame:
    return load_reagent_usages(division)


def save_reagent_usages(division: str, box_id: str, usages_list: list):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM qc1reagentusages WHERE box_id=%s AND COALESCE(bottle_key,'')=''", (str(box_id),))
            for u in usages_list:
                cur.execute("""
                    INSERT INTO qc1reagentusages (box_id, usage, quantity, created_at)
                    VALUES (%s,%s,%s,%s)
                """, (str(box_id), u.get("usage", ""), u.get("quantity", ""), datetime.now()))
        conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def save_unit_usages(box_id: str, bottle_key: str, unit: str, usages_list: list):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM qc1reagentusages WHERE box_id=%s AND bottle_key=%s", (str(box_id), str(bottle_key)))
            for u in usages_list:
                cur.execute("""
                    INSERT INTO qc1reagentusages (box_id, usage, quantity, bottle_key, unit, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s)
                """, (str(box_id), u.get("usage", ""), u.get("quantity", ""), str(bottle_key), str(unit), datetime.now()))
        conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def get_usages_for_code(management_code: str) -> list:
    df = _read_df("""
        SELECT usage AS "Usage", quantity AS "Quantity"
        FROM qc1usagemapping
        WHERE TRIM(management_code) = %s
    """, (str(management_code).strip(),))
    return df.to_dict("records") if not df.empty else []


def get_reagent_data_for_code(management_code: str):
    """Look up (manufacturer, cat_no, item_name) for a Management Code from
    qc1reagentsdata. Returns {"manufacturer": ..., "cat_no": ..., "item_name": ...}
    or None if no match."""
    df = _read_df("""
        SELECT manufacturer AS "Manufacturer", cat_no AS "Cat No", item_name AS "Item Name"
        FROM qc1reagentsdata
        WHERE TRIM(management_code) = %s
    """, (str(management_code).strip(),))
    if df.empty:
        return None
    row = df.iloc[0]
    return {
        "manufacturer": row.get("Manufacturer", "") or "",
        "cat_no": row.get("Cat No", "") or "",
        "item_name": row.get("Item Name", "") or "",
    }


def load_reagentsdata_catalog() -> pd.DataFrame:
    """Management Code / Cat No lookup table from qc1reagentsdata, used to
    recognize a known reagent from a scanned vendor barcode's catalog number."""
    return _read_df("""
        SELECT management_code AS "Management Code", cat_no AS "Cat No"
        FROM qc1reagentsdata
    """)


def load_cell_culture(division: str = None) -> pd.DataFrame:
    return _select_table("qc1cellculture", QC1_CELL_COLS, order_by="cell_id ASC")


def load_cell_usage(division: str = None) -> pd.DataFrame:
    return _select_table("qc1cellusages", QC1_CELL_USAGE_COLS, order_by="usage_id ASC")


def load_standard_quarantine(division: str = None) -> pd.DataFrame:
    df = _select_table("qc1standardsquarantine", QC1_STANDARD_QUARANTINE_COLS, order_by="qid ASC")
    if not df.empty and "QID" in df.columns:
        df["QID"] = df["QID"].astype(str).str.strip()
    return df


def insert_standard_quarantine(
    item_name, management_code="", manufacturer="", catalog_number="",
    lot_number="", storage="", qty_received="", arrival_date=None,
    expiry_date=None, ghs="", assay="", capacity_per_unit="",
    preparation_procedure="", coa_url="", msds_url="", note="", pic="",
    status="Quarantine", usage_tracking_mode="Per Bottle",
    tracking_unit="", low_stock_threshold="", registered_by=""
) -> str:
    _ensure_std_tracking_columns()
    _ensure_std_registered_by_column()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1standardsquarantine (
                    item_name, management_code, manufacturer, catalog_number,
                    lot_number, storage, qty_received, arrival_date, expiry_date,
                    ghs, assay, capacity_per_unit, preparation_procedure,
                    coa_url, msds_url, note, pic, status, timestamp, is_voided,
                    usage_tracking_mode, tracking_unit, low_stock_threshold,
                    registered_by
                ) VALUES (
                    %s,%s,%s,%s, %s,%s,%s,%s,%s,
                    %s,%s,%s,%s, %s,%s,%s,%s,%s,%s,FALSE,
                    %s,%s,%s,
                    %s
                ) RETURNING qid
            """, (
                item_name, management_code, manufacturer, catalog_number,
                lot_number, storage, qty_received, arrival_date, expiry_date,
                ghs, assay, capacity_per_unit, preparation_procedure,
                coa_url, msds_url, note, pic, status,
                datetime.now(),
                usage_tracking_mode, tracking_unit, low_stock_threshold,
                registered_by,
            ))
            new_qid = cur.fetchone()[0]
            conn.commit()
        return str(new_qid)
    except Exception:
        _rollback_quietly(conn)
        raise


def insert_standards_bulk(standards_list: list) -> list:
    """
    Insert multiple standard unit records that all belong to ONE registration
    batch. A single standard_id is generated here (not left to the caller) and
    applied to every row in standards_list, so all units from one registration
    share the same standard_id — matching qc1units' one-reagent-many-units model.

    If a record already carries its own standard_id (e.g. re-using an existing
    batch id for some other workflow), that value is respected instead of being
    overwritten.
    """
    _ensure_std_tracking_columns()
    conn = _get_conn()
    inserted_ids = []

    # Generate ONE standard_id for this whole batch, used as the fallback
    # for any record that doesn't already specify one.
    batch_standard_id = str(int(datetime.now().timestamp() * 1000))

    try:
        with conn.cursor() as cur:
            for std in standards_list:
                std_id = std.get("standard_id") or batch_standard_id
                cur.execute("""
                    INSERT INTO qc1standards (
                        standard_id, item_name, management_code, manufacturer,
                        catalog_number, lot_number, storage, expiry_date,
                        arrival_date, ghs, assay, capacity_per_unit,
                        preparation_procedure, coa_url, msds_url, note, pic,
                        registration_date, serial_number, aliquot_serial, status,
                        opened_date, pao, eao, opened_by, disposed_date,
                        disposed_by, dispose_reason, timestamp, is_voided,
                        usage_tracking_mode, tracking_unit, low_stock_threshold
                    ) VALUES (
                        %s,%s,%s,%s, %s,%s,%s,%s,
                        %s,%s,%s,%s, %s,%s,%s,%s,%s,
                        %s,%s,%s,%s, %s,%s,%s,%s,%s,
                        %s,%s,%s, FALSE,
                        %s,%s,%s
                    )
                """, (
                    std_id, std.get("item_name"), std.get("management_code"),
                    std.get("manufacturer"), std.get("catalog_number"), std.get("lot_number"),
                    std.get("storage"), std.get("expiry_date"), std.get("arrival_date"),
                    std.get("ghs"), std.get("assay"), std.get("capacity_per_unit"),
                    std.get("preparation_procedure"), std.get("coa_url"), std.get("msds_url"),
                    std.get("note"), std.get("pic"), std.get("registration_date"),
                    std.get("serial_number"), std.get("aliquot_serial"), std.get("status"),
                    std.get("opened_date"), std.get("pao"), std.get("eao"), std.get("opened_by"),
                    std.get("disposed_date"), std.get("disposed_by"), std.get("dispose_reason"),
                    datetime.now(),
                    std.get("usage_tracking_mode") or "Per Bottle",
                    std.get("tracking_unit"), std.get("low_stock_threshold"),
                ))
                inserted_ids.append(std_id)
            conn.commit()
        return inserted_ids
    except Exception:
        _rollback_quietly(conn)
        raise


def delete_quarantine(qid):
    """Delete/remove a quarantine record after it's been approved and migrated to Standards"""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM qc1standardsquarantine WHERE qid=%s", (int(qid),))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_standard_opened(record_id, use_date, pao, eao, opened_by):
    """
    Marks ONE unit row as opened/used, identified by its unique `id`
    (primary key) — NOT by standard_id, which is shared across every unit
    in a registration batch (see insert_standards_bulk). The caller must
    pass the specific unit row's `id`, not the batch-level standard_id.
    """
    try:
        row_id = int(record_id) if record_id else None
    except (ValueError, TypeError):
        raise ValueError(f"Invalid id: {record_id}")

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            params = ("used", use_date, pao, eao, opened_by, datetime.now(), row_id)
            cur.execute("""
                UPDATE qc1standards SET
                    status=%s, opened_date=%s, pao=%s, eao=%s, opened_by=%s, timestamp=%s
                WHERE id=%s
            """, params)
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_standard_first_use_meta(standard_id, qty_received, capacity_per_unit,
                                    preparation_procedure, usage_tracking_mode, tracking_unit):
    """
    Registration no longer captures quantity/capacity/prep procedure —
    the FIRST Register Usage call against a given standard_id is where
    these are set. Applied batch-wide (WHERE standard_id=), matching how
    Usage Tracking Mode was already fixed once per whole registration
    batch, just decided later (at first use) instead of at registration.
    Must run BEFORE finalize_standard_first_use_units(), so the sibling
    rows it copies from the placeholder pick up the finalized values.
    """
    _ensure_std_tracking_columns()
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1standards SET qty_received=%s, capacity_per_unit=%s,
                    preparation_procedure=%s, usage_tracking_mode=%s, tracking_unit=%s
                WHERE standard_id=%s
            """, (qty_received, capacity_per_unit, preparation_procedure,
                  usage_tracking_mode, tracking_unit, str(standard_id)))
        conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def finalize_standard_first_use_units(placeholder_unit_id, num_units, management_code=""):
    """
    Converts the 'awaiting_quantity' placeholder row created at approval
    into the real first unit (rename in place via UPDATE on its own `id`
    — status is flipped separately by update_standard_opened, same call
    every other unused->used transition uses). If this mode's count
    determined more than one unit, siblings 2..N are created as fresh
    'unused' rows by copying the placeholder's now-finalized metadata
    (must run AFTER update_standard_first_use_meta).

    Serial format is '{management_code}/N' for every unit (aliquot mode
    passes management_code; regular/cumulative modes label 'Unit N'
    instead) — real physical units either way, just a naming convention.

    Returns [(id, aliquot_serial), ...] for all num_units units in order
    1..N, so the caller can mark however many were actually consumed in
    this same first-use submission as 'used'.
    """
    def _label(i):
        return f"{management_code}/{i}" if management_code else f"Unit {i}"

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE qc1standards SET aliquot_serial=%s WHERE id=%s",
                (_label(1), int(placeholder_unit_id))
            )
            created = [(int(placeholder_unit_id), _label(1))]
            if num_units > 1:
                for i in range(2, num_units + 1):
                    cur.execute("""
                        INSERT INTO qc1standards (
                            standard_id, item_name, management_code, manufacturer, catalog_number,
                            lot_number, storage, expiry_date, arrival_date, ghs, assay,
                            qty_received, capacity_per_unit, preparation_procedure,
                            coa_url, msds_url, note, pic, registration_date, serial_number,
                            aliquot_serial, status, timestamp, is_voided,
                            usage_tracking_mode, tracking_unit, low_stock_threshold
                        )
                        SELECT standard_id, item_name, management_code, manufacturer, catalog_number,
                               lot_number, storage, expiry_date, arrival_date, ghs, assay,
                               qty_received, capacity_per_unit, preparation_procedure,
                               coa_url, msds_url, note, pic, registration_date, serial_number,
                               %s, 'unused', %s, FALSE,
                               usage_tracking_mode, tracking_unit, low_stock_threshold
                        FROM qc1standards WHERE id=%s
                        RETURNING id
                    """, (_label(i), datetime.now(), int(placeholder_unit_id)))
                    created.append((int(cur.fetchone()[0]), _label(i)))
        conn.commit()
        return created
    except Exception:
        _rollback_quietly(conn)
        raise


def update_standard_disposed(record_id, disposal_date, disposed_by, dispose_reason):
    """
    Marks ONE unit row as disposed, identified by its unique `id`
    (primary key) — NOT by standard_id (see note in update_standard_opened).
    """
    try:
        row_id = int(record_id) if record_id else None
    except (ValueError, TypeError):
        raise ValueError(f"Invalid id: {record_id}")

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            params = ("disposed", disposal_date, disposed_by, dispose_reason, datetime.now(), row_id)
            cur.execute("""
                UPDATE qc1standards SET
                    status=%s, disposed_date=%s, disposed_by=%s, dispose_reason=%s, timestamp=%s
                WHERE id=%s
            """, params)
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_standard_usage_aliquot_disposed(aliquot_id):
    """Mark an aliquot usage record as disposed"""
    # aliquot_id is TEXT in qc1standardusages — bind it as str, never int
    # (an integer param makes Postgres fail with "text = integer").
    ali_id = str(aliquot_id).strip()
    if not ali_id:
        # Non-aliquot usage rows store aliquot_id='' — matching empty here
        # would flip every one of them to disposed.
        raise ValueError(f"Invalid aliquot_id: {aliquot_id!r}")

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1standardusages SET
                    aliquot_status=%s, timestamp=%s
                WHERE aliquot_id=%s
            """, ("disposed", datetime.now(), ali_id))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def insert_reagent_aliquot(
    division, reagent_id, unit_label, management_code="", item_name="",
    lot_number="", aliquot_serial="", aliquot_id="", aliquot_name="",
    aliquot_status="active", opened_date=None, pao="", eao="",
    opened_by="", note="", conn=None
) -> str:
    """
    Insert one aliquot row for a reagent unit that's being split on open —
    the reagent-side equivalent of insert_standard_usage()'s aliquot rows.
    `division` is accepted for call-signature compatibility (qc1reagentaliquots
    has no division column). If a caller passes its own `conn` (batching
    several aliquot rows into one atomic transaction, like Register Usage
    does for Standards), reuse it and let the caller commit/rollback.
    """
    _ensure_reagent_aliquot_table()
    _external_conn = conn is not None
    conn = conn if _external_conn else _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qc1reagentaliquots (
                    reagent_id, unit_label, management_code, item_name,
                    lot_number, aliquot_serial, aliquot_id, aliquot_name,
                    aliquot_status, opened_date, pao, eao, opened_by, note,
                    timestamp, created_at
                ) VALUES (
                    %s,%s,%s,%s, %s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s
                ) RETURNING aliquot_usage_id
            """, (
                str(reagent_id), unit_label, management_code, item_name,
                lot_number, aliquot_serial, aliquot_id, aliquot_name,
                aliquot_status, opened_date, pao, eao, opened_by, note,
                datetime.now(), datetime.now()
            ))
            new_id = cur.fetchone()[0]
            if not _external_conn:
                conn.commit()
        return str(new_id)
    except Exception:
        if not _external_conn:
            _rollback_quietly(conn)
        raise


def load_reagent_aliquots(division: str = None) -> pd.DataFrame:
    _ensure_reagent_aliquot_table()
    return _select_table("qc1reagentaliquots", QC1_REAGENT_ALIQUOT_COLS, order_by="aliquot_usage_id ASC")


def update_reagent_aliquot_disposed(aliquot_id, disposal_date=None, disposed_by="", dispose_reason=""):
    """Mark a reagent aliquot as disposed, recording it inline on the row
    (see _ensure_reagent_aliquot_table note on why there's no separate log
    table here, unlike Standards' update_standard_usage_aliquot_disposed)."""
    ali_id = str(aliquot_id).strip()
    if not ali_id:
        # Non-aliquot rows never exist here, but guard the same way
        # update_standard_usage_aliquot_disposed does — an empty match
        # would flip every row with a blank aliquot_id at once.
        raise ValueError(f"Invalid aliquot_id: {aliquot_id!r}")

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1reagentaliquots SET
                    aliquot_status=%s, disposed_date=%s, disposed_by=%s,
                    dispose_reason=%s, timestamp=%s
                WHERE aliquot_id=%s
            """, ("disposed", disposal_date, disposed_by, dispose_reason, datetime.now(), ali_id))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_quarantine_status(qid, status, approved_by="", approved_date=None):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1standardsquarantine SET
                    status=%s, approved_by=%s, approved_date=%s, timestamp=%s
                WHERE qid=%s
            """, (status, approved_by, approved_date, datetime.now(), int(qid)))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def update_quarantine_record(qid, update_dict):
    """Update multiple fields of a quarantine record at once"""
    if not update_dict:
        return

    # Map sheet column names to database column names
    col_map = {
        "Item Name": "item_name",
        "Management Code": "management_code",
        "Manufacturer": "manufacturer",
        "Catalog Number": "catalog_number",
        "Lot Number": "lot_number",
        "Storage": "storage",
        "Qty Received": "qty_received",
        "Arrival Date": "arrival_date",
        "Expiry Date": "expiry_date",
        "GHS": "ghs",
        "Assay": "assay",
        "Capacity Per Unit": "capacity_per_unit",
        "Preparation Procedure": "preparation_procedure",
        "CoA URL": "coa_url",
        "MSDS URL": "msds_url",
        "Note": "note",
        "PIC": "pic",
        "Status": "status",
        "Approved By": "approved_by",
        "Approved Date": "approved_date",
    }

    set_clauses = []
    values = []
    for sheet_col, val in update_dict.items():
        db_col = col_map.get(sheet_col)
        if db_col:
            set_clauses.append(f"{db_col}=%s")
            values.append(val)

    if not set_clauses:
        return

    values.append(datetime.now())
    values.append(int(qid))

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE qc1standardsquarantine SET
                    {', '.join(set_clauses)}, timestamp=%s
                WHERE qid=%s
            """, values)
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def void_quarantine(qid, void_reason, voided_by, void_event_id="", superseded_by=""):
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE qc1standardsquarantine SET
                    is_voided=TRUE, void_reason=%s, voided_by=%s,
                    void_timestamp=%s, void_event_id=%s, superseded_by=%s
                WHERE qid=%s
            """, (void_reason, voided_by, datetime.now(),
                  void_event_id, superseded_by, int(qid)))
            conn.commit()
    except Exception:
        _rollback_quietly(conn)
        raise


def load_standards(division: str = None) -> pd.DataFrame:
    df = _select_table("qc1standards", QC1_STANDARD_COLS, order_by="standard_id ASC")
    if not df.empty:
        for col in ("ID", "Standard ID"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
    return df


def load_standard_usage(division: str = None) -> pd.DataFrame:
    return _select_table("qc1standardusages", QC1_STANDARD_USAGE_COLS, order_by="usage_id ASC")


def load_standard_disposal(division: str = None) -> pd.DataFrame:
    return _select_table("qc1standarddisposal", QC1_STANDARD_DISPOSAL_COLS, order_by="disposal_id ASC")


def load_storage_map(division: str = None) -> pd.DataFrame:
    return _select_table("qc1storagemap", QC1_STORAGE_MAP_COLS)


def load_storage_slots(division: str = None) -> pd.DataFrame:
    return _select_table("qc1storageslot", QC1_STORAGE_SLOT_COLS)


def load_stock_opname(division: str = None) -> pd.DataFrame:
    return _select_table("qc1stockopname", QC1_STOCK_OPNAME_COLS, "timestamp DESC")


# ── Stock Opname discrepancy resolution ─────────────────────────────────────
# resolution_status/resolved_via/resolved_record_id/resolved_by/resolved_at
# live on qc1stockopname (added via ALTER TABLE, not in QC1_STOCK_OPNAME_COLS)
# and are read/written with direct SQL here rather than through the
# gspread-compat shim — the shim does a full DELETE+re-INSERT of the whole
# table on every save (see _replace_table_from_df), and qc1stockopname's real
# `id` PK isn't exposed in QC1_STOCK_OPNAME_COLS at all, so (so_id,
# management_code) — unique per reagent per SO — is used as the stable key
# instead of relying on id values that get reassigned on each full rewrite.
def open_stockopname_discrepancies(so_id: str, division: str = None):
    """Mark every nonzero-diff row for this SO as an open discrepancy.
    Called right after a Stock Opname is saved; never blocks the save."""
    _execute("""
        UPDATE qc1stockopname
        SET resolution_status = 'open'
        WHERE so_id = %s
          AND (COALESCE(unopened_diff, 0) <> 0 OR COALESCE(opened_diff, 0) <> 0)
          AND resolution_status IS NULL
    """, (so_id,))


def load_open_stockopname_discrepancies(division: str = None) -> pd.DataFrame:
    """Flat worklist across ALL sessions of currently-open discrepancies."""
    return _read_df("""
        SELECT
            so_id AS "SO ID",
            opname_date AS "SO Date",
            timestamp AS "Timestamp",
            management_code AS "Management Code",
            reagent_name AS "Reagent Name",
            reagent_id AS "Reagent ID",
            COALESCE(unopened_diff, 0) AS "Unopened Diff",
            COALESCE(opened_diff, 0) AS "Opened Diff",
            (COALESCE(unopened_diff, 0) + COALESCE(opened_diff, 0)) AS "Diff",
            pic AS "PIC"
        FROM qc1stockopname
        WHERE resolution_status = 'open'
        ORDER BY timestamp ASC
    """)


def load_stockopname_resolution_map(division: str = None) -> dict:
    """(so_id, management_code) -> resolution info for every row that has
    ever had a discrepancy (open, pending approval, resolved, or rejected-
    and-back-to-open) — used to show inline resolution status in SO History."""
    df = _read_df("""
        SELECT so_id AS "SO ID", management_code AS "Management Code",
               resolution_status AS "Resolution Status",
               resolved_via AS "Resolved Via", resolved_at AS "Resolved At",
               resolved_by AS "Resolved By",
               pending_kind AS "Pending Kind", submitted_by AS "Submitted By",
               submitted_at AS "Submitted At",
               rejection_reason AS "Rejection Reason", rejected_by AS "Rejected By"
        FROM qc1stockopname
        WHERE resolution_status IS NOT NULL
    """)
    if df.empty:
        return {}
    out = {}
    for _, r in df.iterrows():
        out[(str(r["SO ID"]), str(r["Management Code"]))] = {
            "status": r["Resolution Status"],
            "resolved_via": r["Resolved Via"],
            "resolved_at": r["Resolved At"],
            "resolved_by": r["Resolved By"],
            "pending_kind": r["Pending Kind"],
            "submitted_by": r["Submitted By"],
            "submitted_at": r["Submitted At"],
            "rejection_reason": r["Rejection Reason"],
            "rejected_by": r["Rejected By"],
        }
    return out


def submit_pending_stockopname_resolution(so_id: str, management_code: str,
                                           kind: str, data: dict, submitted_by: str,
                                           division: str = None):
    """Record a proposed discrepancy resolution WITHOUT executing it — the
    underlying Usage/Disposal/Registration/Loss write only happens later, at
    approve_stockopname_resolution(). Clears any stale rejection info from a
    prior round so it doesn't linger once resubmitted. Only fires from an
    'open' row so a stale/duplicate submit can't clobber one already pending
    or resolved."""
    _execute("""
        UPDATE qc1stockopname
        SET resolution_status = 'pending_approval',
            pending_kind = %s, pending_data = %s,
            submitted_by = %s, submitted_at = %s,
            rejection_reason = NULL, rejected_by = NULL, rejected_at = NULL
        WHERE so_id = %s AND management_code = %s AND resolution_status = 'open'
    """, (kind, json.dumps(data), submitted_by, datetime.now(), so_id, management_code))


def load_pending_approval_stockopname_discrepancies(division: str = None) -> pd.DataFrame:
    """Flat worklist across ALL sessions of discrepancies awaiting supervisor
    approval — the underlying inventory record has NOT been written yet."""
    return _read_df("""
        SELECT
            so_id AS "SO ID",
            opname_date AS "SO Date",
            timestamp AS "Timestamp",
            management_code AS "Management Code",
            reagent_name AS "Reagent Name",
            reagent_id AS "Reagent ID",
            COALESCE(unopened_diff, 0) AS "Unopened Diff",
            COALESCE(opened_diff, 0) AS "Opened Diff",
            (COALESCE(unopened_diff, 0) + COALESCE(opened_diff, 0)) AS "Diff",
            pic AS "PIC",
            pending_kind AS "Pending Kind",
            pending_data AS "Pending Data",
            submitted_by AS "Submitted By",
            submitted_at AS "Submitted At"
        FROM qc1stockopname
        WHERE resolution_status = 'pending_approval'
        ORDER BY submitted_at ASC
    """)


def approve_stockopname_resolution(so_id: str, management_code: str,
                                    resolved_via: str, resolved_record_id: str,
                                    approved_by: str, division: str = None):
    """Execute-on-approval step: marks the discrepancy resolved once the
    caller has actually written the underlying Usage/Disposal/Registration/
    Loss record (replaying pending_data). Only fires from a 'pending_approval'
    row — a regular submit can never mark itself resolved."""
    _execute("""
        UPDATE qc1stockopname
        SET resolution_status = 'resolved', resolved_via = %s,
            resolved_record_id = %s, resolved_by = %s, resolved_at = %s
        WHERE so_id = %s AND management_code = %s AND resolution_status = 'pending_approval'
    """, (resolved_via, str(resolved_record_id or ""), approved_by, datetime.now(),
          so_id, management_code))


def reject_stockopname_resolution(so_id: str, management_code: str,
                                   reason: str, rejected_by: str, division: str = None):
    """Sends the discrepancy back to 'open' — nothing was ever written to
    inventory — with the rejection reason kept visible until the next
    submission overwrites it. Only fires from a 'pending_approval' row."""
    _execute("""
        UPDATE qc1stockopname
        SET resolution_status = 'open',
            pending_kind = NULL, pending_data = NULL,
            rejection_reason = %s, rejected_by = %s, rejected_at = %s
        WHERE so_id = %s AND management_code = %s AND resolution_status = 'pending_approval'
    """, (reason, rejected_by, datetime.now(), so_id, management_code))


def count_open_stockopname_discrepancies(division: str = None) -> int:
    """Both 'open' (needs a submitter) and 'pending_approval' (needs a
    supervisor) count as needing attention for the Dashboard alert card."""
    df = _read_df("""
        SELECT count(*) AS c FROM qc1stockopname
        WHERE resolution_status IN ('open', 'pending_approval')
    """)
    return int(df["c"].iloc[0]) if not df.empty else 0

# ── QC2 Stock Opname discrepancy resolution ─────────────────────────────────
# Mirrors the qc1stockopname functions above, but keyed by
# (so_id, category, management_code) since QC2 spans 4 categories (Media,
# Reagent, BI, Standard Strain) instead of QC1's single Reagent-only model.
# so_id/resolution_status/resolved_via/resolved_record_id/resolved_by/
# resolved_at live on qc2_master_so (added via ALTER TABLE, out-of-band from
# QC2_COLUMN_MAP) for the same reason QC1's equivalents are out-of-band:
# save_qc2_table() does a full DELETE+re-INSERT of the whole table on every
# "New SO" save, and would silently drop any column not in the map.
def next_qc2_so_id() -> str:
    """Sequential SO batch id (QC2-SO-001, QC2-SO-002, ...) — mirrors QC1's
    SO-### numbering with a distinct prefix so the two divisions' ids are
    never confused."""
    df = _read_df("SELECT DISTINCT so_id FROM qc2_master_so WHERE so_id IS NOT NULL")
    nums = []
    for so in (df["so_id"] if not df.empty else []):
        m = re.match(r"QC2-SO-(\d+)", str(so))
        if m:
            nums.append(int(m.group(1)))
    return f"QC2-SO-{(max(nums) + 1) if nums else 1:03d}"


def insert_qc2_stock_opname_rows(so_id: str, rows: list):
    """Direct multi-row INSERT into qc2_master_so — bypasses save_qc2_table()'s
    full-table rewrite so existing rows' so_id/resolution_status survive every
    subsequent New SO save. `rows` are plain dicts with keys: so_date, pic,
    category, management_code, item_name, system_stock, actual_stock,
    difference, status, remark, timestamp."""
    for r in rows:
        _execute("""
            INSERT INTO qc2_master_so
                (so_id, timestamp, so_date, pic, category, management_code,
                 item_name, system_stock, actual_stock, difference, status, remark)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (so_id, r["timestamp"], r["so_date"], r["pic"], r["category"],
              r["management_code"], r["item_name"], str(r["system_stock"]),
              str(r["actual_stock"]), str(r["difference"]), r["status"], r["remark"]))


def open_qc2_stockopname_discrepancies(so_id: str):
    """Mark every nonzero-diff row for this SO as an open discrepancy.
    Called right after a Stock Opname is saved; never blocks the save."""
    _execute("""
        UPDATE qc2_master_so
        SET resolution_status = 'open'
        WHERE so_id = %s AND status <> 'Balance' AND resolution_status IS NULL
    """, (so_id,))


def load_open_qc2_stockopname_discrepancies() -> pd.DataFrame:
    """Flat worklist across ALL sessions of currently-open discrepancies."""
    return _read_df("""
        SELECT so_id AS "SO ID", so_date AS "SO Date", timestamp AS "Timestamp",
               category AS "Category", management_code AS "Management Code",
               item_name AS "Item Name", difference AS "Diff", pic AS "PIC"
        FROM qc2_master_so
        WHERE resolution_status = 'open'
        ORDER BY timestamp ASC
    """)


def load_qc2_stockopname_resolution_map() -> dict:
    """(so_id, category, management_code) -> {status, resolved_via, resolved_at}
    for every row that has ever had a discrepancy — used to show inline
    resolution status in SO History."""
    df = _read_df("""
        SELECT so_id AS "SO ID", category AS "Category",
               management_code AS "Management Code",
               resolution_status AS "Resolution Status",
               resolved_via AS "Resolved Via", resolved_at AS "Resolved At"
        FROM qc2_master_so WHERE resolution_status IS NOT NULL
    """)
    if df.empty:
        return {}
    out = {}
    for _, r in df.iterrows():
        out[(str(r["SO ID"]), str(r["Category"]), str(r["Management Code"]))] = {
            "status": r["Resolution Status"],
            "resolved_via": r["Resolved Via"],
            "resolved_at": r["Resolved At"],
        }
    return out


def resolve_qc2_stockopname_discrepancy(so_id: str, category: str, management_code: str,
                                         resolved_via: str, resolved_record_id: str,
                                         resolved_by: str):
    _execute("""
        UPDATE qc2_master_so
        SET resolution_status = 'resolved', resolved_via = %s,
            resolved_record_id = %s, resolved_by = %s, resolved_at = %s
        WHERE so_id = %s AND category = %s AND management_code = %s
          AND resolution_status = 'open'
    """, (resolved_via, str(resolved_record_id or ""), resolved_by, datetime.now(),
          so_id, category, management_code))


def count_open_qc2_stockopname_discrepancies() -> int:
    df = _read_df("SELECT count(*) AS c FROM qc2_master_so WHERE resolution_status = 'open'")
    return int(df["c"].iloc[0]) if not df.empty else 0


def load_low_stock_points(division: str = None) -> dict:
    df = _select_table("qc1lowstockpoints", QC1_LOW_STOCK_COLS)
    if df.empty:
        return {}
    return {
        str(r["Management Code"]).strip(): {
            "warning": int(r.get("Warning Threshold") or 2),
            "critical": int(r.get("Critical Threshold") or 1),
        }
        for _, r in df.iterrows()
        if str(r.get("Management Code", "")).strip()
    }


def upsert_low_stock_point(division, management_code, warning, critical):
    _execute("""
        INSERT INTO qc1lowstockpoints (management_code, warning_threshold, critical_threshold)
        VALUES (%s,%s,%s)
        ON CONFLICT (management_code) DO UPDATE SET
            warning_threshold  = EXCLUDED.warning_threshold,
            critical_threshold = EXCLUDED.critical_threshold
    """, (str(management_code).strip(), int(warning), int(critical)))


def load_purchases(division: str = None) -> pd.DataFrame:
    df = _select_table("qc1purchases", QC1_PURCHASE_COLS)
    if not df.empty:
        df["ID"] = df["ID"].astype(str).str.strip()
        if "Receipt History" in df.columns:
            df["Receipt History"] = df["Receipt History"].fillna("[]").astype(str)
    return df


def insert_purchase(
    division, management_code, reagent_name, manufacturer="",
    catalog_number="", quantity=1, pre_order_date=None,
    estimated_arrival=None, pic="", notes="", purchase_type="",
    mdvan_date=None, status="Ordered"
) -> str:
    return _execute_returning_id("""
        INSERT INTO qc1purchases (
            management_code, reagent_name, manufacturer, catalog_number, quantity,
            pre_order_date, estimated_arrival, status, pic, notes, receipt_history,
            purchase_type, mdvan_date, created_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'[]',%s,%s,%s)
        RETURNING id
    """, (
        management_code, reagent_name, manufacturer, catalog_number, quantity,
        pre_order_date, estimated_arrival, status, pic, notes, purchase_type,
        mdvan_date, datetime.now(),
    ))


def load_active_purchase_codes(division: str = None) -> set:
    df = _read_df("""
        SELECT DISTINCT management_code FROM qc1purchases
        WHERE status IN ('Ordered','Shipped','Partially Received')
    """)
    if df.empty:
        return set()
    return set(df["management_code"].astype(str).str.strip().unique())


# ══════════════════════════════════════════════════════════════════════════════
# QC2 — TEMPORARY ROLE DELEGATION
# ══════════════════════════════════════════════════════════════════════════════
# Dedicated CRUD (not routed through the generic load_qc2_table/save_qc2_table
# path) because that generic path drops id/created_at and does a full
# DELETE+re-INSERT of the whole table on every save — unsafe here since Edit/
# Revoke need to address a single row, and concurrent create/revoke by two
# admins must not clobber each other.

def load_role_delegations(division: str = "QC2") -> pd.DataFrame:
    """All Temporary Role Delegation records for a division, newest first."""
    return _read_df(
        "SELECT * FROM qc2_role_delegations WHERE division=%s ORDER BY created_at DESC",
        (division,)
    )


def insert_role_delegation(
    division, owner_username, owner_role_snapshot, delegate_username,
    start_date, end_date, reason, approved_by
) -> str:
    return _execute_returning_id("""
        INSERT INTO qc2_role_delegations (
            division, owner_username, owner_role_snapshot, delegate_username,
            start_date, end_date, reason, approved_by, created_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """, (
        division, owner_username, owner_role_snapshot, delegate_username,
        start_date, end_date, reason, approved_by, datetime.now(),
    ))


def update_role_delegation(delegation_id, start_date, end_date, reason):
    """Only the mutable fields — identity fields (owner/delegate/role snapshot)
    are never edited; revoke and recreate instead."""
    _execute("""
        UPDATE qc2_role_delegations
        SET start_date=%s, end_date=%s, reason=%s, updated_at=%s
        WHERE id=%s
    """, (start_date, end_date, reason, datetime.now(), delegation_id))


def revoke_role_delegation(delegation_id, revoked_by):
    _execute("""
        UPDATE qc2_role_delegations
        SET is_revoked=TRUE, revoked_by=%s, revoked_at=%s
        WHERE id=%s
    """, (revoked_by, datetime.now(), delegation_id))


def get_active_delegation_for_delegate(username: str, division: str = "QC2"):
    """
    The delegation record (as a dict) currently granting `username` delegated
    approval authority right now, or None. Includes the owner's full name
    (for the "Acting as Delegate for ..." badge) via one joined query.
    """
    result = _run("""
        SELECT d.*, u.full_name AS owner_full_name
        FROM qc2_role_delegations d
        LEFT JOIN users u ON u.username = d.owner_username
        WHERE d.delegate_username=%s AND d.division=%s AND d.is_revoked=FALSE
          AND d.start_date <= CURRENT_DATE AND d.end_date >= CURRENT_DATE
        ORDER BY d.created_at DESC
        LIMIT 1
    """, (username, division), fetch="one")
    return result or None


def get_delegations_for_owner(owner_username: str, division: str = "QC2") -> pd.DataFrame:
    """Non-revoked delegation rows for one owner — used for overlap validation."""
    return _read_df("""
        SELECT * FROM qc2_role_delegations
        WHERE owner_username=%s AND division=%s AND is_revoked=FALSE
        ORDER BY start_date ASC
    """, (owner_username, division))


def log_audit(division: str, username: str, role: str, action: str, category: str, detail: str,
              acting_for_username: str = None, delegation_status: str = None, module: str = None):
    _execute("""
        INSERT INTO qc1audittrail (
            division, username, role, action, category, detail, timestamp,
            acting_for_username, delegation_status, module
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (division, username, role, action, category, detail, datetime.now(),
          acting_for_username, delegation_status, module))


def load_qc2_table(sheet_name: str) -> pd.DataFrame:
    sql_table = QC2_TABLE_MAP.get(sheet_name)
    if not sql_table:
        return pd.DataFrame()
    if sql_table == "email_settings":
        return _read_df("SELECT setting_name AS \"Setting_Name\", setting_value AS \"Setting_Value\" FROM emailsettings")
    if sql_table == "emailrecipients":
        return _read_df("""
            SELECT email AS "Email", categories AS "Categories", active AS "Active"
            FROM emailrecipients
            WHERE division = %s
            ORDER BY email ASC
        """, ("QC2",))
    if sql_table == "audit_trail":
        return _select_table("qc1audittrail", AUDIT_COLS, order_by="id ASC", division="QC2")
    col_map = QC2_COLUMN_MAP.get(sql_table, {})
    # Not every qc2_* table has an "id" column (e.g. qc2_reagent has none) —
    # check the live schema first instead of assuming one exists.
    order_clause = " ORDER BY id ASC" if "id" in _get_table_columns(sql_table) else ""
    df = _read_df(f"SELECT * FROM {sql_table}{order_clause}")
    for col in ["id", "created_at"]:
        if col in df.columns:
            df = df.drop(columns=[col])
    if col_map:
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    return df


def save_qc2_table(sheet_name: str, df: pd.DataFrame):
    sql_table = QC2_TABLE_MAP.get(sheet_name)
    if not sql_table:
        return
    if sql_table == "qc2_culture_inbound":
        _ensure_qc2_culture_function_column()
    if sql_table == "email_settings":
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM emailsettings")
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        cur.execute(
                            "INSERT INTO emailsettings (setting_name, setting_value, updated_at) VALUES (%s,%s,%s)",
                            (row.get("Setting_Name"), row.get("Setting_Value"), datetime.now()),
                        )
            conn.commit()
        except Exception:
            _rollback_quietly(conn)
            raise
        return
    if sql_table == "emailrecipients":
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM emailrecipients WHERE division=%s", ("QC2",))
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO emailrecipients (division, email, categories, active, created_at)
                            VALUES (%s,%s,%s,%s,%s)
                        """, (
                            "QC2",
                            row.get("Email"),
                            row.get("Categories"),
                            str(row.get("Active", "TRUE")).strip().upper() in {"TRUE", "YES", "1"},
                            datetime.now(),
                        ))
            conn.commit()
        except Exception:
            _rollback_quietly(conn)
            raise
        return
    if sql_table == "audit_trail":
        _replace_table_from_df("qc1audittrail", df, AUDIT_COLS, preserve_id=True)
        return
    col_map = QC2_COLUMN_MAP.get(sql_table, {})
    reverse_map = {v: k for k, v in col_map.items()}
    sheet_to_sql = {sheet_col: db_col for sheet_col, db_col in reverse_map.items()}
    if not sheet_to_sql and df is not None:
        sheet_to_sql = {c: c for c in df.columns if c not in {"id", "created_at"}}
    _replace_table_from_df(sql_table, df, sheet_to_sql, preserve_id=False)


# ══════════════════════════════════════════════════════════════════════════════
# QC2 — VOID-BY-ROW (Media / BI / Standard Strain inbound tables)
# ══════════════════════════════════════════════════════════════════════════════
# load_qc2_table()/save_qc2_table() strip the SQL "id" column and round-trip
# the whole table, which is why the old Void expanders could only match by
# Management Code (every arrival sharing a code got voided together). These
# two helpers read/write a single row by its real primary key instead — used
# only by the Media/BI/Standard Strain "Void Entry" expanders in appqc2.py.
# QC2_Reagent's own void flow already targets one row via a different path
# and is untouched by this.

def get_qc2_inbound_rows(sheet_name: str, management_code: str) -> pd.DataFrame:
    """Rows (including the row's SQL id) sharing one Management Code, so the
    Void expander can let the user disambiguate multiple arrivals."""
    sql_table = QC2_TABLE_MAP.get(sheet_name)
    if not sql_table:
        return pd.DataFrame()
    col_map = QC2_COLUMN_MAP.get(sql_table, {})
    reverse_map = {v: k for k, v in col_map.items()}
    mgmt_col = reverse_map.get("Management Code") or reverse_map.get("Management_Code")
    if not mgmt_col:
        return pd.DataFrame()
    df = _read_df(
        f"SELECT * FROM {sql_table} WHERE {mgmt_col} = %s ORDER BY id ASC",
        (management_code,)
    )
    if col_map:
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    return df


def void_qc2_inbound_row(sheet_name: str, row_id, reason: str, by: str) -> bool:
    """Marks exactly ONE inbound row as Void by its SQL primary key —
    sibling rows sharing the same Management Code are left untouched."""
    sql_table = QC2_TABLE_MAP.get(sheet_name)
    if not sql_table:
        return False
    _execute(
        f"UPDATE {sql_table} SET status=%s, void_reason=%s, void_by=%s, void_date=%s WHERE id=%s",
        ("Void", reason, by, datetime.now().strftime('%d %b %Y %H:%M').upper(), row_id)
    )
    return True


def dispose_qc2_inbound_row(sheet_name: str, row_id) -> bool:
    """Marks exactly ONE inbound row as Disposed by its SQL primary key —
    sibling rows sharing the same Management Code are left untouched.
    Reuses the existing 'status' column (the same one Void already uses),
    so no schema change is needed; full disposal details (reason/PIC/
    timestamp) are recorded in the Usage ledger instead."""
    sql_table = QC2_TABLE_MAP.get(sheet_name)
    if not sql_table:
        return False
    _execute(f"UPDATE {sql_table} SET status=%s WHERE id=%s", ("Disposed", row_id))
    return True


def load_sheet_table(sheet_name: str) -> pd.DataFrame:
    name = _normalize_sheet_name(sheet_name)
    if name == "Email_Settings":
        return _read_df("SELECT setting_name AS \"Setting_Name\", setting_value AS \"Setting_Value\" FROM emailsettings")
    if name == "GMP_Corrections":
        return load_gmp_corrections()
    if name in QC2_TABLE_MAP:
        return load_qc2_table(name)
    if name in SHEET_TABLES:
        table, mapping = SHEET_TABLES[name]
        return _select_table(table, mapping)
    return pd.DataFrame()


def save_sheet_table(sheet_name: str, df: pd.DataFrame):
    name = _normalize_sheet_name(sheet_name)
    if name == "Email_Settings":
        save_qc2_table(name, df)
        return True
    if name in QC2_TABLE_MAP:
        save_qc2_table(name, df)
        return True
    if name in SHEET_TABLES:
        table, mapping = SHEET_TABLES[name]
        _replace_table_from_df(table, df, mapping, preserve_id=True)
        return True
    return False


def _sheet_mapping_for(name: str):
    name = _normalize_sheet_name(name)
    if name in QC2_TABLE_MAP:
        table = QC2_TABLE_MAP[name]
        col_map = QC2_COLUMN_MAP.get(table, {})
        return table, {sheet_col: db_col for db_col, sheet_col in col_map.items()}
    if name in SHEET_TABLES:
        return SHEET_TABLES[name]
    return None, {}


def append_sheet_row(sheet_name: str, row_values):
    """
    Append one row using the same logical sheet names/column order used by appqc2.py.
    This keeps append-style workflows out of appqc2.py while preserving its behavior.
    """
    name = _normalize_sheet_name(sheet_name)
    if name in {"QC2_Audit_Log", "AuditTrail"}:
        values = list(row_values) if not isinstance(row_values, dict) else []
        if isinstance(row_values, dict):
            username = row_values.get("User_ID") or row_values.get("Username") or row_values.get("User")
            role = row_values.get("User_Role") or row_values.get("Role")
            action = row_values.get("Aksi") or row_values.get("Action")
            category = row_values.get("Kategori") or row_values.get("Category")
            detail = row_values.get("Detail")
        else:
            username = values[0] if len(values) > 0 else ""
            role = values[2] if len(values) > 2 else ""
            action = values[3] if len(values) > 3 else ""
            category = values[4] if len(values) > 4 else ""
            detail = values[5] if len(values) > 5 else ""
        log_audit("QC2", username, role, action, category, detail)
        return True

    if name == "GMP_Corrections":
        values = list(row_values) if not isinstance(row_values, dict) else []
        if isinstance(row_values, dict):
            insert_gmp_correction(
                row_values.get("Sheet Name", ""),
                row_values.get("Record ID", ""),
                row_values.get("Field Name", ""),
                row_values.get("Old Value", ""),
                row_values.get("New Value", ""),
                row_values.get("Reason", ""),
                row_values.get("Corrected By", ""),
                row_values.get("Record Label", ""),
            )
        else:
            insert_gmp_correction(
                values[2] if len(values) > 2 else "",
                values[3] if len(values) > 3 else "",
                values[4] if len(values) > 4 else "",
                values[5] if len(values) > 5 else "",
                values[6] if len(values) > 6 else "",
                values[7] if len(values) > 7 else "",
                values[9] if len(values) > 9 else "",
                values[11] if len(values) > 11 else "",
            )
        return True

    existing = load_sheet_table(name)

    if name == "QC2_Audit_Log":
        columns = ["User_ID", "User_Name", "User_Role", "Aksi", "Kategori", "Detail", "Timestamp"]
    elif name == "GMP_Corrections":
        columns = [
            "ID", "Division", "Sheet Name", "Record ID", "Field Name",
            "Old Value", "New Value", "Reason", "Status", "Corrected By",
            "Corrected At", "Record Label", "Created At",
        ]
    elif not existing.empty:
        columns = list(existing.columns)
    else:
        _, mapping = _sheet_mapping_for(name)
        columns = list(mapping.keys())

    if isinstance(row_values, dict):
        new_row = row_values
    else:
        new_row = {col: row_values[i] if i < len(row_values) else "" for i, col in enumerate(columns)}

    df_new = pd.concat([existing, pd.DataFrame([new_row])], ignore_index=True)
    return save_sheet_table(name, df_new)


def update_sheet_record(sheet_name: str, id_column: str, id_value, updates: dict):
    """
    Update matching logical sheet rows through the table bridge.
    Unknown update columns are ignored, matching the previous best-effort correction flow.
    """
    name = _normalize_sheet_name(sheet_name)
    df = load_sheet_table(name)
    if df.empty or id_column not in df.columns:
        return False
    mask = df[id_column].astype(str).str.strip() == str(id_value).strip()
    if not mask.any():
        return False
    for col, value in updates.items():
        if col in df.columns:
            df.loc[mask, col] = value
    save_sheet_table(name, df)
    return True


# Bucket is private. Only the object path is stored in the database; a fresh
# signed URL is generated on demand (see get_storage_signed_url) every time a
# document needs to be opened, so nothing expiring is ever persisted.
DEFAULT_SIGNED_URL_TTL_SECONDS = 3600  # 1 hour


def _storage_config():
    cfg = st.secrets.get("connections", {}).get("supabase", {})
    api_url = str(cfg.get("api_url", "")).rstrip("/")
    key = cfg.get("service_role_key") or cfg.get("anon_key") or cfg.get("api_key")
    bucket = cfg.get("storage_bucket", "qc2-documents")
    if not api_url or not key:
        raise RuntimeError(
            "Supabase Storage is not configured. Add api_url and anon_key or service_role_key under "
            "[connections.supabase] in .streamlit/secrets.toml."
        )
    return api_url, key, bucket


def _safe_storage_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9._/-]+", "-", str(value or "").strip())
    safe = re.sub(r"-+", "-", safe).strip("-/.")
    return safe or "file"


def upload_file_to_storage(uploaded_file, folder: str = "qc2", document_type: str = "document") -> str:
    """
    Upload a Streamlit UploadedFile to Supabase Storage (private bucket) and
    return the bucket-relative object path. Store this path as-is; call
    get_storage_signed_url(path) whenever the file actually needs to be opened.
    The bucket defaults to `qc2-documents`; override with `storage_bucket` in secrets.
    """
    api_url, key, bucket = _storage_config()
    original_name = getattr(uploaded_file, "name", "upload.bin")
    content_type = getattr(uploaded_file, "type", None) or mimetypes.guess_type(original_name)[0] or "application/octet-stream"
    file_bytes = uploaded_file.getvalue()
    path = PurePosixPath(
        _safe_storage_name(folder),
        _safe_storage_name(document_type),
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:10]}-{_safe_storage_name(original_name)}",
    ).as_posix()

    encoded_path = "/".join(urllib.parse.quote(part, safe="") for part in path.split("/"))
    upload_url = f"{api_url}/storage/v1/object/{bucket}/{encoded_path}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": content_type,
        "x-upsert": "true",
    }
    resp = requests.post(upload_url, headers=headers, data=file_bytes, timeout=60)
    if resp.status_code >= 400:
        raise RuntimeError(f"Storage upload failed ({resp.status_code}): {resp.text[:300]}")
    return path


@st.cache_data(ttl=1800, show_spinner=False)
def get_storage_signed_url(value: str, expires_in: int = DEFAULT_SIGNED_URL_TTL_SECONDS) -> str:
    """
    Turn a stored Storage object path into a fresh, temporary signed URL the
    browser can open directly. Call this right before displaying/opening a
    document — never store the result. Returns "-" for blank/missing values,
    and passes already-stored full URLs through unchanged (safety net for any
    legacy data saved before paths-only storage was introduced).
    """
    value = str(value or "").strip()
    if value in ("", "-", "nan", "None"):
        return "-"
    if value.startswith("http"):
        return value

    api_url, key, bucket = _storage_config()
    encoded_path = "/".join(urllib.parse.quote(part, safe="") for part in value.split("/"))
    sign_url = f"{api_url}/storage/v1/object/sign/{bucket}/{encoded_path}"
    try:
        sign_resp = requests.post(
            sign_url,
            headers={"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"expiresIn": expires_in},
            timeout=30,
        )
        if sign_resp.status_code >= 400:
            return "-"
        signed_path = sign_resp.json().get("signedURL", "")
        if not signed_path:
            return "-"
        return f"{api_url}/storage/v1{signed_path}"
    except Exception:
        return "-"


def get_storage_file_bytes(file_url: str):
    """Download bytes for a Supabase Storage URL/path."""
    if not file_url or str(file_url).strip() in {"-", ""}:
        return None
    api_url, key, bucket = _storage_config()
    value = str(file_url).strip()
    if value.startswith("http"):
        url = value
    else:
        encoded_path = "/".join(urllib.parse.quote(part, safe="") for part in value.split("/"))
        url = f"{api_url}/storage/v1/object/{bucket}/{encoded_path}"
    resp = requests.get(url, headers={"apikey": key, "Authorization": f"Bearer {key}"}, timeout=60)
    if resp.status_code >= 400:
        return None
    return resp.content


# Additional legacy QC2 sheet aliases/columns used by app9.py.
QC2_TABLE_MAP.setdefault("QC2_Master_Limit", "qc2_media_limit")
QC2_COLUMN_MAP.setdefault("qc2_reagent_usage", {
    "timestamp": "Timestamp", "management_code": "Management_Code",
    "action": "Action", "qty_change": "Qty_Change",
    "new_stock": "New_Stock", "pic": "PIC", "detail": "Detail",
})
QC2_COLUMN_MAP.setdefault("qc2_reagent_bottles", {
    "management_code": "Management_Code", "bottle_id": "Bottle_ID",
    "status": "Status", "opened_date": "Opened_Date",
    "disposed_date": "Disposed_Date", "pic": "PIC", "notes": "Notes",
})
QC2_COLUMN_MAP.setdefault("qc2_signatures", {
    "username": "Username", "full_name": "Full_Name", "role": "Role",
    "signature": "Signature", "timestamp": "Timestamp",
})
QC2_COLUMN_MAP.setdefault("qc2_media_limit", {
    "management_code": "Management Code", "media_name": "Media_Name",
    "min_pack_limit": "Min_Pack_Limit", "set_by": "Set_By",
    "set_date": "Set_Date", "notes": "Notes",
})