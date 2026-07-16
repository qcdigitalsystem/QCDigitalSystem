"""
email_scheduler.py
──────────────────
Drop this file in the same folder as app.py.
Import it once at the top of app.py (see instructions at bottom).

The scheduler starts a background APScheduler thread the first time the
Streamlit app boots.  It fires every Monday at 09:00 (server local time)
and sends the weekly forecast report to all registered email recipients.

Because Streamlit re-runs app.py on every user interaction, the scheduler
is guarded by a threading.Event stored in st.session_state-equivalent
module-level state so it only ever starts once per process.
"""

from __future__ import annotations

import logging
import smtplib
import threading
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
import json
import pandas as pd

logger = logging.getLogger(__name__)

# ── Module-level flag — survives Streamlit re-runs ──────────────────────────
_scheduler_started = threading.Event()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: read email recipients from the JSON file used by planning_page
# ═══════════════════════════════════════════════════════════════════════════════
def _load_emails(email_file: Path) -> list[dict]:
    try:
        if email_file.exists():
            with open(email_file, "r") as f:
                return json.load(f)
    except Exception as exc:
        logger.warning("email_scheduler: could not load recipients: %s", exc)
    return []


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: build forecast HTML from the QC1 google sheet
# ═══════════════════════════════════════════════════════════════════════════════
def _build_forecast_html(
    client,
    sheet_key: str,
    division: str,
) -> str:
    """
    Re-implements the logic of build_email_html() but runs outside of a
    Streamlit session, so it reads data directly from Google Sheets instead
    of relying on st.session_state['forecast_data'].
    """
    try:
        import pretty_html_table  # optional dep — gracefully degrade if missing
        HAS_PRETTY = True
    except ImportError:
        HAS_PRETTY = False

    def tbl_html(df: pd.DataFrame) -> str:
        if df is None or df.empty:
            return "<p><em>None.</em></p>"
        if HAS_PRETTY:
            return pretty_html_table.build_table(df, "green_light")
        # Fallback: plain HTML table
        return df.to_html(index=False, border=1)

    today = datetime.now().date()

    # ── Load reagents ──────────────────────────────────────────────────────────
    try:
        ws_r = client.open_by_key(sheet_key).worksheet(f"{division}_Reagents")
        reagents_df = pd.DataFrame(ws_r.get_all_records())
        if "ID" in reagents_df.columns:
            reagents_df["ID"] = reagents_df["ID"].astype(str).str.strip()
        if "Expiration Date" in reagents_df.columns:
            reagents_df["Expiration Date"] = pd.to_datetime(
                reagents_df["Expiration Date"], errors="coerce"
            )
    except Exception as exc:
        logger.error("email_scheduler: failed to load Reagents: %s", exc)
        return "<p>Error: could not load reagent data.</p>"

    # ── Load bottles ───────────────────────────────────────────────────────────
    try:
        ws_b = client.open_by_key(sheet_key).worksheet(f"{division}_Bottles")
        bottles_df = pd.DataFrame(ws_b.get_all_records())
        if "Box ID" in bottles_df.columns:
            bottles_df.rename(columns={"Box ID": "Reagent ID"}, inplace=True)
        if "Bottle #" in bottles_df.columns:
            bottles_df.rename(columns={"Bottle #": "Bottle Label"}, inplace=True)
        if "Status" in bottles_df.columns:
            bottles_df["Status"] = (
                bottles_df["Status"].str.strip().str.lower().apply(
                    lambda s: "unused" if s in ["unused", "unopened", "available"] else s
                )
            )
        if "Reagent ID" in bottles_df.columns:
            bottles_df["Reagent ID"] = bottles_df["Reagent ID"].astype(str).str.strip()
        for col in ["Opened Date", "Disposed Date"]:
            if col in bottles_df.columns:
                bottles_df[col] = pd.to_datetime(bottles_df[col], errors="coerce")
    except Exception as exc:
        logger.warning("email_scheduler: failed to load Bottles: %s", exc)
        bottles_df = pd.DataFrame()

    # ── Load reorder points ────────────────────────────────────────────────────
    reorder_points: dict = {}
    try:
        ws_rp = client.open_by_key(sheet_key).worksheet(f"{division}_ReorderPoints")
        rp_df = pd.DataFrame(ws_rp.get_all_records())
        for _, row in rp_df.iterrows():
            mgmt = str(row.get("Management Code", "")).strip()
            if mgmt:
                reorder_points[mgmt] = {
                    "warning":  int(row.get("Warning Threshold",  2) or 2),
                    "critical": int(row.get("Critical Threshold", 1) or 1),
                }
    except Exception:
        pass

    # ── Classify every bottle ──────────────────────────────────────────────────
    from datetime import timedelta

    expired_rows: list[dict] = []
    expiring_rows: list[dict] = []
    critical_rows: list[dict] = []
    warning_rows: list[dict] = []

    for _, reagent in reagents_df.iterrows():
        rid = str(reagent["ID"])
        r_bottles = (
            bottles_df[bottles_df["Reagent ID"] == rid]
            if not bottles_df.empty
            else pd.DataFrame()
        )
        active_bottles = (
            r_bottles[r_bottles["Status"] != "disposed"]
            if not r_bottles.empty
            else pd.DataFrame()
        )

        # ── Expiry per bottle ──────────────────────────────────────────────────
        for _, bottle in active_bottles.iterrows():
            status = str(bottle.get("Status", "unused")).lower()
            if status in ["unused", "unopened", "available"]:
                exp_date = reagent.get("Expiration Date")
            else:
                open_date = bottle.get("Opened Date")
                pao_raw   = bottle.get("PAO", "")
                pao_days  = _parse_pao(str(pao_raw)) if pao_raw else None
                if pao_days and pd.notna(open_date):
                    try:
                        exp_date = pd.to_datetime(open_date) + timedelta(days=pao_days)
                    except Exception:
                        exp_date = reagent.get("Expiration Date")
                else:
                    exp_date = reagent.get("Expiration Date")

            if exp_date is None or (hasattr(exp_date, "__class__") and pd.isna(exp_date)):
                continue
            try:
                exp_d = exp_date.date() if hasattr(exp_date, "date") else pd.to_datetime(exp_date).date()
            except Exception:
                continue

            days_left = (exp_d - today).days
            entry = {
                "Reagent Name":    reagent.get("Reagent Name", ""),
                "Management Code": reagent.get("Management Code", ""),
                "Bottle":          bottle.get("Bottle Label", ""),
                "Expired Date":    exp_d.strftime("%d %b %Y"),
            }
            if days_left < 0:
                expired_rows.append(entry)
            elif days_left <= 30:
                expiring_rows.append(entry)

        # ── Low-stock per reagent ──────────────────────────────────────────────
        unopened = (
            len(active_bottles[active_bottles["Status"] == "unused"])
            if not active_bottles.empty
            else 0
        )
        mgmt     = str(reagent.get("Management Code", ""))
        rp       = reorder_points.get(mgmt, {})
        warn_thr = rp.get("warning",  2)
        crit_thr = rp.get("critical", 1)

        # Flag critical if reagent-level expiry already passed
        has_expired = False
        exp_raw = reagent.get("Expiration Date")
        if exp_raw is not None and not pd.isna(exp_raw):
            try:
                exp_chk = exp_raw.date() if hasattr(exp_raw, "date") else pd.to_datetime(exp_raw).date()
                has_expired = exp_chk < today
            except Exception:
                pass

        stock_entry = {
            "Reagent Name":    reagent.get("Reagent Name", ""),
            "Management Code": mgmt,
            "Remaining Stock": unopened,
            "Manufacturer":    reagent.get("Manufacturer", ""),
            "Catalog Number":  reagent.get("Catalog Number", ""),
        }
        if has_expired or unopened <= crit_thr:
            critical_rows.append(stock_entry)
        elif unopened <= warn_thr:
            warning_rows.append(stock_entry)

    # ── Assemble HTML ──────────────────────────────────────────────────────────
    generated_at = datetime.now().strftime("%d %b %Y %H:%M")
    df_exp  = pd.DataFrame(expired_rows)
    df_soon = pd.DataFrame(expiring_rows)
    df_crit = pd.DataFrame(critical_rows)
    df_warn = pd.DataFrame(warning_rows)

    for df_, cols in [
        (df_exp,  ["Reagent Name", "Management Code", "Bottle", "Expired Date"]),
        (df_soon, ["Reagent Name", "Management Code", "Bottle", "Expired Date"]),
        (df_crit, ["Reagent Name", "Management Code", "Remaining Stock", "Manufacturer", "Catalog Number"]),
        (df_warn, ["Reagent Name", "Management Code", "Remaining Stock", "Manufacturer", "Catalog Number"]),
    ]:
        for c in cols:
            if c not in df_.columns:
                df_[c] = ""

    return f"""
    <html>
    <body style="font-family:Arial, sans-serif; margin:20px; color:#1A2B4A;">
      <h2 style="color:#1A2B4A;">📋 Weekly Forecast Monitoring Report — Reagents</h2>
      <p style="color:#718096;">Generated automatically on {generated_at} | Division: {division}</p>
      <hr>

      <h3 style="color:#DC2626;">⛔ Expired Bottles ({len(df_exp)} item{'s' if len(df_exp)!=1 else ''})</h3>
      {tbl_html(df_exp)}

      <h3 style="color:#D97706;">⚠️ Expiring Soon — within 30 days ({len(df_soon)} item{'s' if len(df_soon)!=1 else ''})</h3>
      {tbl_html(df_soon)}

      <h3 style="color:#DC2626;">🔴 Critical Stock — out of stock or expired ({len(df_crit)} item{'s' if len(df_crit)!=1 else ''})</h3>
      {tbl_html(df_crit)}

      <h3 style="color:#D97706;">🟡 Warning Stock — low stock ({len(df_warn)} item{'s' if len(df_warn)!=1 else ''})</h3>
      {tbl_html(df_warn)}

      <hr>
      <p style="font-size:12px;color:#A0AEC0;">
        This is an automated weekly report sent every Monday at 09:00.<br>
        Do not reply to this email.
      </p>
    </body>
    </html>
    """


def _parse_pao(pao_str: str) -> int | None:
    """Parse PAO string like '30 days', '6 months', '1 year' into days."""
    if not pao_str:
        return None
    s = pao_str.strip().lower()
    try:
        amount, unit = s.split()
        amount = float(amount)
    except ValueError:
        try:
            return int(float(s))
        except Exception:
            return None
    if   "day"   in unit: return int(amount)
    elif "month" in unit: return int(amount * 30)
    elif "year"  in unit: return int(amount * 365)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: send one email
# ═══════════════════════════════════════════════════════════════════════════════
def _send_one_email(
    recipient: str,
    subject: str,
    html_body: str,
    smtp_server: str,
    smtp_port: int,
    sender_email: str,
    sender_password: str,
) -> bool:
    try:
        msg = MIMEMultipart()
        msg["From"]    = sender_email
        msg["To"]      = recipient
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        logger.info("email_scheduler: sent to %s", recipient)
        return True
    except Exception as exc:
        logger.error("email_scheduler: failed to send to %s — %s", recipient, exc)
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN SCHEDULED JOB
# ═══════════════════════════════════════════════════════════════════════════════
def _run_weekly_report(
    client,
    sheet_key: str,
    division: str,
    email_file: Path,
    smtp_server: str,
    smtp_port: int,
    sender_email: str,
    sender_password: str,
) -> None:
    """Called by APScheduler every Monday at 09:00."""
    logger.info("email_scheduler: starting weekly report job (%s)", datetime.now())

    recipients = _load_emails(email_file)
    reagent_recipients = [
        r["email"] for r in recipients
        if "Reagents" in r.get("categories", [])
    ]

    if not reagent_recipients:
        logger.info("email_scheduler: no recipients configured, skipping.")
        return

    html_body = _build_forecast_html(client, sheet_key, division)
    subject   = f"[{division}] Weekly Reagent Forecast Report — {datetime.now().strftime('%d %b %Y')}"

    sent = 0
    for email in reagent_recipients:
        if _send_one_email(
            email, subject, html_body,
            smtp_server, smtp_port, sender_email, sender_password,
        ):
            sent += 1

    logger.info("email_scheduler: weekly report sent to %d/%d recipients.", sent, len(reagent_recipients))


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API — call once from app.py at module level
# ═══════════════════════════════════════════════════════════════════════════════
def start_scheduler(
    client,
    sheet_key: str,
    division: str,
    email_file: Path,
    smtp_server: str,
    smtp_port: int,
    sender_email: str,
    sender_password: str,
) -> None:
    """
    Start the APScheduler background thread.
    Safe to call on every Streamlit re-run — only starts once per process.

    Parameters
    ----------
    client          : gspread client (already authorised)
    sheet_key       : Google Sheet key string
    division        : e.g. "QC1"
    email_file      : Path to email_recipients.json used by planning_page
    smtp_server     : e.g. "smtp.gmail.com"
    smtp_port       : e.g. 587
    sender_email    : sending address
    sender_password : app password / SMTP password
    """
    if _scheduler_started.is_set():
        return  # already running — nothing to do

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logger.error(
            "email_scheduler: APScheduler is not installed. "
            "Run:  pip install apscheduler"
        )
        return

    scheduler = BackgroundScheduler(daemon=True)

    scheduler.add_job(
        func=_run_weekly_report,
        trigger=CronTrigger(day_of_week="mon", hour=9, minute=0),
        kwargs=dict(
            client          = client,
            sheet_key       = sheet_key,
            division        = division,
            email_file      = email_file,
            smtp_server     = smtp_server,
            smtp_port       = smtp_port,
            sender_email    = sender_email,
            sender_password = sender_password,
        ),
        id          = f"weekly_report_{division}",
        name        = f"Weekly Forecast Report — {division}",
        replace_existing = True,
        misfire_grace_time = 3600,   # fire up to 1 hour late if server was down
    )

    scheduler.start()
    _scheduler_started.set()
    logger.info(
        "email_scheduler: scheduler started — weekly report fires every Monday at 09:00 (%s)",
        division,
    )
