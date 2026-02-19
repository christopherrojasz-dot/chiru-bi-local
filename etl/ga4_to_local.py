#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
ga4_to_local.py
----------------
Extrae agregados diarios desde GA4 (Google Analytics Data API) y los guarda en Postgres local.

Variables de entorno (OBLIGATORIAS para correr GA4):
- NEON_CONNECTION_STRING: Host=...;Port=...;Username=...;Password=...;Database=...
- GA4_PROPERTY_ID
- GA4_CLIENT: ruta a OAuth Desktop client JSON
- GA4_TOKEN: ruta donde se guardara el token JSON

Comportamiento importante:
- Si GA4 no devuelve filas (propiedad nueva / sin eventos): NO falla. Deja 0 filas y termina OK.
- Usa startDate/endDate en formato YYYY-MM-DD (evita error 400 por usar YYYYMMDD).
'''

import argparse
import datetime as dt
import os
import sys
from typing import Dict, List, Tuple

import psycopg2
from psycopg2.extras import execute_values

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.analytics.data_v1beta.types import FilterExpression, Filter

from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--lookback", type=int, default=30, help="Dias hacia atras desde hoy (inclusive).")
    p.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD (opcional).")
    p.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD (opcional).")
    return p.parse_args()


def _as_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def compute_range(args: argparse.Namespace) -> Tuple[str, str]:
    if args.start_date and args.end_date:
        start = _as_date(args.start_date)
        end = _as_date(args.end_date)
    else:
        end = dt.date.today()
        start = end - dt.timedelta(days=max(args.lookback, 0))
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def load_credentials(client_path: str, token_path: str) -> Credentials:
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(client_path, SCOPES)
        creds = flow.run_local_server(port=0)

    os.makedirs(os.path.dirname(token_path), exist_ok=True)
    with open(token_path, "w", encoding="utf-8") as f:
        f.write(creds.to_json())
    return creds


def run_event_daily(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: str,
    end_date: str
) -> List[Tuple[dt.date, str, int, int, int]]:
    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="date"), Dimension(name="eventName")],
        metrics=[Metric(name="eventCount"), Metric(name="totalUsers"), Metric(name="sessions")],
    )
    resp = client.run_report(request)
    rows = []
    for r in resp.rows:
        day_str = r.dimension_values[0].value  # YYYYMMDD
        day = dt.datetime.strptime(day_str, "%Y%m%d").date()
        event_name = r.dimension_values[1].value
        event_count = int(r.metric_values[0].value or 0)
        total_users = int(r.metric_values[1].value or 0)
        sessions = int(r.metric_values[2].value or 0)
        rows.append((day, event_name, event_count, total_users, sessions))
    return rows


def run_funnel_daily(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: str,
    end_date: str
) -> List[Tuple[dt.date, int, int, int, int, int]]:
    events = ["session_start", "view_item", "add_to_cart", "begin_checkout", "purchase"]

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name="date"), Dimension(name="eventName")],
        metrics=[Metric(name="eventCount")],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="eventName",
                in_list_filter=Filter.InListFilter(values=events),
            )
        ),
    )

    resp = client.run_report(request)
    by_day: Dict[dt.date, Dict[str, int]] = {}
    for r in resp.rows:
        day_str = r.dimension_values[0].value  # YYYYMMDD
        day = dt.datetime.strptime(day_str, "%Y%m%d").date()
        ev = r.dimension_values[1].value
        cnt = int(r.metric_values[0].value or 0)
        by_day.setdefault(day, {})
        by_day[day][ev] = by_day[day].get(ev, 0) + cnt

    out = []
    for day, m in sorted(by_day.items()):
        sessions = m.get("session_start", 0)
        view_item = m.get("view_item", 0)
        add_to_cart = m.get("add_to_cart", 0)
        begin_checkout = m.get("begin_checkout", 0)
        purchase = m.get("purchase", 0)
        out.append((day, sessions, view_item, add_to_cart, begin_checkout, purchase))
    return out


def connect_pg(conn_str: str):
    parts = {}
    for chunk in conn_str.split(";"):
        chunk = chunk.strip()
        if not chunk or "=" not in chunk:
            continue
        k, v = chunk.split("=", 1)
        parts[k.strip().lower()] = v.strip()

    dsn = " ".join([
        f"host={parts.get('host', '')}",
        f"port={parts.get('port', '5432')}",
        f"user={parts.get('username', parts.get('user', ''))}",
        f"password={parts.get('password', '')}",
        f"dbname={parts.get('database', parts.get('dbname', ''))}",
    ])
    return psycopg2.connect(dsn)


def upsert_event_daily(cur, rows: List[Tuple[dt.date, str, int, int, int]]) -> int:
    if not rows:
        return 0
    sql = '''
    INSERT INTO ga4.event_daily (day, event_name, event_count, total_users, sessions, updated_at)
    VALUES %s
    ON CONFLICT (day, event_name)
    DO UPDATE SET
      event_count = EXCLUDED.event_count,
      total_users = EXCLUDED.total_users,
      sessions = EXCLUDED.sessions,
      updated_at = now();
    '''
    execute_values(cur, sql, rows)
    return len(rows)


def upsert_funnel_daily(cur, rows: List[Tuple[dt.date, int, int, int, int, int]]) -> int:
    if not rows:
        return 0
    sql = '''
    INSERT INTO ga4.funnel_daily (day, sessions, view_item, add_to_cart, begin_checkout, purchase, updated_at)
    VALUES %s
    ON CONFLICT (day)
    DO UPDATE SET
      sessions = EXCLUDED.sessions,
      view_item = EXCLUDED.view_item,
      add_to_cart = EXCLUDED.add_to_cart,
      begin_checkout = EXCLUDED.begin_checkout,
      purchase = EXCLUDED.purchase,
      updated_at = now();
    '''
    execute_values(cur, sql, rows)
    return len(rows)


def main() -> int:
    args = parse_args()

    conn_str = os.environ.get("NEON_CONNECTION_STRING", "")
    property_id = os.environ.get("GA4_PROPERTY_ID", "")
    client_path = os.environ.get("GA4_CLIENT", "")
    token_path = os.environ.get("GA4_TOKEN", "")

    if not conn_str:
        print("[ERROR] Falta NEON_CONNECTION_STRING (apunta a Postgres local).")
        return 2

    # Si GA4 no esta configurado, no romper el pipeline (OK sin data).
    if not property_id or property_id == "TU_PROPERTY_ID":
        print("[WARN] GA4_PROPERTY_ID no configurado. Se omite GA4. OK (sin data).")
        return 0

    if not client_path or not os.path.exists(client_path):
        print(f"[WARN] GA4_CLIENT no existe: {client_path}. Se omite GA4. OK (sin data).")
        return 0

    start_date, end_date = compute_range(args)
    print(f"[INFO] Rango GA4: startDate={start_date} endDate={end_date}")

    try:
        creds = load_credentials(client_path, token_path)
        client = BetaAnalyticsDataClient(credentials=creds)
    except Exception as e:
        print(f"[ERROR] OAuth fallo: {e}")
        return 3

    try:
        event_rows = run_event_daily(client, property_id, start_date, end_date)
        funnel_rows = run_funnel_daily(client, property_id, start_date, end_date)
    except Exception as e:
        print(f"[ERROR] GA4 API fallo: {e}")
        return 4

    if not event_rows and not funnel_rows:
        print("[OK] GA4 devolvio 0 filas (sin eventos o rango sin data). Termina OK.")
        return 0

    try:
        conn = connect_pg(conn_str)
        conn.autocommit = False
        with conn.cursor() as cur:
            inserted_events = upsert_event_daily(cur, event_rows)
            inserted_funnel = upsert_funnel_daily(cur, funnel_rows)
        conn.commit()
        conn.close()
        print(f"[OK] Upsert completado: event_daily={inserted_events} filas, funnel_daily={inserted_funnel} filas.")
        return 0
    except Exception as e:
        print(f"[ERROR] Postgres upsert fallo: {e}")
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        return 5


if __name__ == "__main__":
    sys.exit(main())
