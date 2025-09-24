import os
import sqlite3
from datetime import datetime
from typing import Dict, Any

import streamlit as st
import pandas as pd
import numpy as np

DB_PATH = "game.db"
ADMIN_PASS = os.getenv("ADMIN_PASS", "1nn1n")

# -------------------------
# DB & seed data
# -------------------------

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    # Teams
    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        team_id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_name TEXT UNIQUE NOT NULL,
        cash REAL DEFAULT 50000000.0,
        created_at TEXT
    )""")
    # Assets
    cur.execute("""
    CREATE TABLE IF NOT EXISTS assets (
        asset_id TEXT PRIMARY KEY,
        name TEXT,
        sector TEXT,
        subsector TEXT,
        district TEXT,
        size_sqm REAL,
        year INTEGER,
        condition TEXT,
        vacancy REAL,
        erv_psm_pm REAL,
        rent_psm_pm REAL,
        opex_psm_pa REAL,
        capex_y1_pct REAL,
        exp_capex_5y_psm REAL,
        price_psm REAL,
        status TEXT
    )""")
    # Assumptions
    cur.execute("""
    CREATE TABLE IF NOT EXISTS assumptions (
        key TEXT PRIMARY KEY,
        value REAL,
        note TEXT
    )""")
    # Curveballs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS curveballs (
        week INTEGER PRIMARY KEY,
        title TEXT,
        rule TEXT
    )""")
    # Positions
    cur.execute("""
    CREATE TABLE IF NOT EXISTS positions (
        position_id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        asset_id TEXT,
        size_sqm REAL,
        price_psm REAL,
        acq_cost_pct REAL,
        ltv_pct REAL,
        debt_draw REAL,
        equity REAL,
        opened_week INTEGER,
        closed_week INTEGER,
        notes TEXT,
        FOREIGN KEY(team_id) REFERENCES teams(team_id),
        FOREIGN KEY(asset_id) REFERENCES assets(asset_id)
    )""")
    # Transactions
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        week INTEGER,
        asset_id TEXT,
        kind TEXT,
        cashflow REAL,
        description TEXT,
        created_at TEXT,
        FOREIGN KEY(team_id) REFERENCES teams(team_id)
    )""")
    # Game state
    cur.execute("""
    CREATE TABLE IF NOT EXISTS game_state (
        id INTEGER PRIMARY KEY CHECK (id=1),
        current_week INTEGER,
        max_ltv REAL
    )""")
    cur.execute("INSERT OR IGNORE INTO game_state (id, current_week, max_ltv) VALUES (1, 1, 0.60)")
    # Weekly snapshots
    cur.execute("""
    CREATE TABLE IF NOT EXISTS weekly_snapshots (
        snap_id INTEGER PRIMARY KEY AUTOINCREMENT,
        week INTEGER,
        team_name TEXT,
        portfolio_value REAL,
        equity REAL,
        debt REAL,
        ltv REAL,
        noi REAL,
        cash REAL,
        created_at TEXT
    )""")
    conn.commit()
    conn.close()

# -------------------------
# Helpers
# -------------------------

def get_assumption(key: str) -> float:
    conn = get_conn()
    df = pd.read_sql_query("SELECT value FROM assumptions WHERE key=?", conn, params=(key,))
    conn.close()
    return float(df.iloc[0,0]) if len(df) else np.nan

def set_assumption(key: str, value: float):
    conn = get_conn()
    conn.execute("UPDATE assumptions SET value=? WHERE key=?", (value, key))
    conn.commit()
    conn.close()

def get_game_week() -> int:
    conn = get_conn()
    df = pd.read_sql_query("SELECT current_week FROM game_state WHERE id=1", conn)
    conn.close()
    return int(df.iloc[0,0])

def set_game_week(week: int):
    conn = get_conn()
    conn.execute("UPDATE game_state SET current_week=? WHERE id=1", (week,))
    conn.commit()
    conn.close()

def get_max_ltv() -> float:
    conn = get_conn()
    df = pd.read_sql_query("SELECT max_ltv FROM game_state WHERE id=1", conn)
    conn.close()
    return float(df.iloc[0,0])

def set_max_ltv(v: float):
    conn = get_conn()
    conn.execute("UPDATE game_state SET max_ltv=? WHERE id=1", (v,))
    conn.commit()
    conn.close()

def list_assets() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM assets", conn)
    conn.close()
    return df

def get_team(team_name: str) -> Dict[str, Any]:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM teams WHERE team_name=?", conn, params=(team_name,))
    if len(df) == 0:
        conn.execute("INSERT INTO teams (team_name, cash, created_at) VALUES (?,?,?)",
                     (team_name, 50000000.0, datetime.utcnow().isoformat()))
        conn.commit()
        df = pd.read_sql_query("SELECT * FROM teams WHERE team_name=?", conn, params=(team_name,))
    conn.close()
    return df.iloc[0].to_dict()

# -------------------------
# KPIs & Snapshots
# -------------------------

def compute_kpis(team_id: int) -> Dict[str, float]:
    conn = get_conn()
    positions = pd.read_sql_query("SELECT * FROM positions WHERE team_id=? AND closed_week IS NULL", conn, params=(team_id,))
    assets = pd.read_sql_query("SELECT * FROM assets", conn).set_index("asset_id")
    y_map = {
        "Residential": get_assumption("Exit_Yield_Residential_%")/100.0,
        "Office": get_assumption("Exit_Yield_Office_%")/100.0,
        "Retail": get_assumption("Exit_Yield_Retail_%")/100.0,
        "Logistics": get_assumption("Exit_Yield_Logistics_%")/100.0,
        "Development": 0.08,
    }
    ibicut = 0.03 if get_game_week() >= 6 else 0.0

    total_value, total_debt, total_noi = 0.0, 0.0, 0.0
    for _, pos in positions.iterrows():
        a = assets.loc[pos["asset_id"]]
        occ = 1.0 - float(a["vacancy"]) / 100.0
        rent = (a["rent_psm_pm"] if pd.notnull(a["rent_psm_pm"]) else (a["erv_psm_pm"] or 0.0))
        noi = pos["size_sqm"] * (occ * (rent*12.0) - a["opex_psm_pa"]) * (1.0 - ibicut)
        cap = y_map.get(a["sector"], 0.05)
        value = noi / cap if cap > 0 else pos["size_sqm"] * (float(a["price_psm"]) if pd.notnull(a["price_psm"]) else 0.0)
        total_noi += max(noi, 0.0)
        total_value += max(value, 0.0)
        total_debt += pos["debt_draw"]

    cash = pd.read_sql_query("SELECT cash FROM teams WHERE team_id=?", conn, params=(team_id,)).iloc[0,0]
    conn.close()

    equity = total_value + cash - total_debt
    ltv = 0.0 if total_value == 0 else total_debt / total_value
    return {"portfolio_value": total_value + cash, "equity": equity, "debt": total_debt, "ltv": ltv, "noi": total_noi, "cash": cash}

def compute_all_kpis() -> pd.DataFrame:
    conn = get_conn()
    teams = pd.read_sql_query("SELECT * FROM teams", conn)
    conn.close()
    rows = []
    for _, t in teams.iterrows():
        k = compute_kpis(int(t.team_id))
        rows.append({"team_name": t.team_name, **k})
    df = pd.DataFrame(rows).sort_values("equity", ascending=False)
    return df

def take_weekly_snapshot():
    week = get_game_week()
    now = datetime.utcnow().isoformat()
    df = compute_all_kpis()
    if df.empty:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM weekly_snapshots WHERE week=?", (week,))
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO weekly_snapshots (week, team_name, portfolio_value, equity, debt, ltv, noi, cash, created_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (int(week), str(row["team_name"]), float(row["portfolio_value"]), float(row["equity"]),
              float(row["debt"]), float(row["ltv"]), float(row["noi"]), float(row["cash"]), now))
    conn.commit(); conn.close()
    return len(df)

def load_snapshots(week: int = None) -> pd.DataFrame:
    conn = get_conn()
    if week is None:
        df = pd.read_sql_query("SELECT * FROM weekly_snapshots ORDER BY week DESC, equity DESC", conn)
    else:
        df = pd.read_sql_query("SELECT * FROM weekly_snapshots WHERE week=? ORDER BY equity DESC", conn, params=(week,))
    conn.close()
    return df

# -------------------------
# Q&A helper
# -------------------------

def qa_answer(question: str) -> str:
    q = question.lower()
    df = list_assets()
    hits = df[df.apply(lambda r: any(tok in str(r.values).lower() for tok in q.split()), axis=1)]
    if not hits.empty:
        r = hits.iloc[0]
        return (
            f"Asset {r.asset_id} — {r.name}\n"
            f"Sector: {r.sector}/{r.subsector}, District: {r.district}\n"
            f"Size: {r.size_sqm:.0f} sqm, Vacancy: {r.vacancy:.1f}%\n"
            f"Rents: ERV {r.erv_psm_pm or r.rent_psm_pm} €/sqm/mo, Current {r.rent_psm_pm} €/sqm/mo\n"
            f"Opex: {r.opex_psm_pa:.1f} €/sqm/yr, Price ask: {r.price_psm:.0f} €/sqm\n"
            f"Condition: {r.condition}, Status: {r.status}"
        )
    if "ltv" in q:
        return f"Max LTV is {get_max_ltv()*100:.0f}%."
    if "yield" in q or "cap" in q:
        return "Cap value ≈ NOI / Exit Yield."
    if "buy" in q:
        return "Use the Trade tab: choose asset, size, price, costs, LTV → Buy."
    return "Try searching by asset code/name, or ask about LTV/yield/how to buy."

# -------------------------
# UI
# -------------------------

def main():
    st.set_page_config(page_title="Madrid RE Portfolio — Bot", layout="wide")
    st.title("🏢 Madrid Real Estate Portfolio — Class Game Bot")
    st.caption("game by Innin Buyl — for exclusive use")

    init_db()

    colA, colB = st.columns([2,1])
    with colB:
        team_name = st.text_input("Your team name", value="Team A")
        team = get_team(team_name)
        st.success(f"Welcome, {team['team_name']} — Cash: €{team['cash']:,.0f}")
        st.info(f"Current Week: {get_game_week()}")

    tabs = st.tabs(["Information", "Portfolio & KPIs", "Ask the Bot", "Instructor"])

    with tabs[0]:
        st.write("This tab explains how the game works...")

    with tabs[1]:
        st.subheader("Portfolio KPIs")
        k = compute_kpis(team["team_id"])
        st.json(k)

    with tabs[2]:
        st.subheader("Ask the Bot")
        q = st.text_input("Ask something about assets or rules")
        if st.button("Ask"):
            st.write(qa_answer(q))

    with tabs[3]:
        st.subheader("Instructor Controls")
        pwd = st.text_input("Admin password", type="password")
        if pwd == ADMIN_PASS:
            st.success("Admin verified")
            week = st.number_input("Set current week", min_value=1, max_value=14, value=get_game_week())
            if st.button("Update Week"):
                set_game_week(int(week))
                st.info("Week updated.")

            st.markdown("### 📊 Leaderboard (live)")
            st.dataframe(compute_all_kpis(), use_container_width=True)

            st.markdown("### 🔒 Weekly Snapshot (lock scores)")
            if st.button("Lock this week's snapshot"):
                n = take_weekly_snapshot()
                st.success(f"Locked {n} team snapshots for Week {get_game_week()}.")

            sel_week = st.number_input("View snapshots for week", min_value=1, max_value=14, value=get_game_week())
            snap = load_snapshots(int(sel_week))
            st.dataframe(snap, use_container_width=True)
            if not snap.empty:
                csv_bytes = snap.to_csv(index=False).encode("utf-8")
                st.download_button("Download snapshots CSV", data=csv_bytes,
                                   file_name=f"snapshots_week_{int(sel_week)}.csv", mime="text/csv")

if __name__ == "__main__":
    main()
