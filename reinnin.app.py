# re_game_app.py
# Real Estate Portfolio Game — Madrid (50 Assets) with ROI inputs + Instructor reset
# game by Innin Buyl — for exclusive use

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st

# ---------------------------
# Config
# ---------------------------
DB_PATH = "re_game.db"
INITIAL_CASH = 26_000_000.0  # €26m starting budget (edit if you like)
SALE_SOFT_CAP_FACTOR = 1.07  # +7% over entry €/sqm
START_WEEK = 1
END_WEEK = 14
ADMIN_PASS = "1nn1n"  # instructor password for reset

# ---------------------------
# Curveballs (announcements + numeric effects, applied once per week)
# ---------------------------
CURVEBALLS: Dict[int, str] = {
    1: "+25 bps interest rate shock (financing more expensive)",
    2: "Retail softness: secondary retail ERVs drop",
    3: "LP mandate: >=20% allocation to Value-Add/Opportunistic by Week 4 (informational)",
    4: "Bidding war: asking prices +7% pressure (market assets only)",
    5: "FX wobble: noise only; no direct P&L impact",
    6: "IBI/tax re-rate: NOI -3% pressure via taxes",
    7: "Tenant bankruptcy: vacancy risk spikes (effective ERV -20%) for Office & Retail",
    8: "Bank tightens: Max LTV 55% (if using debt module; informational)",
    9: "Energy spike: opex +12% pressure",
    10: "PropTech incentive: retention +10% (informational)",
    11: "Benchmarking bonus: top quartile rewarded (informational)",
    12: "Residential demand: ERV growth +2%",
    13: "Green subsidy: 30% rebate on capex (informational)",
    14: "Freeze & score: final valuations",
}

# ---------------------------
# Data models
# ---------------------------
@dataclass
class AssetSeed:
    asset_id: str
    property_name: str
    sector: str
    location: str
    sqm: int
    ask_psm: float
    erv_psm: float
    opex_psm: float
    tax_psm: float

# ---------------------------
# Fixed catalog: 50 Madrid assets (deterministic)
# ---------------------------
ASSETS_DATA: List[dict] = [
    # Residential (12)
    {"asset_id":"A001","property_name":"RES-MAD-SALAMAN-01","sector":"Residential","location":"Salamanca","sqm":4200,"ask_psm":5200.0,"erv_psm":312.0,"opex_psm":31.2,"tax_psm":24.0},
    {"asset_id":"A002","property_name":"RES-MAD-CHAMART-02","sector":"Residential","location":"Chamartín","sqm":3500,"ask_psm":4800.0,"erv_psm":264.0,"opex_psm":24.0,"tax_psm":21.6},
    {"asset_id":"A003","property_name":"RES-MAD-CHAMBER-03","sector":"Residential","location":"Chamberí","sqm":3900,"ask_psm":5400.0,"erv_psm":291.6,"opex_psm":32.4,"tax_psm":27.0},
    {"asset_id":"A004","property_name":"RES-MAD-CENTRO-04","sector":"Residential","location":"Centro","sqm":2800,"ask_psm":5600.0,"erv_psm":308.0,"opex_psm":33.6,"tax_psm":28.0},
    {"asset_id":"A005","property_name":"RES-MAD-RETIRO-05","sector":"Residential","location":"Retiro","sqm":3100,"ask_psm":5000.0,"erv_psm":285.0,"opex_psm":30.0,"tax_psm":25.0},
    {"asset_id":"A006","property_name":"RES-MAD-TETUAN-06","sector":"Residential","location":"Tetuán","sqm":4600,"ask_psm":3900.0,"erv_psm":195.0,"opex_psm":19.5,"tax_psm":15.6},
    {"asset_id":"A007","property_name":"RES-MAD-ARGANZ-07","sector":"Residential","location":"Arganzuela","sqm":5200,"ask_psm":3600.0,"erv_psm":172.8,"opex_psm":18.0,"tax_psm":14.4},
    {"asset_id":"A008","property_name":"RES-MAD-MONCLO-08","sector":"Residential","location":"Moncloa","sqm":2700,"ask_psm":4700.0,"erv_psm":246.8,"opex_psm":23.5,"tax_psm":18.8},
    {"asset_id":"A009","property_name":"RES-MAD-LATINA-09","sector":"Residential","location":"La Latina","sqm":6000,"ask_psm":3400.0,"erv_psm":163.2,"opex_psm":17.0,"tax_psm":13.6},
    {"asset_id":"A010","property_name":"RES-MAD-CARABN-10","sector":"Residential","location":"Carabanchel","sqm":4800,"ask_psm":3200.0,"erv_psm":156.8,"opex_psm":14.4,"tax_psm":12.8},
    {"asset_id":"A011","property_name":"RES-MAD-USERA-11","sector":"Residential","location":"Usera","sqm":4200,"ask_psm":3300.0,"erv_psm":165.0,"opex_psm":14.9,"tax_psm":11.9},
    {"asset_id":"A012","property_name":"RES-MAD-VALLEC-12","sector":"Residential","location":"Vallecas","sqm":3800,"ask_psm":3500.0,"erv_psm":175.0,"opex_psm":15.8,"tax_psm":12.6},

    # Office (12)
    {"asset_id":"A013","property_name":"OFF-MAD-SALAMAN-13","sector":"Office","location":"Salamanca","sqm":7200,"ask_psm":3800.0,"erv_psm":209.0,"opex_psm":27.4,"tax_psm":19.0},
    {"asset_id":"A014","property_name":"OFF-MAD-CHAMART-14","sector":"Office","location":"Chamartín","sqm":6500,"ask_psm":3600.0,"erv_psm":201.6,"opex_psm":25.2,"tax_psm":18.0},
    {"asset_id":"A015","property_name":"OFF-MAD-CHAMBER-15","sector":"Office","location":"Chamberí","sqm":5400,"ask_psm":3400.0,"erv_psm":176.8,"opex_psm":22.1,"tax_psm":15.3},
    {"asset_id":"A016","property_name":"OFF-MAD-CENTRO-16","sector":"Office","location":"Centro","sqm":4800,"ask_psm":4000.0,"erv_psm":220.0,"opex_psm":28.0,"tax_psm":22.0},
    {"asset_id":"A017","property_name":"OFF-MAD-RETIRO-17","sector":"Office","location":"Retiro","sqm":5100,"ask_psm":3500.0,"erv_psm":189.0,"opex_psm":21.9,"tax_psm":17.5},
    {"asset_id":"A018","property_name":"OFF-MAD-TETUAN-18","sector":"Office","location":"Tetuán","sqm":6900,"ask_psm":3000.0,"erv_psm":162.0,"opex_psm":18.0,"tax_psm":14.4},
    {"asset_id":"A019","property_name":"OFF-MAD-ARGANZ-19","sector":"Office","location":"Arganzuela","sqm":5600,"ask_psm":2900.0,"erv_psm":156.6,"opex_psm":17.4,"tax_psm":13.1},
    {"asset_id":"A020","property_name":"OFF-MAD-MONCLO-20","sector":"Office","location":"Moncloa","sqm":4300,"ask_psm":3200.0,"erv_psm":166.4,"opex_psm":20.5,"tax_psm":14.4},
    {"asset_id":"A021","property_name":"OFF-MAD-LATINA-21","sector":"Office","location":"La Latina","sqm":4700,"ask_psm":2800.0,"erv_psm":145.6,"opex_psm":17.4,"tax_psm":12.3},
    {"asset_id":"A022","property_name":"OFF-MAD-CARABN-22","sector":"Office","location":"Carabanchel","sqm":6200,"ask_psm":2600.0,"erv_psm":135.2,"opex_psm":16.1,"tax_psm":11.7},
    {"asset_id":"A023","property_name":"OFF-MAD-USERA-23","sector":"Office","location":"Usera","sqm":5800,"ask_psm":2500.0,"erv_psm":127.5,"opex_psm":15.0,"tax_psm":11.3},
    {"asset_id":"A024","property_name":"OFF-MAD-VALLEC-24","sector":"Office","location":"Vallecas","sqm":5000,"ask_psm":2400.0,"erv_psm":124.8,"opex_psm":14.4,"tax_psm":10.8},

    # Retail (13)
    {"asset_id":"A025","property_name":"RET-MAD-SALAMAN-25","sector":"Retail","location":"Salamanca","sqm":3000,"ask_psm":3300.0,"erv_psm":181.5,"opex_psm":23.1,"tax_psm":16.5},
    {"asset_id":"A026","property_name":"RET-MAD-CHAMART-26","sector":"Retail","location":"Chamartín","sqm":2600,"ask_psm":3200.0,"erv_psm":172.8,"opex_psm":21.1,"tax_psm":16.0},
    {"asset_id":"A027","property_name":"RET-MAD-CHAMBER-27","sector":"Retail","location":"Chamberí","sqm":2400,"ask_psm":3100.0,"erv_psm":167.4,"opex_psm":22.3,"tax_psm":14.9},
    {"asset_id":"A028","property_name":"RET-MAD-CENTRO-28","sector":"Retail","location":"Centro","sqm":2800,"ask_psm":3500.0,"erv_psm":210.0,"opex_psm":24.5,"tax_psm":19.3},
    {"asset_id":"A029","property_name":"RET-MAD-RETIRO-29","sector":"Retail","location":"Retiro","sqm":2200,"ask_psm":2900.0,"erv_psm":159.3,"opex_psm":20.3,"tax_psm":14.5},
    {"asset_id":"A030","property_name":"RET-MAD-TETUAN-30","sector":"Retail","location":"Tetuán","sqm":3600,"ask_psm":2300.0,"erv_psm":115.0,"opex_psm":18.4,"tax_psm":11.5},
    {"asset_id":"A031","property_name":"RET-MAD-ARGANZ-31","sector":"Retail","location":"Arganzuela","sqm":4100,"ask_psm":2000.0,"erv_psm":100.0,"opex_psm":16.0,"tax_psm":10.0},
    {"asset_id":"A032","property_name":"RET-MAD-MONCLO-32","sector":"Retail","location":"Moncloa","sqm":2700,"ask_psm":2700.0,"erv_psm":145.8,"opex_psm":18.9,"tax_psm":13.5},
    {"asset_id":"A033","property_name":"RET-MAD-LATINA-33","sector":"Retail","location":"La Latina","sqm":3900,"ask_psm":2100.0,"erv_psm":105.0,"opex_psm":16.8,"tax_psm":11.0},
    {"asset_id":"A034","property_name":"RET-MAD-CARABN-34","sector":"Retail","location":"Carabanchel","sqm":3200,"ask_psm":2400.0,"erv_psm":122.4,"opex_psm":19.2,"tax_psm":12.0},
    {"asset_id":"A035","property_name":"RET-MAD-USERA-35","sector":"Retail","location":"Usera","sqm":3500,"ask_psm":2200.0,"erv_psm":118.8,"opex_psm":17.6,"tax_psm":11.0},
    {"asset_id":"A036","property_name":"RET-MAD-VALLEC-36","sector":"Retail","location":"Vallecas","sqm":2300,"ask_psm":1800.0,"erv_psm":108.0,"opex_psm":14.4,"tax_psm":9.9},
    {"asset_id":"A037","property_name":"RET-MAD-MORATAL-37","sector":"Retail","location":"Moratalaz","sqm":2500,"ask_psm":2500.0,"erv_psm":137.5,"opex_psm":17.5,"tax_psm":12.5},

    # Logistics (13)
    {"asset_id":"A038","property_name":"LOG-MAD-VICLVAR-38","sector":"Logistics","location":"Vicálvaro","sqm":9000,"ask_psm":2100.0,"erv_psm":115.5,"opex_psm":15.8,"tax_psm":12.6},
    {"asset_id":"A039","property_name":"LOG-MAD-SANBLA-39","sector":"Logistics","location":"San Blas","sqm":11000,"ask_psm":2000.0,"erv_psm":110.0,"opex_psm":15.0,"tax_psm":12.0},
    {"asset_id":"A040","property_name":"LOG-MAD-BARAJAS-40","sector":"Logistics","location":"Barajas","sqm":8000,"ask_psm":2200.0,"erv_psm":121.0,"opex_psm":16.5,"tax_psm":13.2},
    {"asset_id":"A041","property_name":"LOG-MAD-VALLEC-41","sector":"Logistics","location":"Vallecas","sqm":10000,"ask_psm":1800.0,"erv_psm":102.6,"opex_psm":14.4,"tax_psm":10.8},
    {"asset_id":"A042","property_name":"LOG-MAD-VILLVER-42","sector":"Logistics","location":"Villaverde","sqm":9500,"ask_psm":1750.0,"erv_psm":96.3,"opex_psm":14.0,"tax_psm":10.5},
    {"asset_id":"A043","property_name":"LOG-MAD-HORTALZ-43","sector":"Logistics","location":"Hortaleza","sqm":7800,"ask_psm":1950.0,"erv_psm":107.3,"opex_psm":15.6,"tax_psm":11.7},
    {"asset_id":"A044","property_name":"LOG-MAD-CIU-LIN-44","sector":"Logistics","location":"Ciudad Lineal","sqm":8200,"ask_psm":1900.0,"erv_psm":104.5,"opex_psm":15.2,"tax_psm":11.4},
    {"asset_id":"A045","property_name":"LOG-MAD-USERA-45","sector":"Logistics","location":"Usera","sqm":7000,"ask_psm":1700.0,"erv_psm":93.5,"opex_psm":13.6,"tax_psm":10.2},
    {"asset_id":"A046","property_name":"LOG-MAD-TETUAN-46","sector":"Logistics","location":"Tetuán","sqm":7500,"ask_psm":1650.0,"erv_psm":90.8,"opex_psm":13.2,"tax_psm":9.9},
    {"asset_id":"A047","property_name":"LOG-MAD-ARGANZ-47","sector":"Logistics","location":"Arganzuela","sqm":8200,"ask_psm":1600.0,"erv_psm":88.0,"opex_psm":12.8,"tax_psm":9.6},
    {"asset_id":"A048","property_name":"LOG-MAD-MONCLO-48","sector":"Logistics","location":"Moncloa","sqm":8800,"ask_psm":1850.0,"erv_psm":103.7,"opex_psm":14.8,"tax_psm":11.1},
    {"asset_id":"A049","property_name":"LOG-MAD-LATINA-49","sector":"Logistics","location":"La Latina","sqm":9600,"ask_psm":1500.0,"erv_psm":82.5,"opex_psm":12.0,"tax_psm":9.0},
    {"asset_id":"A050","property_name":"LOG-MAD-CARABN-50","sector":"Logistics","location":"Carabanchel","sqm":10400,"ask_psm":1450.0,"erv_psm":78.3,"opex_psm":11.6,"tax_psm":8.7},
]

# ---------------------------
# DB helpers
# ---------------------------
def conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def ensure_col(c, table: str, definition: str):
    # definition like 'passing_psm REAL'
    col = definition.split()[0]
    cur = c.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")
        c.commit()

def init_db():
    c = conn()
    cur = c.cursor()
    # assets
    cur.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            asset_id TEXT PRIMARY KEY,
            property_name TEXT,
            sector TEXT,
            location TEXT,
            sqm INTEGER,
            ask_psm REAL,
            erv_psm REAL,
            opex_psm REAL,
            tax_psm REAL,
            passing_psm REAL,
            vacancy_pct REAL
        )
    """)
    # teams
    cur.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            name TEXT PRIMARY KEY,
            cash REAL
        )
    """)
    # holdings (global supply gating by presence in holdings)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS holdings (
            name TEXT,
            asset_id TEXT,
            property_name TEXT,
            sector TEXT,
            location TEXT,
            sqm INTEGER,
            entry_psm REAL,
            buy_week INTEGER,
            PRIMARY KEY (name, asset_id)
        )
    """)
    # sale blocks for cap violations
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sale_blocks (
            name TEXT,
            asset_id TEXT,
            blocked_until_week INTEGER,
            PRIMARY KEY (name, asset_id)
        )
    """)
    # game state
    cur.execute("""
        CREATE TABLE IF NOT EXISTS game_state (
            id INTEGER PRIMARY KEY,
            current_week INTEGER
        )
    """)
    # which curveballs already applied
    cur.execute("""
        CREATE TABLE IF NOT EXISTS applied_curveballs (
            week INTEGER PRIMARY KEY
        )
    """)
    # initialize game week if not set
    cur.execute("INSERT OR IGNORE INTO game_state (id, current_week) VALUES (1, ?)", (START_WEEK,))
    c.commit()
    # Ensure new columns exist if DB was old
    ensure_col(c, "assets", "passing_psm REAL")
    ensure_col(c, "assets", "vacancy_pct REAL")
    c.close()

def seed_assets_once():
    c = conn()
    cur = c.cursor()
    existing = cur.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
    if existing == 0:
        for row in ASSETS_DATA:
            cur.execute("""
                INSERT INTO assets (asset_id, property_name, sector, location, sqm, ask_psm, erv_psm, opex_psm, tax_psm)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (row["asset_id"], row["property_name"], row["sector"], row["location"], row["sqm"],
                  row["ask_psm"], row["erv_psm"], row["opex_psm"], row["tax_psm"]))
        c.commit()
    c.close()

# ---------------------------
# ROI income rules
# ---------------------------
def classify_profile(location: str, sector: str) -> str:
    prime = {"Salamanca","Chamberí","Centro","Chamartín","Retiro","Moncloa","Barajas"}
    if location in prime:
        return "Core"
    if sector == "Logistics":
        return "Core+"
    return "Value-Add"

def profile_factors(profile: str) -> Tuple[float, float]:
    # returns (passing_factor_on_ERV, vacancy_pct)
    if profile == "Core":
        return 1.00, 0.04
    if profile == "Core+":
        return 0.92, 0.12
    return 0.85, 0.25

def patch_income_assumptions_once():
    """Fill passing_psm and vacancy_pct if missing, using deterministic rules."""
    c = conn(); cur = c.cursor()
    rows = cur.execute("SELECT asset_id, sector, location, erv_psm, passing_psm, vacancy_pct FROM assets").fetchall()
    updated = 0
    for asset_id, sector, location, erv_psm, passing_psm, vacancy_pct in rows:
        if passing_psm is None or vacancy_pct is None:
            prof = classify_profile(location, sector)
            pf, vac = profile_factors(prof)
            new_pass = round(float(erv_psm) * pf, 2)
            cur.execute("UPDATE assets SET passing_psm=?, vacancy_pct=? WHERE asset_id=?", (new_pass, vac, asset_id))
            updated += 1
    if updated:
        c.commit()
    c.close()

# ---------------------------
# Game state helpers
# ---------------------------
def get_week() -> int:
    c = conn(); cur = c.cursor()
    w = cur.execute("SELECT current_week FROM game_state WHERE id=1").fetchone()[0]
    c.close()
    return int(w)

def set_week(w: int):
    c = conn(); cur = c.cursor()
    cur.execute("UPDATE game_state SET current_week=? WHERE id=1", (w,))
    c.commit(); c.close()

def get_team_cash(name: str) -> float:
    c = conn(); cur = c.cursor()
    row = cur.execute("SELECT cash FROM teams WHERE name=?", (name,)).fetchone()
    c.close()
    return float(row[0]) if row else None

def ensure_team(name: str):
    c = conn(); cur = c.cursor()
    cur.execute("INSERT OR IGNORE INTO teams (name, cash) VALUES (?, ?)", (name, INITIAL_CASH))
    c.commit(); c.close()

def set_team_cash(name: str, cash: float):
    c = conn(); cur = c.cursor()
    cur.execute("UPDATE teams SET cash=? WHERE name=?", (cash, name))
    c.commit(); c.close()

def is_held_globally(asset_id: str) -> bool:
    c = conn(); cur = c.cursor()
    row = cur.execute("SELECT COUNT(*) FROM holdings WHERE asset_id=?", (asset_id,)).fetchone()
    c.close()
    return row[0] > 0

def team_portfolio(name: str) -> pd.DataFrame:
    c = conn()
    df = pd.read_sql_query("SELECT asset_id, property_name, sector, location, sqm, entry_psm, buy_week FROM holdings WHERE name=? ORDER BY asset_id", c, params=(name,))
    c.close()
    return df

def add_holding(name: str, asset_id: str, entry_psm: float, buy_week: int):
    c = conn(); cur = c.cursor()
    # fetch static fields from assets
    a = cur.execute("SELECT property_name, sector, location, sqm FROM assets WHERE asset_id=?", (asset_id,)).fetchone()
    property_name, sector, location, sqm = a
    cur.execute("""
        INSERT OR REPLACE INTO holdings (name, asset_id, property_name, sector, location, sqm, entry_psm, buy_week)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, asset_id, property_name, sector, location, sqm, entry_psm, buy_week))
    c.commit(); c.close()

def remove_holding(name: str, asset_id: str):
    c = conn(); cur = c.cursor()
    cur.execute("DELETE FROM holdings WHERE name=? AND asset_id=?", (name, asset_id))
    c.commit(); c.close()

def is_blocked(name: str, asset_id: str, current_week: int) -> Tuple[bool, int]:
    c = conn(); cur = c.cursor()
    row = cur.execute("SELECT blocked_until_week FROM sale_blocks WHERE name=? AND asset_id=?", (name, asset_id)).fetchone()
    c.close()
    if not row: return (False, -1)
    blocked_until = int(row[0])
    return (current_week < blocked_until, blocked_until)

def block_until_next_week(name: str, asset_id: str, current_week: int):
    c = conn(); cur = c.cursor()
    cur.execute("INSERT OR REPLACE INTO sale_blocks (name, asset_id, blocked_until_week) VALUES (?, ?, ?)",
                (name, asset_id, current_week + 1))
    c.commit(); c.close()

def clear_block(name: str, asset_id: str):
    c = conn(); cur = c.cursor()
    cur.execute("DELETE FROM sale_blocks WHERE name=? AND asset_id=?", (name, asset_id))
    c.commit(); c.close()

def curveball_applied(week: int) -> bool:
    c = conn(); cur = c.cursor()
    row = cur.execute("SELECT 1 FROM applied_curveballs WHERE week=?", (week,)).fetchone()
    c.close()
    return row is not None

def mark_curveball_applied(week: int):
    c = conn(); cur = c.cursor()
    cur.execute("INSERT OR IGNORE INTO applied_curveballs (week) VALUES (?)", (week,))
    c.commit(); c.close()

def apply_curveball_effects(week: int):
    """Apply numeric effects exactly once per week to the assets table."""
    if curveball_applied(week):
        return
    c = conn(); cur = c.cursor()

    if week == 2:
        # Retail softness: ERV -5% for Retail
        cur.execute("UPDATE assets SET erv_psm = ROUND(erv_psm * 0.95, 2) WHERE sector='Retail'")
    elif week == 4:
        # Bidding war: asking +7% for assets currently in market (not held)
        # Get held asset_ids
        held_ids = [r[0] for r in cur.execute("SELECT DISTINCT asset_id FROM holdings").fetchall()]
        if held_ids:
            placeholders = ",".join(["?"]*len(held_ids))
            cur.execute(f"UPDATE assets SET ask_psm = ROUND(ask_psm * 1.07, 2) WHERE asset_id NOT IN ({placeholders})", held_ids)
        else:
            cur.execute("UPDATE assets SET ask_psm = ROUND(ask_psm * 1.07, 2)")
    elif week == 6:
        # IBI/tax re-rate: taxes +3% for all
        cur.execute("UPDATE assets SET tax_psm = ROUND(tax_psm * 1.03, 2)")
    elif week == 7:
        # Effective ERV -20% for Office & Retail (proxy)
        cur.execute("UPDATE assets SET erv_psm = ROUND(erv_psm * 0.80, 2) WHERE sector IN ('Office','Retail')")
    elif week == 9:
        # Energy spike: opex +12%
        cur.execute("UPDATE assets SET opex_psm = ROUND(opex_psm * 1.12, 2)")
    elif week == 12:
        # Residential demand: ERV +2%
        cur.execute("UPDATE assets SET erv_psm = ROUND(erv_psm * 1.02, 2) WHERE sector='Residential'")

    # Recompute passing_psm after any ERV change (keep vacancy fixed)
    if week in (2,7,12):
        rows = cur.execute("SELECT asset_id, sector, location, erv_psm FROM assets").fetchall()
        for asset_id, sector, location, erv_psm in rows:
            prof = classify_profile(location, sector)
            pf, _ = profile_factors(prof)
            new_pass = round(float(erv_psm) * pf, 2)
            cur.execute("UPDATE assets SET passing_psm=? WHERE asset_id=?", (new_pass, asset_id))

    c.commit()
    c.close()
    mark_curveball_applied(week)

# ---------------------------
# UI components
# ---------------------------
def header(name: str):
    st.title("Real Estate Portfolio Game — Madrid (50 Assets)")
    col1, col2, col3 = st.columns([1.6, 1, 1])
    with col1:
        st.text_input("Name", key="name", placeholder="Team / Player", value=name)
    with col2:
        st.metric("Week", get_week())
    with col3:
        cash = get_team_cash(st.session_state.name) if st.session_state.name else None
        st.metric("Cash (€)", f"{cash:,.0f}" if cash is not None else "—")

    # Weekly announcement banner
    cb = CURVEBALLS.get(get_week())
    if cb:
        st.info(f"**This week’s curveball:** {cb}")

def market_view():
    st.subheader("Market — Madrid")
    c = conn()
    # Determine which assets are available (not held by anyone)
    held_df = pd.read_sql_query("SELECT DISTINCT asset_id FROM holdings", c)
    held_set = set(held_df["asset_id"].tolist())
    df = pd.read_sql_query("""
        SELECT asset_id, property_name, sector, location, sqm, ask_psm
        FROM assets
        ORDER BY asset_id
    """, c)
    c.close()
    df["available"] = df["asset_id"].apply(lambda x: "Yes" if x not in held_set else "Held")
    st.dataframe(df, hide_index=True, use_container_width=True)

def portfolio_view():
    st.subheader("Portfolio (Open Positions)")
    name = st.session_state.name.strip()
    if not name:
        st.caption("Enter your team name above to view portfolio.")
        return
    df = team_portfolio(name)
    if df.empty:
        st.caption("No holdings yet.")
        return
    st.dataframe(df, hide_index=True, use_container_width=True)

def buy_view():
    st.subheader("Buy Asset (whole-asset only) — Madrid")
    name = st.session_state.name.strip()
    if not name:
        st.warning("Enter your team name above.")
        return
    ensure_team(name)
    cash = get_team_cash(name)
    c = conn(); cur = c.cursor()
    # Available assets = not in holdings
    held_ids = [r[0] for r in cur.execute("SELECT DISTINCT asset_id FROM holdings").fetchall()]
    if held_ids:
        placeholders = ",".join(["?"]*len(held_ids))
        available = pd.read_sql_query(
            f"SELECT asset_id, property_name, sector, location, sqm, ask_psm FROM assets WHERE asset_id NOT IN ({placeholders}) ORDER BY asset_id",
            c, params=held_ids
        )
    else:
        available = pd.read_sql_query(
            "SELECT asset_id, property_name, sector, location, sqm, ask_psm FROM assets ORDER BY asset_id", c
        )
    c.close()

    if available.empty:
        st.warning("No assets are available right now. Sell something to release supply.")
        return

    available["label"] = available.apply(lambda r: f"{r.asset_id} — {r.property_name} ({r.sector}, {r.location})", axis=1)
    label = st.selectbox("Select asset", options=available["label"].tolist())
    row = available[available["label"] == label].iloc[0]

    ticket = float(row.sqm) * float(row.ask_psm)
    st.write(f"**{row.property_name}** — {row.sector} in {row.location}")
    st.write(f"Size: **{int(row.sqm):,} sqm** | Ask: **€{row.ask_psm:,.2f}/sqm** | Ticket: **€{ticket:,.0f}**")

    if ticket > cash:
        st.error("Insufficient cash to buy this asset.")
        return

    if st.button("Buy (whole asset)"):
        # Deduct cash, add holding at entry_psm = current ask
        set_team_cash(name, cash - ticket)
        add_holding(name, row.asset_id, float(row.ask_psm), get_week())
        st.success(f"Bought **{row.property_name}** for €{ticket:,.0f}.")

def sell_view():
    st.subheader("Sell Asset (whole-asset only) — Madrid")
    name = st.session_state.name.strip()
    if not name:
        st.warning("Enter your team name above.")
        return
    df = team_portfolio(name)
    if df.empty:
        st.caption("No holdings to sell.")
        return

    df["label"] = df.apply(lambda r: f"{r.asset_id} — {r.property_name} ({r.sector}, {r.location})", axis=1)
    label = st.selectbox("Select holding", options=df["label"].tolist())
    h = df[df["label"] == label].iloc[0]

    max_exit_this_week = round(float(h.entry_psm) * SALE_SOFT_CAP_FACTOR, 2)
    blocked, until_week = is_blocked(name, h.asset_id, get_week())
    if blocked:
        st.warning(f"Sale for this asset is blocked until week {until_week} due to exceeding last week's cap.")
        return

    st.write(f"**Max exit this week:** €{max_exit_this_week:,.2f}/sqm (7% over your entry €{h.entry_psm:,.2f}/sqm)")
    proposed_exit_psm = st.number_input(
        "Exit price (€/sqm)", min_value=0.0, value=float(max_exit_this_week), step=10.0,
        help="If you exceed the weekly cap, the sale is rejected and this asset is blocked until next week."
    )

    if proposed_exit_psm > max_exit_this_week:
        block_until_next_week(name, h.asset_id, get_week())
        st.error(
            f"Sale rejected: €{proposed_exit_psm:,.2f}/sqm exceeds this week's cap (€{max_exit_this_week:,.2f}/sqm). "
            f"You must wait until week {get_week()+1} to try again."
        )
        return

    if st.button("Sell (whole asset)"):
        proceeds = float(h.sqm) * proposed_exit_psm
        # Add cash, release asset back to market with new ask=exit price
        cash = get_team_cash(name)
        set_team_cash(name, cash + proceeds)
        c = conn(); cur = c.cursor()
        cur.execute("UPDATE assets SET ask_psm=? WHERE asset_id=?", (proposed_exit_psm, h.asset_id))
        c.commit(); c.close()
        remove_holding(name, h.asset_id)
        clear_block(name, h.asset_id)
        st.success(f"Sold **{h.property_name}** at €{proposed_exit_psm:,.2f}/sqm for **€{proceeds:,.0f}**.")

def week_controls():
    st.subheader("Week Controls")
    current = get_week()
    col1, col2 = st.columns(2)
    with col1:
        if st.button("End Week ➜"):
            if current < END_WEEK:
                new_w = current + 1
                set_week(new_w)
                apply_curveball_effects(new_w)
                st.success(f"Advanced to **Week {new_w}**.")
            else:
                st.info("You are at the final week.")
    with col2:
        if st.button("Reset My Session (UI only)"):
            # Does not touch DB; just resets local Streamlit state
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.success("Session state cleared. Reload the page.")

def info_view():
    st.subheader("Property Book — ROI Inputs")
    c = conn()
    df = pd.read_sql_query("""
        SELECT asset_id, property_name, sector, location, sqm,
               ask_psm, erv_psm, passing_psm, vacancy_pct, opex_psm, tax_psm
        FROM assets
        ORDER BY asset_id
    """, c)
    c.close()
    df["vacancy_%"] = (df["vacancy_pct"] * 100).round(0).astype(int)
    df = df.drop(columns=["vacancy_pct"])
    st.caption("Use these to calculate: Ticket, GPR, Effective Rent, Opex, Tax, NOI, ROI.")
    st.dataframe(df, use_container_width=True, hide_index=True)

def instructor_view():
    st.subheader("Instructor — Admin")
    pw = st.text_input("Admin password", type="password")
    if pw != ADMIN_PASS:
        st.info("Enter the admin password to access instructor tools.")
        return

    st.success("Instructor mode enabled.")
    # Show quick leaderboard (cash only; realized P&L proxy)
    c = conn()
    teams = pd.read_sql_query("SELECT name, cash FROM teams ORDER BY cash DESC", c)
    st.write("**Leaderboard (by cash on hand)**")
    if teams.empty:
        st.caption("No teams yet.")
    else:
        st.dataframe(teams, hide_index=True, use_container_width=True)

    st.divider()
    st.write("**Global Week:**", get_week())

    st.divider()
    st.write("⚠️ Reset tools")
    st.caption("Reset Entire Game wipes the database file and rebuilds everything (assets, week, teams, holdings). Use with caution.")

    if st.button("Reset Entire Game (wipe DB)"):
        try:
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
            init_db()
            seed_assets_once()
            patch_income_assumptions_once()
            apply_curveball_effects(START_WEEK)  # no-op but harmless
            st.success("Database wiped and rebuilt. Page will reflect a fresh game on reload.")
        except Exception as e:
            st.error(f"Failed to reset DB: {e}")

# ---------------------------
# Main app
# ---------------------------
def main():
    st.set_page_config(page_title="RE Portfolio Game — Madrid (50 Assets)", layout="wide")

    # Initialize persistence
    init_db()
    seed_assets_once()
    patch_income_assumptions_once()
    # Ensure we have a session-local team name field
    if "name" not in st.session_state:
        st.session_state.name = ""

    # Header
    header(st.session_state.name)

    # Tabs
    tabs = st.tabs(["Market", "Portfolio", "Buy", "Sell", "Info", "Week", "Instructor"])

    with tabs[0]:
        market_view()
    with tabs[1]:
        portfolio_view()
    with tabs[2]:
        buy_view()
    with tabs[3]:
        sell_view()
    with tabs[4]:
        info_view()
    with tabs[5]:
        week_controls()
    with tabs[6]:
        instructor_view()

    st.markdown("<br/><hr/><center>game by Innin Buyl — for exclusive use</center>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
