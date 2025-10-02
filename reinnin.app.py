# re_game_app.py
# Real Estate Portfolio Game — Madrid (50 fixed assets)
#
# Implements:
# - 50 hardcoded Madrid assets (no randomness; deterministic)
# - Initial cash: €36,000,000
# - Whole-asset trades only (no partial sqm)
# - Supply gating: an asset is unavailable while held; it returns to market only when sold
# - Sale soft cap: 7% above entry €/sqm; exceeding it rejects the sale and blocks the asset until next week
# - Weekly curveball announcements + numeric effects (W2, W4, W6, W7, W9, W12)
# - Portfolio includes 'property name'
# - Removed "ask the bot"; "Team name" -> "Name"
# - Instructor/Admin dashboard: set week, apply curveballs, see all teams' moves, holdings, leaderboard

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime
import os
import sqlite3

import pandas as pd
import streamlit as st

# ---------------------------
# Config
# ---------------------------
DB_PATH = "re_game.db"
INITIAL_CASH = 36_000_000.0  # €36m budget
SALE_SOFT_CAP_FACTOR = 1.07  # +7%
START_WEEK = 1
END_WEEK = 14
ADMIN_PASS = os.getenv("ADMIN_PASS", "1nn1n")

# ---------------------------
# Data models (for seeding)
# ---------------------------
@dataclass
class AssetSeed:
    asset_id: str
    property_name: str
    sector: str  # Residential, Office, Retail, Logistics
    location: str  # Madrid
    sqm: int
    ask_psm: float  # current market ask €/sqm
    erv_psm: float  # ERV €/sqm (income proxy)
    opex_psm: float # Opex €/sqm
    tax_psm: float  # IBI/taxes €/sqm

# ---------------------------
# Fixed catalog: 50 Madrid assets (seed)
# ---------------------------
# Notes on ranges used:
# Residential ask €/sqm ~ 3,200–6,000
# Office ~ 2,200–4,000
# Retail ~ 1,800–3,500
# Logistics ~ 1,400–2,400
# ERV is approx 4%–7% of ask; opex 0.4%–0.9%; tax 0.3%–0.6%
ASSETS_DATA: List[dict] = [
    # Residential (12)
    {"asset_id":"A001","property_name":"RES-MAD-SALAMAN-01","sector":"Residential","location":"Madrid","sqm":4200,"ask_psm":5200.0,"erv_psm":312.0,"opex_psm":31.2,"tax_psm":24.0},
    {"asset_id":"A002","property_name":"RES-MAD-CHAMART-02","sector":"Residential","location":"Madrid","sqm":3500,"ask_psm":4800.0,"erv_psm":264.0,"opex_psm":24.0,"tax_psm":21.6},
    {"asset_id":"A003","property_name":"RES-MAD-CHAMBER-03","sector":"Residential","location":"Madrid","sqm":3900,"ask_psm":5400.0,"erv_psm":291.6,"opex_psm":32.4,"tax_psm":27.0},
    {"asset_id":"A004","property_name":"RES-MAD-CENTRO-04","sector":"Residential","location":"Madrid","sqm":2800,"ask_psm":5600.0,"erv_psm":308.0,"opex_psm":33.6,"tax_psm":28.0},
    {"asset_id":"A005","property_name":"RES-MAD-RETIRO-05","sector":"Residential","location":"Madrid","sqm":3100,"ask_psm":5000.0,"erv_psm":285.0,"opex_psm":30.0,"tax_psm":25.0},
    {"asset_id":"A006","property_name":"RES-MAD-TETUAN-06","sector":"Residential","location":"Madrid","sqm":4600,"ask_psm":3900.0,"erv_psm":195.0,"opex_psm":19.5,"tax_psm":15.6},
    {"asset_id":"A007","property_name":"RES-MAD-ARGANZ-07","sector":"Residential","location":"Madrid","sqm":5200,"ask_psm":3600.0,"erv_psm":172.8,"opex_psm":18.0,"tax_psm":14.4},
    {"asset_id":"A008","property_name":"RES-MAD-MONCLO-08","sector":"Residential","location":"Madrid","sqm":2700,"ask_psm":4700.0,"erv_psm":246.8,"opex_psm":23.5,"tax_psm":18.8},
    {"asset_id":"A009","property_name":"RES-MAD-LATINA-09","sector":"Residential","location":"Madrid","sqm":6000,"ask_psm":3400.0,"erv_psm":163.2,"opex_psm":17.0,"tax_psm":13.6},
    {"asset_id":"A010","property_name":"RES-MAD-CARABN-10","sector":"Residential","location":"Madrid","sqm":4800,"ask_psm":3200.0,"erv_psm":156.8,"opex_psm":14.4,"tax_psm":12.8},
    {"asset_id":"A011","property_name":"RES-MAD-USERA-11","sector":"Residential","location":"Madrid","sqm":4200,"ask_psm":3300.0,"erv_psm":165.0,"ope_psm":14.9 if False else 14.9,"tax_psm":11.9},  # guard
    {"asset_id":"A012","property_name":"RES-MAD-VALLEC-12","sector":"Residential","location":"Madrid","sqm":3800,"ask_psm":3500.0,"erv_psm":175.0,"opex_psm":15.8,"tax_psm":12.6},

    # Office (12)
    {"asset_id":"A013","property_name":"OFF-MAD-SALAMAN-13","sector":"Office","location":"Madrid","sqm":7200,"ask_psm":3800.0,"erv_psm":209.0,"opex_psm":27.4,"tax_psm":19.0},
    {"asset_id":"A014","property_name":"OFF-MAD-CHAMART-14","sector":"Office","location":"Madrid","sqm":6500,"ask_psm":3600.0,"erv_psm":201.6,"opex_psm":25.2,"tax_psm":18.0},
    {"asset_id":"A015","property_name":"OFF-MAD-CHAMBER-15","sector":"Office","location":"Madrid","sqm":5400,"ask_psm":3400.0,"erv_psm":176.8,"opex_psm":22.1,"tax_psm":15.3},
    {"asset_id":"A016","property_name":"OFF-MAD-CENTRO-16","sector":"Office","location":"Madrid","sqm":4800,"ask_psm":4000.0,"erv_psm":220.0,"opex_psm":28.0,"tax_psm":22.0},
    {"asset_id":"A017","property_name":"OFF-MAD-RETIRO-17","sector":"Office","location":"Madrid","sqm":5100,"ask_psm":3500.0,"erv_psm":189.0,"opex_psm":21.9,"tax_psm":17.5},
    {"asset_id":"A018","property_name":"OFF-MAD-TETUAN-18","sector":"Office","location":"Madrid","sqm":6900,"ask_psm":3000.0,"erv_psm":162.0,"opex_psm":18.0,"tax_psm":14.4},
    {"asset_id":"A019","property_name":"OFF-MAD-ARGANZ-19","sector":"Office","location":"Madrid","sqm":5600,"ask_psm":2900.0,"erv_psm":156.6,"opex_psm":17.4,"tax_psm":13.1},
    {"asset_id":"A020","property_name":"OFF-MAD-MONCLO-20","sector":"Office","location":"Madrid","sqm":4300,"ask_psm":3200.0,"erv_psm":166.4,"opex_psm":20.5,"tax_psm":14.4},
    {"asset_id":"A021","property_name":"OFF-MAD-LATINA-21","sector":"Office","location":"Madrid","sqm":4700,"ask_psm":2800.0,"erv_psm":145.6,"opex_psm":17.4,"tax_psm":12.3},
    {"asset_id":"A022","property_name":"OFF-MAD-CARABN-22","sector":"Office","location":"Madrid","sqm":6200,"ask_psm":2600.0,"erv_psm":135.2,"opex_psm":16.1,"tax_psm":11.7},
    {"asset_id":"A023","property_name":"OFF-MAD-USERA-23","sector":"Office","location":"Madrid","sqm":5800,"ask_psm":2500.0,"erv_psm":127.5,"opex_psm":15.0,"tax_psm":11.3},
    {"asset_id":"A024","property_name":"OFF-MAD-VALLEC-24","sector":"Office","location":"Madrid","sqm":5000,"ask_psm":2400.0,"erv_psm":124.8,"opex_psm":14.4,"tax_psm":10.8},

    # Retail (13)
    {"asset_id":"A025","property_name":"RET-MAD-SALAMAN-25","sector":"Retail","location":"Madrid","sqm":3000,"ask_psm":3300.0,"erv_psm":181.5,"opex_psm":23.1,"tax_psm":16.5},
    {"asset_id":"A026","property_name":"RET-MAD-CHAMART-26","sector":"Retail","location":"Madrid","sqm":2600,"ask_psm":3200.0,"erv_psm":172.8,"opex_psm":21.1,"tax_psm":16.0},
    {"asset_id":"A027","property_name":"RET-MAD-CHAMBER-27","sector":"Retail","location":"Madrid","sqm":2400,"ask_psm":3100.0,"erv_psm":167.4,"opex_psm":22.3,"tax_psm":14.9},
    {"asset_id":"A028","property_name":"RET-MAD-CENTRO-28","sector":"Retail","location":"Madrid","sqm":2800,"ask_psm":3500.0,"erv_psm":210.0,"opex_psm":24.5,"tax_psm":19.3},
    {"asset_id":"A029","property_name":"RET-MAD-RETIRO-29","sector":"Retail","location":"Madrid","sqm":2200,"ask_psm":2900.0,"erv_psm":159.3,"opex_psm":20.3,"tax_psm":14.5},
    {"asset_id":"A030","property_name":"RET-MAD-TETUAN-30","sector":"Retail","location":"Madrid","sqm":3600,"ask_psm":2300.0,"erv_psm":115.0,"opex_psm":18.4,"tax_psm":11.5},
    {"asset_id":"A031","property_name":"RET-MAD-ARGANZ-31","sector":"Retail","location":"Madrid","sqm":4100,"ask_psm":2000.0,"erv_psm":100.0,"opex_psm":16.0,"tax_psm":10.0},
    {"asset_id":"A032","property_name":"RET-MAD-MONCLO-32","sector":"Retail","location":"Madrid","sqm":2700,"ask_psm":2700.0,"erv_psm":145.8,"opex_psm":18.9,"tax_psm":13.5},
    {"asset_id":"A033","property_name":"RET-MAD-LATINA-33","sector":"Retail","location":"Madrid","sqm":3900,"ask_psm":2100.0,"erv_psm":105.0,"opex_psm":16.8,"tax_psm":11.0},
    {"asset_id":"A034","property_name":"RET-MAD-CARABN-34","sector":"Retail","location":"Madrid","sqm":3200,"ask_psm":2400.0,"erv_psm":122.4,"opex_psm":19.2,"tax_psm":12.0},
    {"asset_id":"A035","property_name":"RET-MAD-USERA-35","sector":"Retail","location":"Madrid","sqm":3500,"ask_psm":2200.0,"erv_psm":118.8,"opex_psm":17.6,"tax_psm":11.0},
    {"asset_id":"A036","property_name":"RET-MAD-VALLEC-36","sector":"Retail","location":"Madrid","sqm":2300,"ask_psm":1800.0,"erv_psm":108.0,"opex_psm":14.4,"tax_psm":9.9},
    {"asset_id":"A037","property_name":"RET-MAD-MORATAL-37","sector":"Retail","location":"Madrid","sqm":2500,"ask_psm":2500.0,"erv_psm":137.5,"opex_psm":17.5,"tax_psm":12.5},

    # Logistics (13)
    {"asset_id":"A038","property_name":"LOG-MAD-VICLVAR-38","sector":"Logistics","location":"Madrid","sqm":9000,"ask_psm":2100.0,"erv_psm":115.5,"opex_psm":15.8,"tax_psm":12.6},
    {"asset_id":"A039","property_name":"LOG-MAD-SANBLA-39","sector":"Logistics","location":"Madrid","sqm":11000,"ask_psm":2000.0,"erv_psm":110.0,"opex_psm":15.0,"tax_psm":12.0},
    {"asset_id":"A040","property_name":"LOG-MAD-BARAJAS-40","sector":"Logistics","location":"Madrid","sqm":8000,"ask_psm":2200.0,"erv_psm":121.0,"opex_psm":16.5,"tax_psm":13.2},
    {"asset_id":"A041","property_name":"LOG-MAD-VALLEC-41","sector":"Logistics","location":"Madrid","sqm":10000,"ask_psm":1800.0,"erv_psm":102.6,"opex_psm":14.4,"tax_psm":10.8},
    {"asset_id":"A042","property_name":"LOG-MAD-VILLVER-42","sector":"Logistics","location":"Madrid","sqm":9500,"ask_psm":1750.0,"erv_psm":96.3,"opex_psm":14.0,"tax_psm":10.5},
    {"asset_id":"A043","property_name":"LOG-MAD-HORTALZ-43","sector":"Logistics","location":"Madrid","sqm":7800,"ask_psm":1950.0,"erv_psm":107.3,"opex_psm":15.6,"tax_psm":11.7},
    {"asset_id":"A044","property_name":"LOG-MAD-CIU-LIN-44","sector":"Logistics","location":"Madrid","sqm":8200,"ask_psm":1900.0,"erv_psm":104.5,"opex_psm":15.2,"tax_psm":11.4},
    {"asset_id":"A045","property_name":"LOG-MAD-USERA-45","sector":"Logistics","location":"Madrid","sqm":7000,"ask_psm":1700.0,"erv_psm":93.5,"opex_psm":13.6,"tax_psm":10.2},
    {"asset_id":"A046","property_name":"LOG-MAD-TETUAN-46","sector":"Logistics","location":"Madrid","sqm":7500,"ask_psm":1650.0,"erv_psm":90.8,"opex_psm":13.2,"tax_psm":9.9},
    {"asset_id":"A047","property_name":"LOG-MAD-ARGANZ-47","sector":"Logistics","location":"Madrid","sqm":8200,"ask_psm":1600.0,"erv_psm":88.0,"opex_psm":12.8,"tax_psm":9.6},
    {"asset_id":"A048","property_name":"LOG-MAD-MONCLO-48","sector":"Logistics","location":"Madrid","sqm":8800,"ask_psm":1850.0,"erv_psm":103.7,"opex_psm":14.8,"tax_psm":11.1},
    {"asset_id":"A049","property_name":"LOG-MAD-LATINA-49","sector":"Logistics","location":"Madrid","sqm":9600,"ask_psm":1500.0,"erv_psm":82.5,"opex_psm":12.0,"tax_psm":9.0},
    {"asset_id":"A050","property_name":"LOG-MAD-CARABN-50","sector":"Logistics","location":"Madrid","sqm":10400,"ask_psm":1450.0,"erv_psm":78.3,"opex_psm":11.6,"tax_psm":8.7},
]

# ---------------------------
# Curveballs (announcements + numeric effects)
# ---------------------------
CURVEBALLS: Dict[int, str] = {
    1: "+25 bps interest rate shock (financing more expensive)",
    2: "Retail softness: secondary retail ERVs drop",
    3: "LP mandate: >=20% allocation to Value-Add/Opportunistic by Week 4",
    4: "Bidding war: asking prices +7% pressure",
    5: "FX wobble: noise only; no direct P&L impact",
    6: "IBI/tax re-rate: NOI -3% pressure (modeled via +3% taxes)",
    7: "Tenant bankruptcy: vacancy risk spikes (effective ERV -20% for Office & Retail)",
    8: "Bank tightens: Max LTV 55% (if using debt module)",
    9: "Energy spike: opex +12% pressure",
    10: "PropTech incentive: retention +10% (narrative)",
    11: "Benchmarking bonus: top quartile rewarded (narrative)",
    12: "Residential demand: ERV growth +2% for Residential",
    13: "Green subsidy: 30% rebate on capex (narrative)",
    14: "Freeze & score: final valuations",
}

# ---------------------------
# SQLite helpers
# ---------------------------
def conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    c = conn(); cur = c.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS game_state (
        id INTEGER PRIMARY KEY CHECK (id=1),
        current_week INTEGER
    )""")
    cur.execute("INSERT OR IGNORE INTO game_state (id, current_week) VALUES (1, ?)", (START_WEEK,))

    cur.execute("""
    CREATE TABLE IF NOT EXISTS assets (
    # Add new columns (safe if they already exist)
ensure_col(c, "assets", "passing_psm REAL")
ensure_col(c, "assets", "vacancy_pct REAL")
        asset_id TEXT PRIMARY KEY,
        property_name TEXT,
        sector TEXT,
        location TEXT,
        sqm INTEGER,
        ask_psm REAL,
        erv_psm REAL,
        opex_psm REAL,
        tax_psm REAL,
        held_by_team_id INTEGER
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        team_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        cash REAL,
        created_at TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS holdings (
        holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        asset_id TEXT,
        entry_psm REAL,
        buy_week INTEGER,
        FOREIGN KEY(team_id) REFERENCES teams(team_id),
        FOREIGN KEY(asset_id) REFERENCES assets(asset_id)
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sale_blocks (
        team_id INTEGER,
        asset_id TEXT,
        blocked_until_week INTEGER,
        PRIMARY KEY (team_id, asset_id)
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        week INTEGER,
        asset_id TEXT,
        action TEXT,       -- BUY, SELL, REJECTED
        amount REAL,       -- cash +/- ; for SELL = proceeds ; for BUY = -ticket
        price_psm REAL,
        created_at TEXT,
        note TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS curveballs_applied (
        week INTEGER PRIMARY KEY
    )""")
    c.commit()
    c.close()

def seed_assets_once():
    c = conn(); cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM assets")
    n = cur.fetchone()[0]
    if n == 0:
        for row in ASSETS_DATA:
            # Fix a typo in seed row A011 (ensure opex_psm key exists)
            if row["asset_id"] == "A011" and "opex_psm" not in row:
                row["opex_psm"] = 14.9
            cur.execute("""
            INSERT INTO assets (asset_id,property_name,sector,location,sqm,ask_psm,erv_psm,opex_psm,tax_psm,held_by_team_id)
            VALUES (?,?,?,?,?,?,?,?,?,NULL)
            """, (
                row["asset_id"], row["property_name"], row["sector"], row["location"], row["sqm"],
                row["ask_psm"], row["erv_psm"], row["opex_psm"], row["tax_psm"]
            ))
        c.commit()
    c.close()

def get_week() -> int:
    c = conn()
    wk = c.execute("SELECT current_week FROM game_state WHERE id=1").fetchone()[0]
    c.close()
    return int(wk)

def set_week(week: int):
    c = conn(); c.execute("UPDATE game_state SET current_week=? WHERE id=1", (int(week),)); c.commit(); c.close()

def get_or_create_team(name: str) -> dict:
    name = name.strip()
    if not name:
        raise ValueError("Name required")
    c = conn(); cur = c.cursor()
    row = cur.execute("SELECT team_id, name, cash FROM teams WHERE name=?", (name,)).fetchone()
    if row is None:
        cur.execute("INSERT INTO teams (name,cash,created_at) VALUES (?,?,?)",
                    (name, INITIAL_CASH, datetime.utcnow().isoformat()))
        c.commit()
        row = cur.execute("SELECT team_id, name, cash FROM teams WHERE name=?", (name,)).fetchone()
    c.close()
    return {"team_id": row[0], "name": row[1], "cash": float(row[2])}

def update_cash(team_id: int, delta: float):
    c = conn(); c.execute("UPDATE teams SET cash=cash+? WHERE team_id=?", (float(delta), int(team_id))); c.commit(); c.close()

def list_market() -> pd.DataFrame:
    c = conn()
    df = pd.read_sql_query("""
        SELECT asset_id, property_name, sector, location, sqm, ask_psm,
               CASE WHEN held_by_team_id IS NULL THEN 'Yes' ELSE 'Held' END AS available
        FROM assets
        ORDER BY asset_id
    """, c)
    c.close()
    return df

def list_portfolio(team_id: int) -> pd.DataFrame:
    c = conn()
    df = pd.read_sql_query("""
        SELECT h.asset_id, a.property_name, a.sector, a.location, a.sqm,
               h.entry_psm, h.buy_week
        FROM holdings h JOIN assets a ON a.asset_id=h.asset_id
        WHERE h.team_id=? 
        ORDER BY h.buy_week, h.asset_id
    """, c, params=(team_id,))
    c.close()
    return df

def get_asset(asset_id: str) -> Optional[dict]:
    c = conn()
    row = c.execute("""
        SELECT asset_id, property_name, sector, location, sqm, ask_psm, erv_psm, opex_psm, tax_psm, held_by_team_id
        FROM assets WHERE asset_id=?
    """, (asset_id,)).fetchone()
    c.close()
    if row:
        keys = ["asset_id","property_name","sector","location","sqm","ask_psm","erv_psm","opex_psm","tax_psm","held_by_team_id"]
        return dict(zip(keys, row))
    return None

def holding_row(team_id: int, asset_id: str) -> Optional[dict]:
    c = conn()
    row = c.execute("""
        SELECT holding_id, team_id, asset_id, entry_psm, buy_week
        FROM holdings WHERE team_id=? AND asset_id=?
    """, (team_id, asset_id)).fetchone()
    c.close()
    if row:
        return {"holding_id": row[0], "team_id": row[1], "asset_id": row[2], "entry_psm": row[3], "buy_week": row[4]}
    return None

def is_blocked(team_id: int, asset_id: str) -> Optional[int]:
    c = conn()
    row = c.execute("SELECT blocked_until_week FROM sale_blocks WHERE team_id=? AND asset_id=?", (team_id, asset_id)).fetchone()
    c.close()
    return None if row is None else int(row[0])

def block_until_next_week(team_id: int, asset_id: str):
    wk = get_week() + 1
    c = conn()
    c.execute("""
        INSERT INTO sale_blocks (team_id, asset_id, blocked_until_week)
        VALUES (?,?,?)
        ON CONFLICT(team_id,asset_id) DO UPDATE SET blocked_until_week=excluded.blocked_until_week
    """, (team_id, asset_id, wk))
    c.commit(); c.close()

def clear_block(team_id: int, asset_id: str):
    c = conn(); c.execute("DELETE FROM sale_blocks WHERE team_id=? AND asset_id=?", (team_id, asset_id)); c.commit(); c.close()

def log_tx(team_id: int, asset_id: str, action: str, amount: float, price_psm: float, note: str):
    c = conn()
    c.execute("""
        INSERT INTO transactions (team_id, week, asset_id, action, amount, price_psm, created_at, note)
        VALUES (?,?,?,?,?,?,?,?)
    """, (team_id, get_week(), asset_id, action, amount, price_psm, datetime.utcnow().isoformat(), note))
    c.commit(); c.close()

# ---------------------------
# Curveball effects (apply once globally per week)
# ---------------------------
def apply_curveball_effects(week: int):
    c = conn(); cur = c.cursor()
    if cur.execute("SELECT 1 FROM curveballs_applied WHERE week=?", (week,)).fetchone():
        c.close(); return  # already applied

    # W2: Retail softness — ERV -5% for Retail
    if week == 2:
        cur.execute("UPDATE assets SET erv_psm = ROUND(erv_psm * 0.95, 2) WHERE sector='Retail'")
    # W4: Bidding war — asking +7% for assets currently in market (not held)
    elif week == 4:
        cur.execute("UPDATE assets SET ask_psm = ROUND(ask_psm * 1.07, 2) WHERE held_by_team_id IS NULL")
    # W6: IBI/tax re-rate — taxes +3% for all
    elif week == 6:
        cur.execute("UPDATE assets SET tax_psm = ROUND(tax_psm * 1.03, 2)")
    # W7: Tenant bankruptcy shock — effective ERV -20% for Office & Retail
    elif week == 7:
        cur.execute("UPDATE assets SET erv_psm = ROUND(erv_psm * 0.80, 2) WHERE sector IN ('Office','Retail')")
    # W9: Energy spike — opex +12% for all
    elif week == 9:
        cur.execute("UPDATE assets SET opex_psm = ROUND(opex_psm * 1.12, 2)")
    # W12: Residential demand — ERV +2% for Residential
    elif week == 12:
        cur.execute("UPDATE assets SET erv_psm = ROUND(erv_psm * 1.02, 2) WHERE sector='Residential'")

    cur.execute("INSERT INTO curveballs_applied (week) VALUES (?)", (week,))
    c.commit(); c.close()

# ---------------------------
# Trading actions
# ---------------------------
def buy_asset(team_id: int, asset_id: str):
    a = get_asset(asset_id)
    if not a:
        st.error("Asset not found."); return
    if a["held_by_team_id"] is not None:
        st.error("This asset is currently held by another team."); return

    # Whole-asset ticket
    ticket = a["sqm"] * a["ask_psm"]

    # Check cash
    team = get_or_create_team(st.session_state["__team_name"])
    if ticket > team["cash"]:
        st.error("Insufficient cash."); return

    c = conn(); cur = c.cursor()
    # Deduct cash, create holding, mark asset as held
    cur.execute("UPDATE teams SET cash=cash-? WHERE team_id=?", (ticket, team_id))
    cur.execute("""
        INSERT INTO holdings (team_id, asset_id, entry_psm, buy_week) VALUES (?,?,?,?)
    """, (team_id, asset_id, a["ask_psm"], get_week()))
    cur.execute("UPDATE assets SET held_by_team_id=? WHERE asset_id=?", (team_id, asset_id))
    c.commit(); c.close()

    log_tx(team_id, asset_id, "BUY", -ticket, a["ask_psm"], f"Buy whole asset {asset_id} @ €{a['ask_psm']:.2f}/sqm")
    st.success(f"Bought **{a['property_name']}** for €{ticket:,.0f}.")

def sell_asset(team_id: int, asset_id: str, exit_psm: float):
    h = holding_row(team_id, asset_id)
    if not h:
        st.error("You do not hold this asset."); return

    blocked_until = is_blocked(team_id, asset_id)
    wk = get_week()
    if blocked_until is not None and wk < blocked_until:
        st.warning(f"Sale is blocked until Week {blocked_until}."); return

    # Enforce sale soft cap
    max_exit = round(h["entry_psm"] * SALE_SOFT_CAP_FACTOR, 2)
    if exit_psm > max_exit:
        # REJECT and block until next week
        block_until_next_week(team_id, asset_id)
        log_tx(team_id, asset_id, "REJECTED", 0.0, float(exit_psm),
               f"Rejected: asked €{exit_psm:.2f}/sqm > cap €{max_exit:.2f}/sqm; blocked until Week {wk+1}")
        st.error(f"Sale rejected: cap this week is €{max_exit:,.2f}/sqm. Asset blocked until Week {wk+1}.")
        return

    # Process sale
    a = get_asset(asset_id)
    proceeds = a["sqm"] * exit_psm

    c = conn(); cur = c.cursor()
    cur.execute("UPDATE teams SET cash=cash+? WHERE team_id=?", (proceeds, team_id))
    cur.execute("DELETE FROM holdings WHERE team_id=? AND asset_id=?", (team_id, asset_id))
    cur.execute("UPDATE assets SET held_by_team_id=NULL, ask_psm=? WHERE asset_id=?", (exit_psm, asset_id))
    cur.execute("DELETE FROM sale_blocks WHERE team_id=? AND asset_id=?", (team_id, asset_id))
    c.commit(); c.close()

    log_tx(team_id, asset_id, "SELL", proceeds, float(exit_psm),
           f"Sell whole asset {asset_id} @ €{exit_psm:.2f}/sqm")
    st.success(f"Sold **{a['property_name']}** @ €{exit_psm:,.2f}/sqm for **€{proceeds:,.0f}**.")

# ---------------------------
# KPIs / Leaderboard
# ---------------------------
def team_kpis(team_id: int) -> dict:
    c = conn()
    cash = c.execute("SELECT cash FROM teams WHERE team_id=?", (team_id,)).fetchone()
    cash = float(cash[0]) if cash else 0.0
    rows = c.execute("""
        SELECT a.sqm, a.ask_psm
        FROM holdings h JOIN assets a ON a.asset_id=h.asset_id
        WHERE h.team_id=?
    """, (team_id,)).fetchall()
    c.close()
    m2m_value = sum(r[0]*r[1] for r in rows)
    portfolio_value = cash + m2m_value
    return {"cash": cash, "m2m_assets": m2m_value, "portfolio_value": portfolio_value}

def compute_leaderboard() -> pd.DataFrame:
    c = conn()
    teams = pd.read_sql_query("SELECT team_id, name, cash FROM teams ORDER BY name", c)
    c.close()
    rows = []
    for _, t in teams.iterrows():
        k = team_kpis(int(t.team_id))
        rows.append({
            "name": t.name,
            "portfolio_value": k["portfolio_value"],
            "cash": k["cash"],
            "mark_to_market_assets": k["m2m_assets"],
        })
    if not rows:
        return pd.DataFrame(columns=["name","portfolio_value","cash","mark_to_market_assets"])
    df = pd.DataFrame(rows).sort_values("portfolio_value", ascending=False)
    return df

def all_holdings_df() -> pd.DataFrame:
    c = conn()
    df = pd.read_sql_query("""
        SELECT t.name, a.asset_id, a.property_name, a.sector, a.location, a.sqm,
               h.entry_psm, h.buy_week
        FROM holdings h
        JOIN teams t ON t.team_id=h.team_id
        JOIN assets a ON a.asset_id=h.asset_id
        ORDER BY t.name, h.buy_week
    """, c)
    c.close()
    return df

def tx_df(limit: int = 300) -> pd.DataFrame:
    c = conn()
    df = pd.read_sql_query("""
        SELECT datetime(created_at) as ts, week, 
               (SELECT name FROM teams WHERE teams.team_id=transactions.team_id) as name,
               asset_id, action, amount, price_psm, note
        FROM transactions
        ORDER BY tx_id DESC
        LIMIT ?
    """, c, params=(limit,))
    c.close()
    return df

# ---------------------------
# UI components
# ---------------------------
def header():
    st.title("🏢 Real Estate Portfolio Game — Madrid (50 Assets)")
    st.caption("game by Innin Buyl — for exclusive use")

    col1, col2, col3 = st.columns([1.8, 1, 1])
    with col1:
        st.text_input("Name", key="__team_name", placeholder="Team / Player", value=st.session_state.get("__team_name",""))
    with col2:
        st.metric("Week", get_week())
    with col3:
        # show current team cash if set
        name = st.session_state.get("__team_name","").strip()
        if name:
            t = get_or_create_team(name)
            st.metric("Cash (€)", f"{t['cash']:,.0f}")
        else:
            st.metric("Cash (€)", "—")

    # Weekly announcement banner
    cb = CURVEBALLS.get(get_week())
    if cb:
        st.info(f"**This week’s curveball:** {cb}")

def market_view():
    st.subheader("Market — Madrid")
    df = list_market()
    st.dataframe(df, use_container_width=True, hide_index=True)

def portfolio_view():
    st.subheader("Portfolio (Open Positions)")
    name = st.session_state.get("__team_name","").strip()
    if not name:
        st.warning("Enter your Name above to load your portfolio.")
        return
    team = get_or_create_team(name)
    df = list_portfolio(team["team_id"])
    if df.empty:
        st.caption("No holdings yet.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)
    # KPIs
    k = team_kpis(team["team_id"])
    c1, c2, c3 = st.columns(3)
    c1.metric("Portfolio Value", f"€{k['portfolio_value']:,.0f}")
    c2.metric("Cash", f"€{k['cash']:,.0f}")
    c3.metric("M2M Assets", f"€{k['m2m_assets']:,.0f}")

def buy_view():
    st.subheader("Buy Asset (whole-asset only)")
    name = st.session_state.get("__team_name","").strip()
    if not name:
        st.warning("Enter your Name above first."); return
    team = get_or_create_team(name)

    # Available assets only (not held by any team)
    c = conn()
    df = pd.read_sql_query("""
        SELECT asset_id, property_name, sector, location, sqm, ask_psm
        FROM assets WHERE held_by_team_id IS NULL
        ORDER BY asset_id
    """, c)
    c.close()
    if df.empty:
        st.warning("No assets are available right now. Sell something to release supply.")
        return

    options = [f"{r.asset_id} — {r.property_name} ({r.sector}, {r.location}) — {int(r.sqm)} sqm @ €{r.ask_psm:,.2f}/sqm"
               for _, r in df.iterrows()]
    sel = st.selectbox("Select asset", options=options)
    asset_id = sel.split(" — ")[0]
    a = get_asset(asset_id)
    ticket = a["sqm"] * a["ask_psm"]

    st.write(f"**{a['property_name']}** — Size: **{a['sqm']:,} sqm** | Ask: **€{a['ask_psm']:,.2f}/sqm** | Ticket: **€{ticket:,.0f}**")
    if st.button("Buy (whole asset)"):
        if ticket > get_or_create_team(name)["cash"]:
            st.error("Insufficient cash.")
        else:
            buy_asset(team["team_id"], asset_id)

def sell_view():
    st.subheader("Sell Asset (whole-asset only)")
    name = st.session_state.get("__team_name","").strip()
    if not name:
        st.warning("Enter your Name above first."); return
    team = get_or_create_team(name)

    df = list_portfolio(team["team_id"])
    if df.empty:
        st.caption("No holdings to sell."); return

    options = [f"{r.asset_id} — {r.property_name} ({r.sector}, {r.location}) — entry €{r.entry_psm:,.2f}/sqm"
               for _, r in df.iterrows()]
    sel = st.selectbox("Select holding", options=options)
    asset_id = sel.split(" — ")[0]

    h = holding_row(team["team_id"], asset_id)
    if not h:
        st.error("Holding not found."); return

    max_exit = round(h["entry_psm"] * SALE_SOFT_CAP_FACTOR, 2)
    blocked_until = is_blocked(team["team_id"], asset_id)
    wk = get_week()
    if blocked_until is not None and wk < blocked_until:
        st.warning(f"Sale blocked until Week {blocked_until} due to last week's rejected attempt.")
        return

    st.info(f"**Max exit this week:** €{max_exit:,.2f}/sqm (7% over your entry €{h['entry_psm']:,.2f}/sqm)")
    exit_psm = st.number_input("Exit price (€/sqm)", min_value=0.0, value=float(max_exit), step=10.0)

    if st.button("Sell (whole asset)"):
        sell_asset(team["team_id"], asset_id, float(exit_psm))

def instructor_view():
    st.subheader("Instructor / Admin")
    pwd = st.text_input("Admin password", type="password")
    if pwd != ADMIN_PASS:
        st.info("Enter the admin password to manage week and view all teams.")
        return

    st.success("Admin verified")
    wk = get_week()
    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        new_wk = st.number_input("Set current week", min_value=START_WEEK, max_value=END_WEEK, value=wk)
        if st.button("Update Week"):
            set_week(int(new_wk))
            apply_curveball_effects(int(new_wk))
            st.success(f"Week set to {int(new_wk)} and curveball applied (if any).")

    with col2:
        if st.button("Advance ➜ Next Week"):
            if wk < END_WEEK:
                set_week(wk+1)
                apply_curveball_effects(wk+1)
                st.success(f"Advanced to Week {wk+1}.")
            else:
                st.info("Already at final week.")

    with col3:
        if st.button("Re-apply curveball for current week"):
            apply_curveball_effects(get_week())
            st.warning("Curveball re-applied (if not already).")

    st.markdown("### 📊 Leaderboard (Mark-to-Market)")
    st.dataframe(compute_leaderboard(), use_container_width=True, hide_index=True)

    st.markdown("### 🏷️ All Open Holdings")
    st.dataframe(all_holdings_df(), use_container_width=True, hide_index=True)

    st.markdown("### 🧾 Recent Transactions")
    st.dataframe(tx_df(400), use_container_width=True, hide_index=True)

    st.markdown("### 🛒 Market (Availability)")
    st.dataframe(list_market(), use_container_width=True, hide_index=True)

def classify_profile(location: str, sector: str) -> str:
    prime = {"Salamanca","Chamberí","Centro","Chamartín","Retiro","Moncloa","Barajas"}
    if location in prime:
        return "Core"
    if sector == "Logistics":
        return "Core+"
    return "Value-Add"

def profile_factors(profile: str):
    """
    Returns (passing_rent_factor, vacancy_pct).
    passing_rent_factor is applied to ERV €/sqm/month.
    vacancy_pct is between 0 and 1.
    """
    if profile == "Core":
        return 1.00, 0.04
    if profile == "Core+":
        return 0.92, 0.12
    return 0.85, 0.25

def patch_income_assumptions_once():
    """
    If passing_psm or vacancy_pct is missing, fill them using our profile rules.
    Safe to run multiple times.
    """
    c = conn(); cur = c.cursor()
    rows = cur.execute("SELECT asset_id, sector, location, erv_psm, passing_psm, vacancy_pct FROM assets").fetchall()
    updated = 0
    for asset_id, sector, location, erv_psm, passing_psm, vacancy_pct in rows:
        if passing_psm is None or vacancy_pct is None:
            prof = classify_profile(location, sector)
            pass_factor, vac = profile_factors(prof)
            new_passing = round(float(erv_psm) * pass_factor, 2)
            cur.execute(
                "UPDATE assets SET passing_psm=?, vacancy_pct=? WHERE asset_id=?",
                (new_passing, vac, asset_id)
            )
            updated += 1
    if updated:
        c.commit()
    c.close()

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
    # Make vacancy easy to read
    df["vacancy_%"] = (df["vacancy_pct"]*100).round(0).astype(int)
    df = df.drop(columns=["vacancy_pct"])
    st.caption("Inputs for ROI: Ticket = ask_psm×sqm; Effective Gross Rent uses passing €/sqm/mo and vacancy.")
    st.dataframe(df, use_container_width=True, hide_index=True)

# ---------------------------
# App entry
# ---------------------------
def main():
    st.set_page_config(page_title="RE Portfolio Game — Madrid (50 Assets)", layout="wide")
    init_db()
    seed_assets_once()
    patch_income_assumptions_once()

    header()
    tabs = st.tabs(["Market", "Portfolio", "Buy", "Sell", "Instructor"])

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
        instructor_view()
        

if __name__ == "__main__":
    main()

