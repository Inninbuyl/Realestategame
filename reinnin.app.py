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
    # Weekly snapshots (locked scores)
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

ASSETS = [
    ("RES-MAD-001","Salamanca Prime Apt","Residential","Prime Apartment","Salamanca",110,2010,"Core",0.0,28.0,27.0,9.0*12,0.01,120.0,6500.0,"Stabilized"),
    ("RES-MAD-002","Chamberí Rental Block (12u)","Residential","Multi-let","Chamberí",900,1968,"Value-Add",15.0,22.0,18.5,7.0*12,0.04,250.0,5200.0,"Leased"),
    ("RES-MAD-003","Arganzuela Starter Units (20u)","Residential","BTR","Arganzuela",1600,2002,"Core+",8.0,18.0,16.0,6.0*12,0.02,150.0,4300.0,"Leased"),
    ("OFF-MAD-001","CBD Castellana Office","Office","CBD","Chamartín",5200,1999,"Core",5.0,42.0,40.0,12.0*12,0.015,220.0,7200.0,"Leased"),
    ("OFF-MAD-002","Decentralized Office (M-30)","Office","Periphery","Ciudad Lineal",7800,1985,"Value-Add",25.0,25.0,21.0,10.0*12,0.05,350.0,3500.0,"Partly Vacant"),
    ("RET-MAD-001","Gran Vía Flagship Unit","Retail","High Street","Centro",950,2005,"Core",0.0,145.0,140.0,18.0*12,0.01,180.0,18000.0,"Leased"),
    ("RET-MAD-002","Barrio Secondary Retail","Retail","Secondary","Tetuán",600,1990,"Opportunistic",40.0,20.0,12.0,8.0*12,0.07,300.0,2800.0,"Vacant/Leased mix"),
    ("LOG-MAD-001","Getafe Logistics Box","Logistics","Big Box","Getafe",15400,2016,"Core+",0.0,6.1,6.0,3.5*12,0.01,80.0,980.0,"Leased"),
    ("LOG-MAD-002","Alcalá Last-mile","Logistics","Last Mile","Alcalá de Henares",6200,2008,"Value-Add",20.0,6.5,5.5,3.0*12,0.03,120.0,900.0,"Partly Vacant"),
    ("DEV-MAD-001","Valdebebas Residential Plot","Development","Resi Plot","Hortaleza",4500,None,"Land",100.0,None,0.0,0.0,0.0,0.0,400.0,"Land Bank"),
    ("DEV-MAD-002","Usera Office-to-Resi","Development","Conversion","Usera",5200,1980,"Opportunistic",70.0,None,6.0,9.0*12,0.10,600.0,1800.0,"Conversion Candidate"),
]

ASSUMPTIONS = [
    ("Inflation_%", 2.0, "CPI baseline"),
    ("Rent_Growth_Base_%", 3.0, "Annual rent growth base"),
    ("Opex_Growth_%", 2.0, "Annual opex growth"),
    ("Exit_Yield_Residential_%", 4.25, "Exit cap"),
    ("Exit_Yield_Office_%", 4.70, "Exit cap"),
    ("Exit_Yield_Retail_%", 4.50, "Exit cap"),
    ("Exit_Yield_Logistics_%", 5.25, "Exit cap"),
    ("Euribor12M_%", 2.2, "12M Euribor base"),
    ("Loan_Spread_bp", 180.0, "bps over Euribor"),
    ("Max_LTV_%", 60.0, "Max LTV"),
]

CURVEBALLS = [
    (1, "Kickoff: +25bp rates", "Euribor12M_% += 0.25"),
    (2, "Retail softness", "Retail secondary ERV -5%"),
    (3, "LP pushes opportunistic", ">=20% VA/Opp by W4"),
    (4, "Bidding war", "+7% on asks"),
    (5, "FX wobble", "Info only"),
    (6, "IBI/tax re-rate", "NOI -3%"),
    (7, "Tenant bankruptcy", "+20% vacancy"),
    (8, "Bank tightens", "Max LTV 55%"),
    (9, "Energy spike", "+12% opex"),
    (10, "PropTech incentive", "+10% retention"),
    (11, "Benchmarking bonus", "Top quartile bonus"),
    (12, "Resi demand +2% ERV growth", "+2% resi ERV"),
    (13, "Green subsidy", "30% capex rebate"),
    (14, "Final valuation", "Freeze & score"),
]

def seed_data():
    conn = get_conn()
    cur = conn.cursor()
    for row in ASSETS:
        cur.execute("""
        INSERT OR IGNORE INTO assets (asset_id,name,sector,subsector,district,size_sqm,year,condition,vacancy,erv_psm_pm,rent_psm_pm,opex_psm_pa,capex_y1_pct,exp_capex_5y_psm,price_psm,status)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, row)
    for key, val, note in ASSUMPTIONS:
        cur.execute("INSERT OR IGNORE INTO assumptions (key,value,note) VALUES (?,?,?)", (key, val, note))
    for row in CURVEBALLS:
        cur.execute("INSERT OR IGNORE INTO curveballs (week,title,rule) VALUES (?,?,?)", row)
    conn.commit(); conn.close()

# -------------------------
# Helpers & queries
# -------------------------

def get_assumption(key: str) -> float:
    conn = get_conn()
    df = pd.read_sql_query("SELECT value FROM assumptions WHERE key=?", conn, params=(key,))
    conn.close()
    return float(df.iloc[0,0]) if len(df) else np.nan

def set_assumption(key: str, value: float):
    conn = get_conn(); conn.execute("UPDATE assumptions SET value=? WHERE key=?", (value, key)); conn.commit(); conn.close()

def get_game_week() -> int:
    conn = get_conn(); df = pd.read_sql_query("SELECT current_week FROM game_state WHERE id=1", conn); conn.close(); return int(df.iloc[0,0])

def set_game_week(week: int):
    conn = get_conn(); conn.execute("UPDATE game_state SET current_week=? WHERE id=1", (week,)); conn.commit(); conn.close()

def get_max_ltv() -> float:
    conn = get_conn(); df = pd.read_sql_query("SELECT max_ltv FROM game_state WHERE id=1", conn); conn.close(); return float(df.iloc[0,0])

def set_max_ltv(v: float):
    conn = get_conn(); conn.execute("UPDATE game_state SET max_ltv=? WHERE id=1", (v,)); conn.commit(); conn.close()

def list_assets() -> pd.DataFrame:
    conn = get_conn(); df = pd.read_sql_query("SELECT * FROM assets", conn); conn.close(); return df

def get_team(team_name: str) -> Dict[str, Any]:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM teams WHERE team_name=?", conn, params=(team_name,))
    if len(df) == 0:
        conn.execute("INSERT INTO teams (team_name, cash, created_at) VALUES (?,?,?)", (team_name, 50000000.0, datetime.utcnow().isoformat()))
        conn.commit()
        df = pd.read_sql_query("SELECT * FROM teams WHERE team_name=?", conn, params=(team_name,))
    conn.close()
    return df.iloc[0].to_dict()

def list_team_positions(team_id: int) -> pd.DataFrame:
    conn = get_conn(); df = pd.read_sql_query("SELECT * FROM positions WHERE team_id=? AND closed_week IS NULL", conn, params=(team_id,)); conn.close(); return df

def list_all_positions() -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT p.*, t.team_name
        FROM positions p JOIN teams t ON p.team_id=t.team_id
        WHERE p.closed_week IS NULL
        ORDER BY t.team_name, p.opened_week
        """, conn)
    conn.close(); return df

def list_transactions(limit: int = 200) -> pd.DataFrame:
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM transactions ORDER BY created_at DESC LIMIT ?", conn, params=(limit,))
    conn.close(); return df

# -------------------------
# Market valuation band
# -------------------------

def get_market_price_band(asset_id: str) -> Dict[str, float]:
    """Compute a market exit €/sqm band from NOI/cap and curveballs."""
    conn = get_conn()
    a = pd.read_sql_query("SELECT * FROM assets WHERE asset_id=?", conn, params=(asset_id,)).iloc[0]
    conn.close()
    y_map = {
        "Residential": get_assumption("Exit_Yield_Residential_%")/100.0,
        "Office": get_assumption("Exit_Yield_Office_%")/100.0,
        "Retail": get_assumption("Exit_Yield_Retail_%")/100.0,
        "Logistics": get_assumption("Exit_Yield_Logistics_%")/100.0,
        "Development": 0.08,  # placeholder for dev (yield on cost normally)
    }
    cap = y_map.get(a["sector"], 0.05)
    occ = 1.0 - float(a["vacancy"]) / 100.0
    rent = (a["rent_psm_pm"] if pd.notnull(a["rent_psm_pm"]) else (a["erv_psm_pm"] or 0.0))
    ibicut = 0.03 if get_game_week() >= 6 else 0.0  # week 6 tax hit
    noi_psm = max(occ * (rent * 12.0) - a["opex_psm_pa"], 0.0) * (1.0 - ibicut)
    fair_psm = noi_psm / cap if cap > 0 else float(a["price_psm"])
    # curveball adjustments
    week = get_game_week()
    mult = 1.0
    if week >= 2 and a["sector"] == "Retail" and a["subsector"] == "Secondary":
        mult *= 0.95
    if week >= 4:
        mult *= 1.07
    if week >= 12 and a["sector"] == "Residential":
        mult *= 1.02
    lower = fair_psm * 0.90 * mult
    upper = fair_psm * 1.10 * mult
    ask = float(a["price_psm"]) if pd.notnull(a["price_psm"]) else fair_psm
    hard_low = min(ask, fair_psm) * 0.5
    hard_up = max(ask, fair_psm) * 1.5
    return {"lower": max(lower, hard_low), "upper": min(upper, hard_up), "fair": fair_psm}

# -------------------------
# Trading
# -------------------------

def buy_asset(team_id: int, asset_id: str, size_sqm: float, price_psm: float, acq_cost_pct: float, ltv_pct: float):
    if ltv_pct > get_max_ltv():
        st.error(f"Max LTV exceeded. Max: {get_max_ltv()*100:.0f}%"); return
    gross = size_sqm * price_psm
    acq_costs = gross * acq_cost_pct
    debt_draw = gross * ltv_pct
    equity = gross + acq_costs - debt_draw
    conn = get_conn(); cur = conn.cursor()
    cash = pd.read_sql_query("SELECT cash FROM teams WHERE team_id=?", conn, params=(team_id,)).iloc[0,0]
    if equity > cash:
        st.error("Insufficient cash."); conn.close(); return
    week = get_game_week()
    cur.execute("""
        INSERT INTO positions (team_id, asset_id, size_sqm, price_psm, acq_cost_pct, ltv_pct, debt_draw, equity, opened_week)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (team_id, asset_id, size_sqm, price_psm, acq_cost_pct, ltv_pct, debt_draw, equity, week))
    cur.execute("UPDATE teams SET cash=cash-? WHERE team_id=?", (equity, team_id))
    cur.execute("INSERT INTO transactions (team_id, week, asset_id, kind, cashflow, description, created_at) VALUES (?,?,?,?,?,?,?)",
                (team_id, week, asset_id, "BUY", -equity, f"Buy {asset_id} {size_sqm} sqm @ {price_psm}/sqm", datetime.utcnow().isoformat()))
    conn.commit(); conn.close(); st.success("Asset purchased.")

def sell_position(position_id: int, exit_price_psm: float):
    conn = get_conn()
    cur = conn.cursor()
    pos = pd.read_sql_query("SELECT * FROM positions WHERE position_id=?", conn, params=(position_id,)).iloc[0]
    week = get_game_week()
    band = get_market_price_band(pos["asset_id"])
    if exit_price_psm < band["lower"] or exit_price_psm > band["upper"]:
        adj = min(max(exit_price_psm, band["lower"]), band["upper"])
        st.warning(f"Exit price adjusted to market band: {adj:,.0f} €/sqm (allowed {band['lower']:,.0f}–{band['upper']:,.0f}; fair ~ {band['fair']:,.0f})")
        exit_price_psm = adj
    gross = pos["size_sqm"] * exit_price_psm
    costs = gross * 0.02  # selling costs
    proceeds = gross - costs
    equity_back = proceeds - pos["debt_draw"]
    cur.execute("UPDATE teams SET cash=cash+? WHERE team_id=?", (equity_back, pos["team_id"]))
    cur.execute("UPDATE positions SET closed_week=? WHERE position_id=?", (week, position_id))
    cur.execute("INSERT INTO transactions (team_id, week, asset_id, kind, cashflow, description, created_at) VALUES (?,?,?,?,?,?,?)",
                (pos["team_id"], week, pos["asset_id"], "SELL", equity_back, f"Sell {pos['asset_id']} @ {exit_price_psm}/sqm", datetime.utcnow().isoformat()))
    conn.commit(); conn.close(); st.success("Position sold.")

# -------------------------
# KPIs & Leaderboard
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
    total_value = total_debt = total_noi = 0.0
    for _, pos in positions.iterrows():
        a = assets.loc[pos["asset_id"]]
        occ = 1.0 - float(a["vacancy"]) / 100.0
        rent = (a["rent_psm_pm"] if pd.notnull(a["rent_psm_pm"]) else (a["erv_psm_pm"] or 0.0))
        noi = pos["size_sqm"] * (occ * (rent*12.0) - a["opex_psm_pa"]) * (1.0 - ibicut)
        cap = y_map.get(a["sector"], 0.05)
        value = noi / cap if cap>0 else pos["size_sqm"] * (float(a["price_psm"]) if pd.notnull(a["price_psm"]) else 0.0)
        total_noi += max(noi, 0.0)
        total_value += max(value, 0.0)
        total_debt += pos["debt_draw"]
    cash = pd.read_sql_query("SELECT cash FROM teams WHERE team_id=?", conn, params=(team_id,)).iloc[0,0]
    conn.close()
    equity = total_value + cash - total_debt
    ltv = 0.0 if total_value == 0 else total_debt/total_value
    return {"portfolio_value": total_value + cash, "equity": equity, "debt": total_debt, "ltv": ltv, "noi": total_noi, "cash": cash}

def compute_all_kpis() -> pd.DataFrame:
    conn = get_conn(); teams = pd.read_sql_query("SELECT * FROM teams", conn); conn.close()
    rows = []
    for _, t in teams.iterrows():
        k = compute_kpis(int(t.team_id))
        rows.append({"team_name": t.team_name, **k})
    df = pd.DataFrame(rows).sort_values("equity", ascending=False)
    return df

# -------------------------
# Snapshots (weekly lock)
# -------------------------

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
    seed_data()

    # Login / team
    colA, colB = st.columns([2,1])
    with colB:
        team_name = st.text_input("Your team name", value="Team A")
        team = get_team(team_name)
        st.success(f"Welcome, {team['team_name']} — Cash: €{team['cash']:,.0f}")
        st.info(f"Current Week: {get_game_week()}")

    tabs = st.tabs(["Information", "Market & Assets", "Trade", "Portfolio & KPIs", "Ask the Bot", "Instructor"])

    # Information
    with tabs[0]:
        st.subheader("How the game works — Information")
        st.markdown(
            """
            **Columns explained (Assets Catalog):**
            - **sector / subsector**: risk/lease structure & cycle sensitivity; affects exit yield (cap rate).
            - **district**: submarket quality; informs rent/ERV assumptions.
            - **size_sqm**: area you can buy; price = size × €/sqm.
            - **year / condition**: newer/core assets are lower risk; value-add/opportunistic need capex.
            - **vacancy %**: reduces effective income; impacts NOI and valuation.
            - **ERV €/sqm/mo**: estimated market rent; higher ERV → higher potential NOI.
            - **rent €/sqm/mo**: current passing rent; if below ERV, there’s upside on re-letting.
            - **opex €/sqm/yr**: operating costs; higher opex lowers NOI and value.
            - **capex_y1_pct / exp_capex_5y_psm**: expected investment; can lift ERV but needs cash.
            - **price €/sqm**: guidance/ask. Final sale prices are **bounded by market bands** (see below).
            - **status**: leased/vacant/value-add/dev readiness.

            **Selling price rules:** You can’t pick any price. The app computes a **market fair €/sqm** from
            NOI and a sector exit yield, then sets an **allowed band** ±10% around fair (adjusted by curveballs).
            If your exit price is outside the band, it’s **auto-adjusted** to the nearest allowed price.

            **Persistence:** Teams are saved in a local database. Use the **same team name** each week to load your book.
            """
        )

    # Market & Assets
    with tabs[1]:
        st.subheader("Assets Catalog")
        st.dataframe(list_assets(), use_container_width=True)
        st.caption("Prices, ERVs and opex are simplified for the game.")

    # Trade
    with tabs[2]:
        st.subheader("Trade")
        assets = list_assets()
        asset = st.selectbox("Select asset", assets["asset_id"])
        arow = assets[assets.asset_id == asset].iloc[0]
        size = st.number_input("Size (sqm)", min_value=10.0, value=float(arow.size_sqm))
        price_psm = st.number_input("Price (€/sqm)", min_value=100.0, value=float(arow.price_psm))
        acq_cost_pct = st.number_input("Acq. costs (%)", min_value=0.0, max_value=0.1, value=0.03)
        ltv_pct = st.number_input("LTV (%)", min_value=0.0, max_value=1.0, value=0.5)
        if st.button("Buy Asset"):
            buy_asset(team["team_id"], asset, size, price_psm, acq_cost_pct, ltv_pct)

        st.markdown("### Open Positions")
        pos = list_team_positions(team["team_id"])
        st.dataframe(pos, use_container_width=True)
        if len(pos) > 0:
            sel = st.selectbox("Select position to sell", pos["position_id"].tolist())
            try:
                pos_row = pos[pos["position_id"] == sel].iloc[0]
                band = get_market_price_band(pos_row["asset_id"])
                st.info(f"Allowed market band: {band['lower']:,.0f}–{band['upper']:,.0f} €/sqm (fair ~ {band['fair']:,.0f})")
                def_start = float(band['fair'])
            except Exception:
                def_start = float(arow.price_psm)
            exit_psm = st.number_input("Exit price (€/sqm)", min_value=100.0, value=def_start)
            if st.button("Sell Position"):
                sell_position(int(sel), float(exit_psm))

    # Portfolio & KPIs
    with tabs[3]:
        st.subheader("Portfolio KPIs")
        k = compute_kpis(team["team_id"])
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Portfolio Value (inc. cash)", f"€{k['portfolio_value']:,.0f}")
        c2.metric("Equity", f"€{k['equity']:,.0f}")
        c3.metric("Debt", f"€{k['debt']:,.0f}")
        c4.metric("LTV", f"{k['ltv']*100:.1f}%")
        c5.metric("NOI (approx)", f"€{k['noi']:,.0f} pa")
        c6.metric("Cash", f"€{k['cash']:,.0f}")

    # Ask the Bot
    with tabs[4]:
        st.subheader("Ask the Bot")
        q = st.text_input("Ask about assets, LTV, yields, or how to buy")
        if st.button("Ask"):
            st.write(qa_answer(q))

    # Instructor
    with tabs[5]:
        st.subheader("Instructor Controls")
        pwd = st.text_input("Admin password", type="password")
        if pwd == ADMIN_PASS:
            st.success("Admin verified")
            week = st.number_input("Set current week", min_value=1, max_value=14, value=get_game_week())
            if st.button("Update Week"):
                set_game_week(int(week))
                st.info("Week updated.")
            st.markdown("### Apply curveball")
            conn = get_conn()
            dfc = pd.read_sql_query("SELECT * FROM curveballs", conn)
            conn.close()
            st.dataframe(dfc, use_container_width=True)
            if st.button("Apply curveball for current week"):
                cw = get_game_week()
                title = dfc[dfc.week == cw]["title"].iloc[0]
                # Implemented examples
                if cw == 1:
                    set_assumption("Euribor12M_%", get_assumption("Euribor12M_%") + 0.25)
                if cw == 8:
                    set_max_ltv(0.55)
                st.warning(f"Curveball applied: {title}")

            st.markdown("### 📊 Leaderboard (live)")
            lb = compute_all_kpis()
            st.dataframe(lb, use_container_width=True)

            st.markdown("### 📑 All Open Positions")
            st.dataframe(list_all_positions(), use_container_width=True)

            st.markdown("### 🧾 Recent Transactions")
            st.dataframe(list_transactions(200), use_container_width=True)

            st.markdown("---")
            st.markdown("### 🔒 Weekly Snapshot (lock scores)")
            if st.button("Lock this week's snapshot"):
                n = take_weekly_snapshot()
                st.success(f"Locked {n} team snapshots for Week {get_game_week()}.")

            sel_week = st.number_input("View snapshots for week", min_value=1, max_value=14, value=get_game_week())
            snap = load_snapshots(int(sel_week))
            st.dataframe(snap, use_container_width=True)
            if not snap.empty:
                csv_bytes = snap.to_csv(index=False).encode("utf-8")
                st.download_button("Download snapshots CSV", data=csv_bytes, file_name=f"snapshots_week_{int(sel_week)}.csv", mime="text/csv")
        else:
            st.info("Enter admin password to manage the week, curveballs, and view the instructor dashboard.")

if __name__ == "__main__":
    main()
