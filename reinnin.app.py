# re_game_app.py
# Real Estate Portfolio Game — Madrid (50 fixed assets)
#
# Implements:
# - 50 hardcoded Madrid assets (no randomness; deterministic)
# - Initial cash: €10,000,000
# - Whole-asset trades only (no partial sqm)
# - Supply gating: an asset is unavailable while held; it returns to market only when sold
# - Sale soft cap: 7% above entry €/sqm; exceeding it rejects the sale and blocks the asset until next week
# - Weekly curveball announcements + numeric effects (W2, W4, W6, W7, W9, W12)
# - Portfolio includes 'property name'
# - Removed "ask the bot"; "Team name" -> "Name"

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List

import pandas as pd
import streamlit as st

# ---------------------------
# Config
# ---------------------------
INITIAL_CASH = 10_000_000.0  # €10m budget
SALE_SOFT_CAP_FACTOR = 1.07  # +7%
START_WEEK = 1
END_WEEK = 14

# ---------------------------
# Data models
# ---------------------------
@dataclass
class Asset:
    asset_id: str
    property_name: str
    sector: str  # Residential, Office, Retail, Logistics
    location: str  # Madrid
    sqm: int
    ask_psm: float  # current market ask €/sqm
    erv_psm: float  # ERV €/sqm (income proxy)
    opex_psm: float # Opex €/sqm
    tax_psm: float  # IBI/taxes €/sqm

@dataclass
class Holding:
    asset_id: str
    property_name: str
    sector: str
    location: str
    sqm: int
    entry_psm: float  # price per sqm paid
    buy_week: int

# ---------------------------
# Fixed catalog: 50 Madrid assets
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
    {"asset_id":"A011","property_name":"RES-MAD-USERA-11","sector":"Residential","location":"Madrid","sqm":4200,"ask_psm":3300.0,"erv_psm":165.0,"opex_psm":14.9,"tax_psm":11.9},
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

def load_assets() -> Dict[str, Asset]:
    assets: Dict[str, Asset] = {}
    for row in ASSETS_DATA:
        assets[row["asset_id"]] = Asset(**row)
    return assets

# ---------------------------
# Curveballs (announcements + numeric effects)
# ---------------------------
CURVEBALLS: Dict[int, str] = {
    1: "+25 bps interest rate shock (financing more expensive)",
    2: "Retail softness: secondary retail ERVs drop",
    3: "LP mandate: >=20% allocation to Value-Add/Opportunistic by Week 4",
    4: "Bidding war: asking prices +7% pressure",
    5: "FX wobble: noise only; no direct P&L impact",
    6: "IBI/tax re-rate: NOI -3% pressure",
    7: "Tenant bankruptcy: vacancy risk spikes (effective ERV -20%)",
    8: "Bank tightens: Max LTV 55% (if using debt module)",
    9: "Energy spike: opex +12% pressure",
    10: "PropTech incentive: retention +10%",
    11: "Benchmarking bonus: top quartile rewarded",
    12: "Residential demand: ERV growth +2%",
    13: "Green subsidy: 30% rebate on capex",
    14: "Freeze & score: final valuations",
}

def apply_curveball_effects(week: int):
    """Apply numeric effects exactly once per week."""
    applied = st.session_state.get("applied_curveballs", set())
    if week in applied:
        return

    assets = st.session_state.assets

    if week == 2:
        # Retail softness: ERV -5% for Retail
        for a in assets.values():
            if a.sector == "Retail":
                a.erv_psm = round(a.erv_psm * 0.95, 2)
    elif week == 4:
        # Bidding war: asking +7% for assets currently in market (not held)
        held_ids = set(st.session_state.portfolio.keys())
        for a in assets.values():
            if a.asset_id not in held_ids:
                a.ask_psm = round(a.ask_psm * 1.07, 2)
    elif week == 6:
        # IBI/tax re-rate: taxes +3% for all
        for a in assets.values():
            a.tax_psm = round(a.tax_psm * 1.03, 2)
    elif week == 7:
        # Tenant bankruptcy shock: effective ERV -20% for Office & Retail
        for a in assets.values():
            if a.sector in ("Office", "Retail"):
                a.erv_psm = round(a.erv_psm * 0.80, 2)
    elif week == 9:
        # Energy spike: opex +12% for all
        for a in assets.values():
            a.opex_psm = round(a.opex_psm * 1.12, 2)
    elif week == 12:
        # Residential demand: ERV +2% for Residential
        for a in assets.values():
            if a.sector == "Residential":
                a.erv_psm = round(a.erv_psm * 1.02, 2)

    applied.add(week)
    st.session_state.applied_curveballs = applied

# ---------------------------
# State init
# ---------------------------
def init_state():
    if "assets" not in st.session_state:
        st.session_state.assets: Dict[str, Asset] = load_assets()
    if "portfolio" not in st.session_state:
        st.session_state.portfolio: Dict[str, Holding] = {}
    if "cash" not in st.session_state:
        st.session_state.cash = INITIAL_CASH
    if "current_week" not in st.session_state:
        st.session_state.current_week = START_WEEK
    if "sale_blocks" not in st.session_state:
        st.session_state.sale_blocks: Dict[str, int] = {}
    if "name" not in st.session_state:
        st.session_state.name = ""
    if "applied_curveballs" not in st.session_state:
        st.session_state.applied_curveballs = set()

# Helpers for sale block (soft cap rejection)
def is_blocked(asset_id: str) -> bool:
    blocked_until = st.session_state.sale_blocks.get(asset_id)
    return blocked_until is not None and st.session_state.current_week < blocked_until

def block_until_next_week(asset_id: str):
    st.session_state.sale_blocks[asset_id] = st.session_state.current_week + 1

def clear_block(asset_id: str):
    st.session_state.sale_blocks.pop(asset_id, None)

# ---------------------------
# UI components
# ---------------------------
def header():
    st.title("Real Estate Portfolio Game — Madrid (50 Assets)")
    col1, col2, col3 = st.columns([1.6, 1, 1])
    with col1:
        st.text_input("Name", key="name", placeholder="Team / Player")
    with col2:
        st.metric("Week", st.session_state.current_week)
    with col3:
        st.metric("Cash (€)", f"{st.session_state.cash:,.0f}")

    # Weekly announcement banner
    cb = CURVEBALLS.get(st.session_state.current_week)
    if cb:
        st.info(f"**This week’s curveball:** {cb}")

def market_view():
    st.subheader("Market — Madrid")
    # Asset is available if NOT currently held
    held_ids = set(st.session_state.portfolio.keys())

    rows = []
    for a in st.session_state.assets.values():
        available = a.asset_id not in held_ids
        rows.append({
            "asset_id": a.asset_id,
            "property_name": a.property_name,
            "sector": a.sector,
            "location": a.location,
            "sqm": a.sqm,
            "ask_psm": a.ask_psm,
            "available": "Yes" if available else "Held",
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, hide_index=True)

def portfolio_view():
    st.subheader("Portfolio (Open Positions)")
    if not st.session_state.portfolio:
        st.caption("No holdings yet.")
        return
    rows = []
    for h in st.session_state.portfolio.values():
        rows.append({
            "asset_id": h.asset_id,
            "property_name": h.property_name,
            "sector": h.sector,
            "location": h.location,
            "sqm": h.sqm,
            "entry_psm": h.entry_psm,
            "buy_week": h.buy_week,
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, hide_index=True)

def buy_view():
    st.subheader("Buy Asset (whole-asset only) — Madrid")

    held_ids = set(st.session_state.portfolio.keys())
    available_assets = [a for a in st.session_state.assets.values() if a.asset_id not in held_ids]
    if not available_assets:
        st.warning("No assets are available right now. Sell something to release supply.")
        return

    choices = {f"{a.asset_id} — {a.property_name} ({a.sector}, {a.location})": a.asset_id for a in available_assets}
    label = st.selectbox("Select asset", options=list(choices.keys()))
    asset = st.session_state.assets[choices[label]]

    st.write(f"**{asset.property_name}** — {asset.sector} in {asset.location}")
    st.write(f"Size: **{asset.sqm:,} sqm** | Ask: **€{asset.ask_psm:,.2f}/sqm** | Ticket: **€{asset.sqm * asset.ask_psm:,.0f}**")

    ticket = asset.sqm * asset.ask_psm

    if ticket > st.session_state.cash:
        st.error("Insufficient cash to buy this asset.")
        return

    if st.button("Buy (whole asset)"):
        st.session_state.cash -= ticket
        st.session_state.portfolio[asset.asset_id] = Holding(
            asset_id=asset.asset_id,
            property_name=asset.property_name,
            sector=asset.sector,
            location=asset.location,
            sqm=asset.sqm,
            entry_psm=asset.ask_psm,
            buy_week=st.session_state.current_week,
        )
        st.success(f"Bought **{asset.property_name}** for €{ticket:,.0f}.")

def sell_view():
    st.subheader("Sell Asset (whole-asset only) — Madrid")

    if not st.session_state.portfolio:
        st.caption("No holdings to sell.")
        return

    labels = {f"{h.asset_id} — {h.property_name} ({h.sector}, {h.location})": h.asset_id for h in st.session_state.portfolio.values()}
    label = st.selectbox("Select holding", options=list(labels.keys()))
    holding = st.session_state.portfolio[labels[label]]

    max_exit_this_week = round(holding.entry_psm * SALE_SOFT_CAP_FACTOR, 2)

    if is_blocked(holding.asset_id):
        st.warning(f"Sale for this asset is blocked until week {st.session_state.sale_blocks[holding.asset_id]} due to exceeding last week's cap.")
        return

    st.write(f"**Max exit this week:** €{max_exit_this_week:,.2f}/sqm (7% over your entry €{holding.entry_psm:,.2f}/sqm)")

    proposed_exit_psm = st.number_input(
        "Exit price (€/sqm)",
        min_value=0.0,
        value=float(max_exit_this_week),
        step=10.0,
        help="If you exceed the weekly cap, the sale is rejected immediately and this asset is blocked until next week.",
    )

    if proposed_exit_psm > max_exit_this_week:
        block_until_next_week(holding.asset_id)
        st.error(
            f"Sale rejected: €{proposed_exit_psm:,.2f}/sqm exceeds this week's cap (€{max_exit_this_week:,.2f}/sqm). "
            f"You must wait until week {st.session_state.sale_blocks[holding.asset_id]} to try again."
        )
        return

    if st.button("Sell (whole asset)"):
        proceeds = holding.sqm * proposed_exit_psm
        st.session_state.cash += proceeds

        # Return asset to market at new ask (available because it's no longer held)
        asset = st.session_state.assets[holding.asset_id]
        asset.ask_psm = proposed_exit_psm

        st.session_state.portfolio.pop(holding.asset_id, None)
        clear_block(holding.asset_id)

        st.success(f"Sold **{holding.property_name}** at €{proposed_exit_psm:,.2f}/sqm for **€{proceeds:,.0f}**.")

def week_controls():
    st.subheader("Week Controls")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("End Week ➜"):
            if st.session_state.current_week < END_WEEK:
                st.session_state.current_week += 1
                apply_curveball_effects(st.session_state.current_week)
                st.success(f"Advanced to **Week {st.session_state.current_week}**.")
            else:
                st.info("You are at the final week.")
    with col2:
        if st.button("Reset Game"):
            for k in ["assets", "portfolio", "cash", "current_week", "sale_blocks", "applied_curveballs"]:
                if k in st.session_state:
                    del st.session_state[k]
            init_state()
            st.success("Game reset.")

# ---------------------------
# Main app
# ---------------------------
def main():
    st.set_page_config(page_title="RE Portfolio Game — Madrid (50 Assets)", layout="wide")
    init_state()
    header()

    tabs = st.tabs(["Market", "Portfolio", "Buy", "Sell", "Week"])

    with tabs[0]:
        market_view()
    with tabs[1]:
        portfolio_view()
    with tabs[2]:
        buy_view()
    with tabs[3]:
        sell_view()
    with tabs[4]:
        week_controls()

if __name__ == "__main__":
    main()
