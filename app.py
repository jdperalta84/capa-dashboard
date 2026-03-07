import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import io

from data_engine import load_and_compute, load_and_compute_multi, REGION_ORDER, REGION_COLORS
from export_utils import export_regional_summary

st.set_page_config(page_title="CAPA · PTO Dashboard", page_icon="📋",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

/* ══════════════════════════════════════════════
   CSS VARIABLES — change one place, updates all
   ══════════════════════════════════════════════ */
:root {
    --navy:      #0f1c2e;
    --navy-mid:  #1a2d45;
    --navy-soft: #243b55;
    --accent:    #8C1D18;
    --accent-lt: #f9e8e7;
    --canvas:    #f4f6f9;
    --surface:   #ffffff;
    --border:    #e2e6ed;
    --border-dk: #c8cfd8;
    --txt-primary: #0f1c2e;
    --txt-secondary: #5a6577;
    --txt-muted:  #9aa3b0;
    --green:  #0d7a4e;
    --green-lt: #dcf5ec;
    --amber:  #b45309;
    --amber-lt: #fef3c7;
    --red:    #c0392b;
    --red-lt: #fdecea;
    --mono: 'DM Mono', 'Courier New', monospace;
    --sans: 'DM Sans', system-ui, sans-serif;
}

/* ── Reset & Base ── */
html, body, [class*="css"] {
    font-family: var(--sans);
    color: var(--txt-primary);
}

/* ── Main canvas ── */
.main { background: var(--canvas) !important; }
.block-container {
    padding: 1.4rem 2rem 2.5rem !important;
    max-width: 1480px !important;
}

/* ══════════════════════════════════════════════
   SIDEBAR — deep navy, instrument panel feel
   ══════════════════════════════════════════════ */
section[data-testid="stSidebar"] {
    background: var(--navy) !important;
    border-right: 1px solid var(--navy-soft) !important;
}
section[data-testid="stSidebar"] * {
    color: #c8d6e8 !important;
    font-family: var(--sans) !important;
}
section[data-testid="stSidebar"] .stSelectbox > div > div {
    background: var(--navy-mid) !important;
    border: 1px solid var(--navy-soft) !important;
    border-radius: 5px !important;
    color: #c8d6e8 !important;
    font-size: 0.8rem !important;
}
section[data-testid="stSidebar"] .stFileUploader {
    background: var(--navy-mid) !important;
    border: 1px dashed var(--navy-soft) !important;
    border-radius: 7px !important;
    padding: 0.5rem !important;
}
section[data-testid="stSidebar"] .stButton > button {
    background: var(--navy-mid) !important;
    color: #a8c4e0 !important;
    border: 1px solid var(--navy-soft) !important;
    border-radius: 5px !important;
    font-size: 0.78rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.02em !important;
    padding: 0.45rem 0.9rem !important;
    width: 100% !important;
    transition: all 0.15s ease !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: var(--navy-soft) !important;
    border-color: #A52320 !important;
    color: #ffe8e7 !important;
}
section[data-testid="stSidebar"] .stToggle span { color: #c8d6e8 !important; }

.sidebar-section {
    font-size: 0.58rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    color: #4a6380 !important;
    padding: 0.25rem 0 0.3rem;
    margin: 1rem 0 0.5rem;
    border-bottom: 1px solid #1e3050;
}

/* ══════════════════════════════════════════════
   PAGE HEADER
   ══════════════════════════════════════════════ */
.page-header {
    padding: 1rem 1.5rem;
    background: var(--navy);
    border-radius: 10px;
    margin-bottom: 1.2rem;
    box-shadow: 0 2px 12px rgba(15,28,46,0.18);
    position: relative;
    overflow: hidden;
}
.page-header::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: linear-gradient(90deg, #8C1D18 0%, #C0392B 50%, #8C1D18 100%);
}
.page-title {
    font-size: 1.2rem;
    font-weight: 700;
    color: #e8f4ff !important;
    letter-spacing: -0.01em;
    margin: 0;
    line-height: 1.3;
}
.page-meta {
    font-size: 0.72rem;
    color: #6a8aaa !important;
    margin-top: 0.35rem;
    display: flex;
    align-items: center;
    gap: 0.6rem;
    flex-wrap: wrap;
}
.page-meta .dot { color: #2a4060 !important; }
.page-meta .pill {
    background: rgba(140,29,24,0.15);
    color: #e8a0a0 !important;
    padding: 2px 9px;
    border-radius: 4px;
    font-size: 0.68rem;
    font-weight: 600;
    border: 1px solid rgba(140,29,24,0.25);
    letter-spacing: 0.02em;
}
.page-meta .pill-green {
    background: rgba(13,122,78,0.2);
    color: #4fd1a0 !important;
    border-color: rgba(13,122,78,0.3);
}
.date-range-badge {
    background: rgba(180,83,9,0.2);
    color: #fbbf24 !important;
    padding: 2px 10px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 600;
    border: 1px solid rgba(251,191,36,0.25);
    font-family: var(--mono);
}

/* ══════════════════════════════════════════════
   METRIC CARDS
   ══════════════════════════════════════════════ */
.metric-card {
    background: var(--surface);
    border-radius: 8px;
    padding: 1rem 1.2rem;
    border: 1px solid var(--border);
    border-top: 3px solid;
    box-shadow: 0 1px 4px rgba(15,28,46,0.06), 0 0 0 0 transparent;
    flex: 1;
    min-width: 0;
    transition: box-shadow 0.2s ease, transform 0.2s ease;
}
.metric-card:hover {
    box-shadow: 0 4px 16px rgba(15,28,46,0.12);
    transform: translateY(-1px);
}
.metric-hdr {
    font-size: 0.52rem;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--txt-muted);
    font-weight: 700;
    margin-bottom: 0.45rem;
    opacity: 0.65;
}
.metric-val {
    font-family: var(--mono);
    font-size: 1.7rem;
    font-weight: 500;
    line-height: 1;
    letter-spacing: -0.03em;
}
.metric-lbl {
    font-size: 0.62rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--txt-muted);
    margin-top: 0.4rem;
    font-weight: 600;
}
.metric-sub {
    font-size: 0.7rem;
    color: var(--txt-muted);
    margin-top: 0.15rem;
    font-family: var(--mono);
}
.metric-divider {
    height: 1px;
    background: var(--border);
    margin: 0.65rem 0 0.5rem;
}
.metric-ye-lbl {
    font-size: 0.58rem;
    color: var(--txt-muted);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-weight: 600;
}
.metric-ye-val {
    font-family: var(--mono);
    font-size: 0.88rem;
    font-weight: 500;
    margin-top: 0.15rem;
}

/* ══════════════════════════════════════════════
   SECTION HEADERS
   ══════════════════════════════════════════════ */
.section-hdr {
    font-size: 0.6rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.14em;
    color: var(--txt-muted);
    border-bottom: 2px solid var(--border);
    padding-bottom: 0.4rem;
    margin-bottom: 0.85rem;
    margin-top: 1.2rem;
    display: flex;
    align-items: center;
    gap: 0.4rem;
}

/* ══════════════════════════════════════════════
   TABS — clean pill switcher
   ══════════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
    gap: 3px;
    background: var(--border);
    padding: 3px;
    border-radius: 8px;
    border: none !important;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 5px;
    padding: 0.45rem 1.3rem;
    font-size: 0.8rem;
    font-weight: 500;
    color: var(--txt-secondary);
    background: transparent;
    border: none;
    letter-spacing: 0.01em;
    transition: all 0.15s;
}
.stTabs [aria-selected="true"] {
    background: #8C1D18 !important;
    color: #fff0ef !important;
    box-shadow: 0 1px 4px rgba(140,29,24,0.35) !important;
    font-weight: 600 !important;
}

/* ══════════════════════════════════════════════
   OVERDUE PILL
   ══════════════════════════════════════════════ */
.ov-pill {
    padding: 3px 10px;
    border-radius: 4px;
    font-size: 0.74rem;
    font-weight: 700;
    display: inline-block;
    font-family: var(--mono);
    letter-spacing: 0.02em;
}

/* ══════════════════════════════════════════════
   TOP 20 TABLE
   ══════════════════════════════════════════════ */
.top20-row {
    padding: 0.32rem 0;
    border-bottom: 1px solid var(--border);
    font-size: 0.81rem;
}

/* ══════════════════════════════════════════════
   CHART CONTAINER
   ══════════════════════════════════════════════ */
.chart-wrap {
    background: var(--surface);
    border-radius: 8px;
    border: 1px solid var(--border);
    padding: 0.5rem;
    box-shadow: 0 1px 4px rgba(15,28,46,0.05);
}

/* ══════════════════════════════════════════════
   DATAFRAME / TABLE OVERRIDES
   ══════════════════════════════════════════════ */
[data-testid="stDataFrame"] th {
    background: var(--navy) !important;
    color: #a8c4e0 !important;
    font-size: 0.68rem !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    font-family: var(--sans) !important;
}
[data-testid="stDataFrame"] td {
    font-family: var(--mono) !important;
    font-size: 0.78rem !important;
    color: var(--txt-primary) !important;
}

/* ══════════════════════════════════════════════
   BUTTONS — DOWNLOAD
   ══════════════════════════════════════════════ */
.stDownloadButton > button {
    background: var(--navy) !important;
    color: #c8d6e8 !important;
    border: 1px solid var(--navy-soft) !important;
    border-radius: 5px !important;
    font-size: 0.78rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.03em !important;
    transition: all 0.15s !important;
}
.stDownloadButton > button:hover {
    background: var(--navy-soft) !important;
    color: #e8f4ff !important;
}

/* ══════════════════════════════════════════════
   EQUAL HEIGHT METRIC CARDS
   ══════════════════════════════════════════════ */
/* Make scorecard columns stretch to equal height */
[data-testid="stHorizontalBlock"]:has(.metric-card) {
    align-items: stretch !important;
}
[data-testid="stHorizontalBlock"]:has(.metric-card) > [data-testid="column"] {
    display: flex !important;
    flex-direction: column !important;
}
[data-testid="stHorizontalBlock"]:has(.metric-card) > [data-testid="column"] > div {
    flex: 1 !important;
    display: flex !important;
    flex-direction: column !important;
}
.metric-card {
    flex: 1 !important;
    display: flex !important;
    flex-direction: column !important;
}
/* Remove default Streamlit red top bar */
#MainMenu, footer, header { visibility: hidden; }
/* Tighten column gaps slightly */
[data-testid="column"] { padding: 0 0.3rem !important; }
/* Subtle scrollbar */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: var(--canvas); }
::-webkit-scrollbar-thumb { background: var(--border-dk); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--txt-muted); }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="padding:1.1rem 0 0.6rem">
      <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.3rem">
        <div style="width:28px;height:28px;background:linear-gradient(135deg,#A52320,#6b1210);
                    border-radius:6px;display:flex;align-items:center;justify-content:center;
                    font-size:0.85rem;flex-shrink:0">📋</div>
        <div style="font-size:1.0rem;font-weight:700;color:#e8f4ff;letter-spacing:-0.01em;
                    font-family:'DM Sans',sans-serif">CAPA · PTO</div>
      </div>
      <div style="font-size:0.58rem;color:#3d5a78;text-transform:uppercase;
                  letter-spacing:0.16em;padding-left:2px">
        Quality Performance Dashboard
      </div>
    </div>
    <div style="height:1px;background:linear-gradient(90deg,#8C1D18 0%,#1a2d45 100%);
                margin-bottom:0.5rem"></div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">Data Source</div>', unsafe_allow_html=True)
    uploaded_files = st.file_uploader("Upload Excel file", type=["xlsx"],
                                     label_visibility="collapsed",
                                     accept_multiple_files=True)
    load_btn = st.button("⟳  Reload Data", use_container_width=True)

    st.markdown('<div class="sidebar-section">Region</div>', unsafe_allow_html=True)
    region_placeholder = st.empty()

    st.markdown('<div class="sidebar-section">Location</div>', unsafe_allow_html=True)
    location_placeholder = st.empty()

    st.markdown('<div class="sidebar-section">Date Range</div>', unsafe_allow_html=True)
    date_start_ph = st.empty()
    date_end_ph   = st.empty()

    st.markdown('<div class="sidebar-section">Filters</div>', unsafe_allow_html=True)
    exclude_jn = st.toggle("Exclude JN PTOs", value=False)

    st.markdown('<div class="sidebar-section">Export</div>', unsafe_allow_html=True)
    export_reg_btn = st.button("↓  Regional Summary (Excel)", use_container_width=True)
    st.markdown("""
    <div style="font-size:0.62rem;color:#3d5a78;margin-top:0.4rem;line-height:1.6">
      PDF: browser Print → Save as PDF
    </div>""", unsafe_allow_html=True)

    st.markdown('<div style="height:1px;background:#1a2d45;margin:1rem 0 0.5rem"></div>',
                unsafe_allow_html=True)
    loaded_at_ph = st.empty()

# ══════════════════════════════════════════════════════════════════
# LOAD DATA
# ══════════════════════════════════════════════════════════════════
@st.cache_data(show_spinner="Computing metrics…")
def get_data(all_file_bytes, exclude_jn=False):
    return load_and_compute_multi(
        [io.BytesIO(b) for b in all_file_bytes], exclude_jn=exclude_jn)

if uploaded_files:
    all_bytes  = tuple(f.read() for f in uploaded_files)
    file_hash  = hash(all_bytes)
    jn_changed = st.session_state.get("exclude_jn") != exclude_jn
    if ("data" not in st.session_state
            or st.session_state.get("file_hash") != file_hash
            or load_btn or jn_changed):
        st.session_state.exclude_jn = exclude_jn
        try:
            get_data.clear()
            st.session_state.data = get_data(all_bytes, exclude_jn)
            st.session_state.file_hash = file_hash
            st.session_state.filename = uploaded_files[0].name
        except Exception as e:
            st.error(f"Error loading file: {e}")
            st.stop()
elif "data" not in st.session_state:
    st.markdown("""
    <div style="display:flex;align-items:center;justify-content:center;
                height:60vh;flex-direction:column;gap:1rem">
      <div style="width:60px;height:60px;background:linear-gradient(135deg,#A52320,#6b1210);
                  border-radius:14px;display:flex;align-items:center;justify-content:center;
                  font-size:1.8rem;box-shadow:0 4px 20px rgba(30,111,217,0.3)">📋</div>
      <div style="font-size:1.1rem;font-weight:700;color:#0f1c2e">Upload your Excel file</div>
      <div style="font-size:0.85rem;color:#9aa3b0;text-align:center;max-width:300px;
                  line-height:1.6">
        Drag and drop your CAPA/PTO tracking file into the sidebar uploader to get started.
      </div>
    </div>""", unsafe_allow_html=True)
    st.stop()

D = st.session_state.data

with loaded_at_ph:
    fname = st.session_state.get("filename", "")
    st.markdown(f"""
    <div style="font-size:0.65rem;color:#3d5a78;line-height:1.7;
                padding:0.6rem 0.5rem;background:#0d1829;border-radius:5px;
                border:1px solid #1a2d45">
      <div style="color:#4a6380;font-size:0.56rem;text-transform:uppercase;
                  letter-spacing:0.12em;margin-bottom:2px">Last Loaded</div>
      <div style="color:#e8a0a0;font-family:'DM Mono',monospace">{D['loaded_at']}</div>
      <div style="color:#2a4060;margin-top:3px;word-break:break-all;
                  font-size:0.6rem">{fname}</div>
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# FILTERS
# ══════════════════════════════════════════════════════════════════
region_map     = D['region_map']
all_locations  = D['all_locations']
all_months     = D['month_labels']
NM             = len(all_months)
loc_id_map     = D.get('loc_id_map', {})

def loc_label(name):
    """Return 'Location Name - ID' if ID available, else just name."""
    lid = loc_id_map.get(name, '')
    return f'{name} - {lid}' if lid else name

def loc_from_label(label):
    """Reverse loc_label — extract the location name from a display label."""
    if not label or label == 'ALL':
        return 'ALL'
    # Strip trailing ' - ID' suffix if present
    for name in all_locations:
        if label == loc_label(name) or label == name:
            return name
    return label  # fallback

region_options = ['ALL REGIONS'] + [f'[{r}]' for r in REGION_ORDER if r in region_map]

with region_placeholder:
    selected_region = st.selectbox("Region", region_options, key="region_sel",
                                   label_visibility="collapsed")

if selected_region == 'ALL REGIONS':
    filtered_locs = all_locations
    region_key    = 'ALL'
else:
    rname         = selected_region.strip('[]')
    filtered_locs = sorted(region_map.get(rname, []))
    region_key    = f'REGION:{rname}'

# Display labels include "- ID" suffix; data_key uses the plain name
filtered_loc_labels = [loc_label(l) for l in filtered_locs]
with location_placeholder:
    selected_loc_label = st.selectbox("Location", ['ALL'] + filtered_loc_labels,
                                      key="loc_sel", label_visibility="collapsed")
selected_loc = loc_from_label(selected_loc_label)

data_key = region_key if selected_loc == 'ALL' else selected_loc

# ── Date range ────────────────────────────────────────────────────
with date_start_ph:
    start_month = st.selectbox("From", all_months,
                               index=0, key="date_start",
                               label_visibility="collapsed")
with date_end_ph:
    end_month = st.selectbox("To", all_months,
                             index=NM - 1, key="date_end",
                             label_visibility="collapsed")

# Resolve indices (guard against end < start)
start_idx = all_months.index(start_month)
end_idx   = all_months.index(end_month)
if end_idx < start_idx:
    end_idx = start_idx
slice_months = all_months[start_idx:end_idx + 1]

def slice_data(lst):
    return lst[start_idx:end_idx + 1]

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def ov_color(val, t_hi, t_lo):
    if val > t_hi:   return '#c0392b', '#fff'
    elif val > t_lo: return '#b45309', '#fff'
    else:            return '#0d7a4e', '#fff'

THEME = {
    'car':      {'primary': '#1e6fd9', 'bar2': '#64748b', 'line': '#c0392b', 'wavg': '#9aa3b0'},
    'pto':      {'primary': '#0d7a4e', 'bar2': '#64748b', 'line': '#8b1a1a', 'wavg': '#9aa3b0'},
    'combined': {'primary': '#8C1D18', 'bar2': '#64748b', 'line': '#5a6577', 'wavg': '#9aa3b0'},
}

def get_full(key):  return D[key].get(data_key, D[key]['ALL'])
def get_sliced(key):return slice_data(D[key].get(data_key, D[key]['ALL']))

# ══════════════════════════════════════════════════════════════════
# SCORECARD  (always full data range — no slicing)
# ══════════════════════════════════════════════════════════════════
def scorecard(metrics, wavg_vals, colors, closed_label, t_hi, t_lo):
    NM           = len(metrics)
    last_month   = slice_months[-1] if slice_months else all_months[-1]

    # YE benchmark: always last complete year (dynamic, not hardcoded)
    last_dec_yr       = D['last_dec_year']
    last_dec_idx_full = D['last_dec_idx']
    ye_label          = f"{last_dec_yr} YE"

    closed_list   = [r['closed'] for r in metrics]
    ov_snap_list  = [r['ov90']   for r in metrics]   # currently open >90 at month-end

    total_closed  = sum(closed_list)
    last_ov_snap  = ov_snap_list[-1] if ov_snap_list else 0
    cur_wavg      = wavg_vals[-1] if wavg_vals else 0

    # Closed >90: closed records whose days2close > 90 (computed per-month in engine)
    closed_ov90_list  = [r.get('closed_ov90', 0) for r in metrics]
    total_closed_ov90 = sum(closed_ov90_list)

    # ── 6-month trend on snapshot ──
    if NM >= 6:
        recent_3   = ov_snap_list[-3:]
        prior_3    = ov_snap_list[-6:-3:]
        recent_avg = round(np.mean(recent_3), 1)
        prior_avg  = round(np.mean(prior_3),  1)
        if prior_avg > 0:
            pct_change = int(round((recent_avg - prior_avg) / prior_avg * 100))
        else:
            pct_change = 0 if recent_avg == 0 else 100
        abs_delta  = recent_avg - prior_avg
        sign       = '+' if pct_change > 0 else ''
        snap_trend = f"{'▲' if abs_delta > 0.5 else ('▼' if abs_delta < -0.5 else '→')} {sign}{pct_change}% (last 3 vs prior 3 mo)"
        snap_trend_color = '#c0392b' if abs_delta > 0.5 else ('#0d7a4e' if abs_delta < -0.5 else '#5a6577')
    else:
        snap_trend       = '—'
        snap_trend_color = '#9aa3b0'

    # YE values
    full_m  = get_full('car_metrics' if closed_label == 'CARs Closed'
                       else ('pto_metrics' if closed_label == 'PTOs Closed'
                             else 'cmb_metrics'))
    full_w  = get_full('car_wavg'    if closed_label == 'CARs Closed'
                       else ('pto_wavg'    if closed_label == 'PTOs Closed'
                             else 'cmb_wavg'))
    all_months_full  = D['month_labels']
    ye_start_idx     = next((i for i, m in enumerate(all_months_full)
                             if m.endswith(str(last_dec_yr))), last_dec_idx_full - 11)
    ye_slice         = full_m[ye_start_idx:last_dec_idx_full + 1]
    ye_closed        = sum(r['closed'] for r in ye_slice)
    ye_ov_snap       = full_m[last_dec_idx_full]['ov90']
    ye_closed_ov90   = sum(r.get('closed_ov90', 0) for r in ye_slice)
    ye_wavg          = full_w[last_dec_idx_full]
    ye_ov_color      = ov_color(ye_ov_snap, t_hi, t_lo)[0]
    ye_cov_color     = ov_color(ye_closed_ov90, t_hi, t_lo)[0]

    def card(header, border, val_color, val_size, val, lbl, sub, ye_color, ye_val, ye_lbl=None):
        hdr_html = f'<div class="metric-hdr">{header}</div>' if header else ''
        ye_lbl_s = ye_lbl or ye_label
        return f"""
        <div class="metric-card" style="border-color:{border}">
          {hdr_html}
          <div class="metric-val"  style="color:{val_color};font-size:{val_size}">{val}</div>
          <div class="metric-lbl">{lbl}</div>
          <div class="metric-sub">{sub}</div>
          <div class="metric-divider"></div>
          <div class="metric-ye-lbl">{ye_lbl_s}</div>
          <div class="metric-ye-val" style="color:{ye_color}">{ye_val}</div>
        </div>"""

    # ── 4-card layout ──────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(card(
            "VOLUME",
            colors['primary'], colors['primary'], '1.8rem',
            f"{total_closed:,}", closed_label, f"{start_month} – {last_month}",
            colors['primary'], f"{ye_closed:,}"), unsafe_allow_html=True)
    with c2:
        bg, _ = ov_color(last_ov_snap, t_hi, t_lo)
        st.markdown(card(
            "AGING — OPEN BACKLOG",
            bg, bg, '1.8rem',
            last_ov_snap,
            "Currently Open >90 Days",
            f"Snapshot: end of {last_month}",
            ye_ov_color, ye_ov_snap,
            f"{ye_label} snapshot (Dec 31)"), unsafe_allow_html=True)
    with c3:
        bg3, _ = ov_color(total_closed_ov90, t_hi, t_lo)
        st.markdown(card(
            "AGING — CLOSED LATE",
            bg3, bg3, '1.8rem',
            total_closed_ov90,
            "Closed Records That Took >90 Days",
            f"{start_month} – {last_month}",
            ye_cov_color, ye_closed_ov90,
            f"{ye_label} count"), unsafe_allow_html=True)
    with c4:
        st.markdown(card(
            "CYCLE TIME",
            colors['wavg'], colors['primary'], '1.8rem',
            cur_wavg,
            "Wtd Avg Days to Close",
            f"YTD running avg — resets Jan",
            colors['primary'], ye_wavg), unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# CHART  (sliced to selected date range)
# ══════════════════════════════════════════════════════════════════
def build_chart(sliced_metrics, sliced_wavg, colors, title, show_split=False):
    closed   = [r['closed']    for r in sliced_metrics]
    ov90     = [r['ov90']   for r in sliced_metrics]   # snapshot per month-end

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Bar(
        x=slice_months, y=closed, name="Closed",
        marker_color=colors['primary'], opacity=0.8,
        marker_line_width=0,
        hovertemplate="<b>%{x}</b><br>Closed: %{y}<extra></extra>"),
        secondary_y=False)

    if show_split:
        fig.add_trace(go.Bar(
            x=slice_months, y=[r.get('ov90_car', 0) for r in sliced_metrics],
            name="CARs Open >90", marker_color='#64748b', opacity=0.85,
            marker_line_width=0,
            hovertemplate="<b>%{x}</b><br>CARs >90: %{y}<extra></extra>"),
            secondary_y=False)
        fig.add_trace(go.Bar(
            x=slice_months, y=[r.get('ov90_pto', 0) for r in sliced_metrics],
            name="PTOs Open >90", marker_color='#94a3b8', opacity=0.85,
            marker_line_width=0,
            hovertemplate="<b>%{x}</b><br>PTOs >90: %{y}<extra></extra>"),
            secondary_y=False)
    else:
        fig.add_trace(go.Bar(
            x=slice_months, y=ov90, name="Open >90 Days",
            marker_color=colors['bar2'], opacity=0.8,
            marker_line_width=0,
            hovertemplate="<b>%{x}</b><br>Open >90: %{y}<extra></extra>"),
            secondary_y=False)

    fig.add_trace(go.Scatter(
        x=slice_months, y=sliced_wavg, name="Wtd Avg (YTD)",
        mode="lines+markers",
        line=dict(color=colors['wavg'], width=2, dash='dot'),
        marker=dict(size=4, color=colors['wavg']),
        hovertemplate="<b>%{x}</b><br>Wtd Avg: %{y:.0f}<extra></extra>"),
        secondary_y=True)

    # Year boundary lines within the slice (use numeric index for categorical x-axis)
    for yr_idx, yr_label in [(i, all_months[i][:4])
                              for i, m in enumerate(all_months)
                              if m.startswith('Jan') and start_idx < i <= end_idx]:
        slice_pos = yr_idx - start_idx - 0.5  # position within sliced x-axis
        if 0 <= slice_pos <= len(slice_months):
            fig.add_shape(type='line',
                          x0=slice_pos, x1=slice_pos, y0=0, y1=1,
                          xref='x', yref='paper',
                          line=dict(dash='dot', color='rgba(239,68,68,0.5)', width=1.5))
            fig.add_annotation(x=slice_pos, y=1, xref='x', yref='paper',
                               text=yr_label, showarrow=False,
                               font=dict(size=9, color='#ef4444'),
                               xanchor='left', yanchor='bottom')

    fig.update_layout(
        title=dict(text=title, font=dict(size=11, color='#0f1c2e', family='DM Sans'), x=0, y=0.97),
        barmode='group',
        plot_bgcolor='white', paper_bgcolor='white',
        legend=dict(
            orientation='h', yanchor='bottom', y=1.01,
            xanchor='right', x=1,
            font=dict(size=9, color='#5a6577', family='DM Sans'),
            bgcolor='rgba(0,0,0,0)',
        ),
        margin=dict(l=0, r=0, t=44, b=0),
        hovermode='x unified',
        hoverlabel=dict(
            bgcolor='#0f1c2e',
            bordercolor='#1a2d45',
            font=dict(color='#c8d6e8', size=11, family='DM Mono'),
        ),
        xaxis=dict(
            tickfont=dict(size=9, color='#9aa3b0', family='DM Sans'),
            showgrid=False, zeroline=False,
            linecolor='#e2e6ed',
            tickangle=-30,
        ),
        yaxis=dict(
            title=dict(text='Count', font=dict(size=9, color='#9aa3b0', family='DM Sans')),
            gridcolor='#f4f6f9', zeroline=False,
            tickfont=dict(size=9, color='#9aa3b0', family='DM Mono'),
            gridwidth=1,
        ),
        yaxis2=dict(
            title=dict(text='Days', font=dict(size=9, color='#9aa3b0', family='DM Sans')),
            gridcolor='#f4f6f9', zeroline=False,
            tickfont=dict(size=9, color='#9aa3b0', family='DM Mono'),
        ),
        height=310,
        font=dict(family='DM Sans'),
    )
    return fig

# ══════════════════════════════════════════════════════════════════
# MONTHLY TABLE  (sliced)
# ══════════════════════════════════════════════════════════════════
def monthly_table(sliced_metrics, sliced_wavg, extra_cols=None):
    rows = []
    for i, (m, r, w) in enumerate(zip(slice_months, sliced_metrics, sliced_wavg)):
        row = {'Month': m, 'Closed': r['closed'], 'Open >90': r['ov90'], 'Wtd Avg': w}
        if extra_cols:
            row.update(extra_cols[i])
        rows.append(row)
    df = pd.DataFrame(rows)
    return (df.style
              .format({'Open >90': '{:.0f}', 'Wtd Avg': '{:.0f}'}))

# ══════════════════════════════════════════════════════════════════
# TOP 20  (uses last month of sliced range)
# ══════════════════════════════════════════════════════════════════
def top20_table(top20_data, t_hi, t_lo, val_label, secondary_cols=None):
    range_label = f"{start_month} – {slice_months[-1]}" if slice_months else all_months[-1]
    st.markdown(
        f'<div class="section-hdr">⚠ &nbsp;Top 20 Locations — Open >90 Days &nbsp;'
        f'<span style="color:#8C1D18;font-weight:700">({range_label})</span></div>',
        unsafe_allow_html=True)

    # Column label: total across range
    ov_col_lbl = f'{val_label} Total'
    headers = ['#', 'Location', ov_col_lbl, 'Monthly Avg', 'Total Closed']
    if secondary_cols:
        headers += list(secondary_cols[0].keys())
    widths = [0.3, 2.8, 1.1, 1, 1] + ([1] * len(secondary_cols[0]) if secondary_cols else [])

    hcols = st.columns(widths)
    for h, c in zip(headers, hcols):
        c.markdown(
            f"<div style='font-size:0.6rem;font-weight:700;color:#9aa3b0;"
            f"text-transform:uppercase;letter-spacing:0.1em;padding-bottom:5px;"
            f"border-bottom:2px solid #e2e6ed'>{h}</div>",
            unsafe_allow_html=True)

    for i, item in enumerate(top20_data, 1):
        bg, fg  = ov_color(item['last_ov'], t_hi, t_lo)
        rank    = f'<span style="color:#8b949e;font-size:0.8rem">{i}</span>'
        row_bg  = 'background:#fafbfc;' if i % 2 == 0 else ''
        dcols   = st.columns(widths)
        dcols[0].markdown(
            f"<div style='font-size:0.8rem;padding:5px 0;{row_bg}'>{rank}</div>",
            unsafe_allow_html=True)
        dcols[1].markdown(
            f"<div style='font-size:0.8rem;padding:5px 0;font-weight:500;color:#0f1c2e;{row_bg}'>"
            f"{item['loc']}</div>", unsafe_allow_html=True)
        dcols[2].markdown(
            f"<div style='padding:5px 0;{row_bg}'>"
            f"<span class='ov-pill' style='background:{bg};color:{fg}'>{item['last_ov']}</span>"
            f"</div>", unsafe_allow_html=True)
        dcols[3].markdown(
            f"<div style='font-size:0.8rem;padding:5px 0;color:#5a6577;{row_bg}'>"
            f"{item['avg_ov']}</div>", unsafe_allow_html=True)
        dcols[4].markdown(
            f"<div style='font-size:0.8rem;padding:5px 0;color:#5a6577;{row_bg}'>"
            f"{item['total_closed']:,}</div>", unsafe_allow_html=True)
        if secondary_cols:
            for j, (k, v) in enumerate(secondary_cols[i - 1].items(), 5):
                dcols[j].markdown(
                    f"<div style='font-size:0.8rem;padding:5px 0;color:#57606a;{row_bg}'>{v}</div>",
                    unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# PAGE HEADER
# ══════════════════════════════════════════════════════════════════
title_loc = (selected_loc_label if selected_loc != 'ALL'
             else (selected_region if selected_region != 'ALL REGIONS' else 'All Locations'))

# Context pill for region/location
if selected_loc != 'ALL':
    rname_display = selected_region.strip('[]') if selected_region != 'ALL REGIONS' else ''
    ctx_color = REGION_COLORS.get(rname_display, '#8C1D18') if rname_display else '#8C1D18'
    loc_pill = (f'<span style="background:{ctx_color}22;color:{ctx_color};padding:2px 10px;'
                f'border-radius:4px;font-size:0.72rem;font-weight:600">{selected_loc_label}</span>')
elif selected_region != 'ALL REGIONS':
    rname_display = selected_region.strip('[]')
    ctx_color = REGION_COLORS.get(rname_display, '#8C1D18')
    nlocs = len(region_map.get(rname_display, []))
    loc_pill = (f'<span style="background:{ctx_color}22;color:{ctx_color};padding:2px 10px;'
                f'border-radius:4px;font-size:0.72rem;font-weight:600">'
                f'{rname_display} &nbsp;·&nbsp; {nlocs} labs</span>')
else:
    loc_pill = (f'<span class="pill">{len(all_locations)} locations</span> '
                f'<span class="pill pill-green">{len(region_map)} regions</span>')

date_filtered = start_idx != 0 or end_idx != NM - 1
# Data range = full span of uploaded file(s)
data_range_label = f"{all_months[0]} – {all_months[-1]}"
# Filtered badge — specific about what the filter is showing
if date_filtered:
    n_months = end_idx - start_idx + 1
    date_badge = (f'<span class="date-range-badge">'
                  f'📅 Filtered: {start_month} – {end_month} &nbsp;·&nbsp; {n_months} mo'
                  f'</span>')
else:
    date_badge = ''

st.markdown(f"""
<div class="page-header">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:0.5rem">
    <div>
      <div class="page-title">CAPA · PTO Performance Dashboard</div>
      <div class="page-meta" style="margin-top:0.4rem">
        {loc_pill}
        <span class="dot">·</span>
        <span style="color:#6a8aaa;font-size:0.68rem;text-transform:uppercase;
                     letter-spacing:0.06em;font-weight:600">Data:</span>
        <span style="font-family:'DM Mono',monospace;color:#c8d6e8;font-size:0.72rem">
          {data_range_label}
        </span>
        <span class="dot">·</span>
        <span>Updated {D['loaded_at'].split()[0]}</span>
      </div>
    </div>
    <div>{date_badge}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════
tab_car, tab_pto, tab_combined = st.tabs(["📘  CARs", "📗  PTOs", "📊  Combined"])

def render_tab(metrics_key, wavg_key, theme_key, closed_label, t_hi, t_lo,
               top20_data, val_label, show_split=False, secondary_cols_fn=None):
    full_metrics  = get_full(metrics_key)
    full_wavg     = get_full(wavg_key)
    sliced_m      = get_sliced(metrics_key)
    sliced_w      = get_sliced(wavg_key)
    colors        = THEME[theme_key]

    scorecard(sliced_m, sliced_w, colors, closed_label, t_hi, t_lo)
    st.markdown('<div style="margin-top:1.1rem"></div>', unsafe_allow_html=True)

    col_chart, col_gap, col_table = st.columns([2.4, 0.05, 1])
    with col_chart:
        fig = build_chart(sliced_m, sliced_w, colors,
                          f"{closed_label.replace(' Closed','')} Performance — {title_loc}",
                          show_split=show_split)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    with col_table:
        st.markdown('<div class="section-hdr">Monthly Breakdown</div>', unsafe_allow_html=True)
        extra = None
        if show_split:
            extra = [{'CAR >90': r.get('ov90_car', 0), 'PTO >90': r.get('ov90_pto', 0)}
                     for r in sliced_m]
        st.dataframe(monthly_table(sliced_m, sliced_w, extra_cols=extra),
                     use_container_width=True, hide_index=True,
                     height=min(380, 36 + len(slice_months) * 35))

    st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)

    # Build Top 20 dynamically — uses sliced range so it responds to sidebar filters
    stats_key = metrics_key.replace('_metrics', '_stats')
    stats     = D[stats_key]
    per_loc_metrics_key = metrics_key  # e.g. 'car_metrics'

    # Get locations in scope for current filter
    if data_key == 'ALL':
        scope_locs = D['all_locations']
    elif data_key.startswith('REGION:'):
        region = data_key[7:]
        scope_locs = [l for l in D['all_locations']
                      if l in D['region_map'].get(region, [])]
    else:
        scope_locs = [data_key] if data_key in stats else []

    # Compute per-location stats across the FULL SELECTED SLICE
    all_loc_metrics = D[per_loc_metrics_key]

    def loc_slice(loc):
        lm = all_loc_metrics.get(loc, [])
        return lm[start_idx:end_idx + 1] if lm else []

    def loc_total_ov(loc):
        s = loc_slice(loc)
        return sum(r['ov90'] for r in s) if s else 0

    def loc_avg_ov(loc):
        s = loc_slice(loc)
        if not s: return 0
        return round(sum(r['ov90'] for r in s) / len(s), 1)

    def loc_total_closed(loc):
        s = loc_slice(loc)
        return sum(r['closed'] for r in s) if s else 0

    ranked = sorted(scope_locs, key=loc_total_ov, reverse=True)[:20]
    dynamic_top20 = [{'loc': l,
                      'last_ov':      loc_total_ov(l),   # reuse key; now = slice total
                      'avg_ov':       loc_avg_ov(l),
                      'total_closed': loc_total_closed(l)} for l in ranked]

    sec = secondary_cols_fn(dynamic_top20) if secondary_cols_fn else None
    top20_table(dynamic_top20, t_hi, t_lo, val_label, secondary_cols=sec)

with tab_car:
    render_tab('car_metrics', 'car_wavg', 'car', 'CARs Closed',
               D['car_t_hi'], D['car_t_lo'], D['car_top20'], 'CARs Ov90')

with tab_pto:
    render_tab('pto_metrics', 'pto_wavg', 'pto', 'PTOs Closed',
               D['pto_t_hi'], D['pto_t_lo'], D['pto_top20'], 'PTOs Ov90')

with tab_combined:
    def cmb_sec(top20):
        car_lm = D['car_metrics']
        pto_lm = D['pto_metrics']
        def get_slice_ov(loc_metrics, loc):
            lm = loc_metrics.get(loc, [])
            s = lm[start_idx:end_idx + 1] if lm else []
            return sum(r['ov90'] for r in s) if s else 0
        return [{'CAR >90': get_slice_ov(car_lm, i['loc']),
                 'PTO >90': get_slice_ov(pto_lm, i['loc'])} for i in top20]
    render_tab('cmb_metrics', 'cmb_wavg', 'combined', 'Total Closed (CARs + PTOs)',
               D['cmb_t_hi'], D['cmb_t_lo'], D['cmb_top20'], 'Total Ov90',
               show_split=True, secondary_cols_fn=cmb_sec)

# ── Export ────────────────────────────────────────────────────────
if export_reg_btn:
    buf = export_regional_summary(D, as_of_date=datetime.now().strftime("%b %d, %Y"))
    st.sidebar.download_button(
        "⬇ Download Regional Summary", buf,
        file_name=f"CAPA_Regional_Summary_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
