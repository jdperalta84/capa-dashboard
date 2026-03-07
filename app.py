import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import io

from data_engine import load_and_compute, REGION_ORDER, REGION_COLORS
from export_utils import export_regional_summary

st.set_page_config(page_title="CAPA · PTO Dashboard", page_icon="📋",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #0d1117;
    border-right: 1px solid #21262d;
}
section[data-testid="stSidebar"] * { color: #c9d1d9 !important; }
section[data-testid="stSidebar"] .stSelectbox > div > div {
    background: #161b22 !important;
    border: 1px solid #30363d !important;
    border-radius: 6px !important;
    color: #c9d1d9 !important;
}
section[data-testid="stSidebar"] .stFileUploader {
    background: #161b22;
    border: 1px dashed #30363d;
    border-radius: 8px;
    padding: 0.5rem;
}
section[data-testid="stSidebar"] .stButton > button {
    background: #21262d !important;
    color: #c9d1d9 !important;
    border: 1px solid #30363d !important;
    border-radius: 6px !important;
    font-size: 0.78rem !important;
    padding: 0.4rem 0.8rem !important;
    width: 100%;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: #30363d !important;
    border-color: #58a6ff !important;
}

/* ── Main canvas ── */
.main { background: #f6f8fa; }
.block-container { padding: 1.5rem 2rem 2rem; max-width: 1400px; }

/* ── Page header ── */
.page-header {
    padding: 1.2rem 1.5rem;
    background: white;
    border-radius: 12px;
    border: 1px solid #e1e4e8;
    margin-bottom: 1.2rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
.page-title {
    font-size: 1.35rem; font-weight: 700; color: #0d1117;
    letter-spacing: -0.02em; margin: 0; line-height: 1.2;
}
.page-meta {
    font-size: 0.75rem; color: #8b949e; margin-top: 0.3rem;
    display: flex; align-items: center; gap: 0.6rem; flex-wrap: wrap;
}
.page-meta .dot { color: #d0d7de; }
.page-meta .pill {
    background: #f0f6ff; color: #0550ae;
    padding: 1px 8px; border-radius: 20px;
    font-size: 0.7rem; font-weight: 500;
}
.page-meta .pill-green { background: #dafbe1; color: #116329; }
.date-range-badge {
    background: #fff8e1; color: #7d5a00;
    padding: 2px 10px; border-radius: 20px;
    font-size: 0.72rem; font-weight: 600;
    border: 1px solid #f0c040;
}

/* ── Metric cards ── */
.cards-row { display: flex; gap: 0.9rem; margin-bottom: 1.2rem; }
.metric-card {
    background: white; border-radius: 10px;
    padding: 1.1rem 1.3rem; border-left: 3px solid;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    flex: 1; min-width: 0;
    transition: box-shadow 0.15s;
}
.metric-card:hover { box-shadow: 0 3px 8px rgba(0,0,0,0.1); }
.metric-val   { font-size: 1.8rem; font-weight: 700; line-height: 1; letter-spacing: -0.02em; }
.metric-lbl   { font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.09em;
                color: #8b949e; margin-top: 0.35rem; font-weight: 500; }
.metric-sub   { font-size: 0.72rem; color: #adb5bd; margin-top: 0.1rem; }
.metric-divider { height: 1px; background: #f0f0f0; margin: 0.6rem 0; }
.metric-ye-lbl  { font-size: 0.62rem; color: #adb5bd; text-transform: uppercase;
                  letter-spacing: 0.08em; font-weight: 500; }
.metric-ye-val  { font-size: 0.9rem; font-weight: 600; }

/* ── Section headers ── */
.section-hdr {
    font-size: 0.62rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.12em; color: #8b949e;
    border-bottom: 1px solid #e1e4e8;
    padding-bottom: 0.35rem; margin-bottom: 0.9rem; margin-top: 1.4rem;
}
.sidebar-section {
    font-size: 0.6rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.12em; color: #484f58;
    padding-bottom: 0.3rem; margin-bottom: 0.6rem; margin-top: 1.1rem;
    border-bottom: 1px solid #21262d;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px; background: #eaedf0; padding: 3px; border-radius: 9px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 6px; padding: 0.4rem 1.2rem; font-size: 0.8rem;
    font-weight: 500; color: #57606a; background: transparent; border: none;
}
.stTabs [aria-selected="true"] {
    background: white !important; color: #0d1117 !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    font-weight: 600 !important;
}

/* ── Overdue pill ── */
.ov-pill {
    padding: 2px 11px; border-radius: 20px;
    font-size: 0.76rem; font-weight: 600;
    display: inline-block;
}

/* ── Top 20 table ── */
.top20-hdr {
    font-size: 0.62rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.1em; color: #8b949e; padding: 0.4rem 0;
    border-bottom: 2px solid #e1e4e8; margin-bottom: 0.2rem;
}
.top20-row {
    padding: 0.35rem 0; border-bottom: 1px solid #f6f8fa;
    font-size: 0.82rem;
}

/* ── Chart container ── */
.chart-wrap {
    background: white; border-radius: 10px;
    border: 1px solid #e1e4e8; padding: 0.5rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}

/* ── Download button ── */
.stDownloadButton > button {
    background: #0d1117 !important; color: white !important;
    border: none !important; border-radius: 6px !important;
    font-size: 0.78rem !important;
}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="padding:1rem 0 0.5rem">
      <div style="font-size:1rem;font-weight:700;color:#e6edf3;letter-spacing:-0.01em">
        CAPA · PTO
      </div>
      <div style="font-size:0.65rem;color:#484f58;text-transform:uppercase;
                  letter-spacing:0.12em;margin-top:3px">
        Performance Dashboard
      </div>
    </div>
    <div style="height:1px;background:#21262d;margin-bottom:0.5rem"></div>
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
    exclude_jn = st.toggle("Exclude JN PTOs", value=True)

    st.markdown('<div class="sidebar-section">Export</div>', unsafe_allow_html=True)
    export_reg_btn = st.button("↓  Regional Summary (Excel)", use_container_width=True)
    st.markdown("""
    <div style="font-size:0.65rem;color:#484f58;margin-top:0.3rem;line-height:1.5">
      PDF: browser Print → Save as PDF
    </div>""", unsafe_allow_html=True)

    st.markdown('<div style="height:1px;background:#21262d;margin:1rem 0 0.5rem"></div>',
                unsafe_allow_html=True)
    loaded_at_ph = st.empty()

# ══════════════════════════════════════════════════════════════════
# LOAD DATA
# ══════════════════════════════════════════════════════════════════
@st.cache_data(show_spinner="Computing metrics…")
def get_data(file_bytes, exclude_jn=True):
    return load_and_compute(io.BytesIO(file_bytes), exclude_jn=exclude_jn)

if uploaded_files:
    file_bytes = uploaded_files[0].read()
    file_hash  = hash(file_bytes)
    jn_changed = st.session_state.get("exclude_jn") != exclude_jn
    if ("data" not in st.session_state
            or st.session_state.get("file_hash") != file_hash
            or load_btn or jn_changed):
        st.session_state.exclude_jn = exclude_jn
        try:
            get_data.clear()
            st.session_state.data = get_data(file_bytes, exclude_jn)
            st.session_state.file_hash = file_hash
            st.session_state.filename = uploaded_files[0].name
        except Exception as e:
            st.error(f"Error loading file: {e}")
            st.stop()
elif "data" not in st.session_state:
    st.markdown("""
    <div style="display:flex;align-items:center;justify-content:center;
                height:60vh;flex-direction:column;gap:1rem">
      <div style="font-size:2.5rem">📋</div>
      <div style="font-size:1.1rem;font-weight:600;color:#0d1117">Upload your Excel file</div>
      <div style="font-size:0.85rem;color:#8b949e;text-align:center;max-width:320px">
        Drag and drop your CAPA/PTO Excel file into the sidebar uploader to get started.
      </div>
    </div>""", unsafe_allow_html=True)
    st.stop()

D = st.session_state.data

with loaded_at_ph:
    fname = st.session_state.get("filename", "")
    st.markdown(f"""
    <div style="font-size:0.65rem;color:#484f58;line-height:1.6">
      <div style="color:#8b949e">Last loaded</div>
      <div style="color:#c9d1d9">{D['loaded_at']}</div>
      <div style="color:#484f58;margin-top:2px;word-break:break-all">{fname}</div>
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# FILTERS
# ══════════════════════════════════════════════════════════════════
region_map     = D['region_map']
all_locations  = D['all_locations']
all_months     = D['month_labels']
NM             = len(all_months)

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

with location_placeholder:
    selected_loc = st.selectbox("Location", ['ALL'] + filtered_locs,
                                key="loc_sel", label_visibility="collapsed")

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
    if val > t_hi:   return '#ef4444', '#fff'
    elif val > t_lo: return '#f59e0b', '#1a1a1a'
    else:            return '#22c55e', '#fff'

THEME = {
    'car':      {'primary': '#2563eb', 'bar2': '#f97316', 'line': '#dc2626', 'wavg': '#94a3b8'},
    'pto':      {'primary': '#16a34a', 'bar2': '#fb923c', 'line': '#991b1b', 'wavg': '#94a3b8'},
    'combined': {'primary': '#7c3aed', 'bar2': '#f97316', 'line': '#dc2626', 'wavg': '#94a3b8'},
}

def get_full(key):  return D[key].get(data_key, D[key]['ALL'])
def get_sliced(key):return slice_data(D[key].get(data_key, D[key]['ALL']))

# ══════════════════════════════════════════════════════════════════
# SCORECARD  (always full data range — no slicing)
# ══════════════════════════════════════════════════════════════════
def scorecard(metrics, wavg_vals, colors, closed_label, t_hi, t_lo):
    NM           = len(metrics)
    last_month   = slice_months[-1] if slice_months else all_months[-1]
    prev_month   = slice_months[-2] if len(slice_months) > 1 else last_month

    # YE benchmark: always last complete year (dynamic, not hardcoded)
    last_dec_yr  = D['last_dec_year']
    last_dec_idx_full = D['last_dec_idx']
    ye_label     = f"{last_dec_yr} YE"

    # Find Dec position within sliced range (may be outside slice)
    dec_in_slice = last_dec_idx_full - start_idx
    has_dec      = 0 <= dec_in_slice < NM

    closed_list  = [r['closed']   for r in metrics]
    avg_list     = [r['avg_days'] for r in metrics if r['closed'] > 0]
    ov_list      = [r['ov90']     for r in metrics]

    total_closed = sum(closed_list)
    avg_days_val = int(round(np.mean(avg_list))) if avg_list else 0
    avg_ov90     = int(round(np.mean(ov_list))) if ov_list else 0
    last_ov      = ov_list[-1] if ov_list else 0
    prev_ov      = ov_list[-2] if NM > 1 else last_ov
    cur_wavg     = wavg_vals[-1] if wavg_vals else 0

    # YE values — from full data if Dec is outside slice, else from slice
    full_m  = get_full('car_metrics' if closed_label == 'CARs Closed'
                       else ('pto_metrics' if closed_label == 'PTOs Closed'
                             else 'cmb_metrics'))
    full_w  = get_full('car_wavg'    if closed_label == 'CARs Closed'
                       else ('pto_wavg'    if closed_label == 'PTOs Closed'
                             else 'cmb_wavg'))
    # YE calc: only the 12 months of last_dec_year (not everything up to Dec)
    all_months_full = D['month_labels']
    ye_start_idx = next((i for i, m in enumerate(all_months_full)
                         if m.endswith(str(last_dec_yr))), last_dec_idx_full - 11)
    ye_slice     = full_m[ye_start_idx:last_dec_idx_full + 1]
    ye_closed   = sum(r['closed']   for r in ye_slice)
    ye_avg_list = [r['avg_days']   for r in ye_slice if r['closed'] > 0]
    ye_avg_days = int(round(np.mean(ye_avg_list))) if ye_avg_list else 0
    ye_avg_ov90 = int(round(np.mean([r['ov90'] for r in ye_slice])))
    ye_last_ov  = full_m[last_dec_idx_full]['ov90']
    ye_wavg     = full_w[last_dec_idx_full]

    trend        = '▲ Worse'   if last_ov > prev_ov else ('▼ Improved' if last_ov < prev_ov else '→ Flat')
    trend_color  = '#ef4444'   if '▲' in trend else ('#22c55e' if '▼' in trend else '#6b7c93')
    ye_ov_color  = ov_color(ye_last_ov, t_hi, t_lo)[0]
    ye_trend_lbl = f"Dec {last_dec_yr}: {ye_last_ov} open over 90d"

    def card(border, val_color, val_size, val, lbl, sub, ye_color, ye_val):
        return f"""
        <div class="metric-card" style="border-color:{border}">
          <div class="metric-val"  style="color:{val_color};font-size:{val_size}">{val}</div>
          <div class="metric-lbl">{lbl}</div>
          <div class="metric-sub">{sub}</div>
          <div class="metric-divider"></div>
          <div class="metric-ye-lbl">{ye_label}</div>
          <div class="metric-ye-val" style="color:{ye_color}">{ye_val}</div>
        </div>"""

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(card(colors['primary'], colors['primary'], '1.8rem',
            f"{total_closed:,}", closed_label, f"{start_month} – {last_month}",
            colors['primary'], f"{ye_closed:,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(card('#64748b', '#0d1117', '1.8rem',
            avg_days_val, "Avg Days to Close", "Simple monthly avg",
            '#64748b', ye_avg_days), unsafe_allow_html=True)
    with c3:
        bg, _ = ov_color(avg_ov90, t_hi, t_lo)
        st.markdown(card(bg, bg, '1.8rem',
            avg_ov90, "Avg Open ≥90 Days", "Monthly average",
            ye_ov_color, ye_avg_ov90), unsafe_allow_html=True)
    with c4:
        st.markdown(card(trend_color, trend_color, '1.25rem',
            trend, f"{last_month} vs {prev_month}", "Open ≥90 trend",
            ye_ov_color, ye_trend_lbl), unsafe_allow_html=True)
    with c5:
        st.markdown(f"""
        <div class="metric-card" style="border-color:{colors['wavg']}">
          <div class="metric-val" style="color:{colors['primary']};font-size:1.8rem">{cur_wavg}</div>
          <div class="metric-lbl">Wtd Avg Days Closed</div>
          <div class="metric-sub">{last_month} YTD running avg</div>
          <div class="metric-divider"></div>
          <div class="metric-ye-lbl">{ye_label}</div>
          <div class="metric-ye-val" style="color:{colors['primary']}">{ye_wavg}</div>
        </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# CHART  (sliced to selected date range)
# ══════════════════════════════════════════════════════════════════
def build_chart(sliced_metrics, sliced_wavg, colors, title, show_split=False):
    closed   = [r['closed']   for r in sliced_metrics]
    avg_days = [r['avg_days'] for r in sliced_metrics]
    ov90     = [r['ov90']     for r in sliced_metrics]

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
            name="CARs Open ≥90", marker_color='#f97316', opacity=0.8,
            marker_line_width=0,
            hovertemplate="<b>%{x}</b><br>CARs ≥90: %{y}<extra></extra>"),
            secondary_y=False)
        fig.add_trace(go.Bar(
            x=slice_months, y=[r.get('ov90_pto', 0) for r in sliced_metrics],
            name="PTOs Open ≥90", marker_color='#fbbf24', opacity=0.8,
            marker_line_width=0,
            hovertemplate="<b>%{x}</b><br>PTOs ≥90: %{y}<extra></extra>"),
            secondary_y=False)
    else:
        fig.add_trace(go.Bar(
            x=slice_months, y=ov90, name="Open ≥90 Days",
            marker_color=colors['bar2'], opacity=0.8,
            marker_line_width=0,
            hovertemplate="<b>%{x}</b><br>Open ≥90: %{y}<extra></extra>"),
            secondary_y=False)

    fig.add_trace(go.Scatter(
        x=slice_months, y=avg_days, name="Avg Days to Close",
        mode="lines+markers",
        line=dict(color=colors['line'], width=2.5),
        marker=dict(size=5, color=colors['line']),
        hovertemplate="<b>%{x}</b><br>Avg Days: %{y:.0f}<extra></extra>"),
        secondary_y=True)

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
        title=dict(text=title, font=dict(size=12, color='#0d1117'), x=0, y=0.97),
        barmode='group',
        plot_bgcolor='white', paper_bgcolor='white',
        legend=dict(
            orientation='h', yanchor='bottom', y=1.01,
            xanchor='right', x=1,
            font=dict(size=9, color='#57606a'),
            bgcolor='rgba(0,0,0,0)',
        ),
        margin=dict(l=0, r=0, t=44, b=0),
        hovermode='x unified',
        xaxis=dict(
            tickfont=dict(size=9, color='#8b949e'),
            showgrid=False, zeroline=False,
            linecolor='#e1e4e8',
        ),
        yaxis=dict(
            title=dict(text='Count', font=dict(size=10, color='#8b949e')),
            gridcolor='#f6f8fa', zeroline=False,
            tickfont=dict(size=9, color='#8b949e'),
        ),
        yaxis2=dict(
            title=dict(text='Days', font=dict(size=10, color='#8b949e')),
            gridcolor='#f6f8fa', zeroline=False,
            tickfont=dict(size=9, color='#8b949e'),
        ),
        height=310,
    )
    return fig

# ══════════════════════════════════════════════════════════════════
# MONTHLY TABLE  (sliced)
# ══════════════════════════════════════════════════════════════════
def monthly_table(sliced_metrics, sliced_wavg, extra_cols=None):
    rows = []
    for i, (m, r, w) in enumerate(zip(slice_months, sliced_metrics, sliced_wavg)):
        prev_ov = sliced_metrics[i - 1]['ov90'] if i > 0 else None
        ov      = r['ov90']
        trend   = '—' if prev_ov is None else ('▲' if ov > prev_ov else ('▼' if ov < prev_ov else '→'))
        row = {'Month': m, 'Closed': r['closed'],
               'Avg Days': r['avg_days'], 'Open ≥90': ov, 'Wtd Avg': w, 'Trend': trend}
        if extra_cols:
            row.update(extra_cols[i])
        rows.append(row)
    df = pd.DataFrame(rows)

    def color_trend(v):
        if v == '▲': return 'color:#ef4444;font-weight:600'
        if v == '▼': return 'color:#22c55e;font-weight:600'
        return 'color:#94a3b8'

    return (df.style
              .applymap(color_trend, subset=['Trend'])
              .format({'Avg Days': '{:.0f}', 'Open ≥90': '{:.0f}', 'Wtd Avg': '{:.0f}'}))

# ══════════════════════════════════════════════════════════════════
# TOP 20  (uses last month of sliced range)
# ══════════════════════════════════════════════════════════════════
def top20_table(top20_data, t_hi, t_lo, val_label, secondary_cols=None):
    end_label = slice_months[-1] if slice_months else all_months[-1]
    st.markdown(
        f'<div class="section-hdr">⚠ &nbsp;Top 20 Locations — Open ≥90 Days &nbsp;'
        f'<span style="color:#0550ae;font-weight:700">({end_label})</span></div>',
        unsafe_allow_html=True)

    headers = ['#', 'Location', val_label, 'Avg Ov90', 'Total Closed']
    if secondary_cols:
        headers += list(secondary_cols[0].keys())
    widths = [0.3, 2.8, 1.1, 1, 1] + ([1] * len(secondary_cols[0]) if secondary_cols else [])

    hcols = st.columns(widths)
    for h, c in zip(headers, hcols):
        c.markdown(
            f"<div style='font-size:0.62rem;font-weight:600;color:#8b949e;"
            f"text-transform:uppercase;letter-spacing:0.08em;padding-bottom:4px;"
            f"border-bottom:2px solid #e1e4e8'>{h}</div>",
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
            f"<div style='font-size:0.8rem;padding:5px 0;font-weight:500;color:#0d1117;{row_bg}'>"
            f"{item['loc']}</div>", unsafe_allow_html=True)
        dcols[2].markdown(
            f"<div style='padding:5px 0;{row_bg}'>"
            f"<span class='ov-pill' style='background:{bg};color:{fg}'>{item['last_ov']}</span>"
            f"</div>", unsafe_allow_html=True)
        dcols[3].markdown(
            f"<div style='font-size:0.8rem;padding:5px 0;color:#57606a;{row_bg}'>"
            f"{item['avg_ov']}</div>", unsafe_allow_html=True)
        dcols[4].markdown(
            f"<div style='font-size:0.8rem;padding:5px 0;color:#57606a;{row_bg}'>"
            f"{item['total_closed']:,}</div>", unsafe_allow_html=True)
        if secondary_cols:
            for j, (k, v) in enumerate(secondary_cols[i - 1].items(), 5):
                dcols[j].markdown(
                    f"<div style='font-size:0.8rem;padding:5px 0;color:#57606a;{row_bg}'>{v}</div>",
                    unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# PAGE HEADER
# ══════════════════════════════════════════════════════════════════
title_loc = (selected_loc if selected_loc != 'ALL'
             else (selected_region if selected_region != 'ALL REGIONS' else 'All Locations'))

# Context pill for region/location
if selected_loc != 'ALL':
    rname_display = selected_region.strip('[]') if selected_region != 'ALL REGIONS' else ''
    ctx_color = REGION_COLORS.get(rname_display, '#0550ae') if rname_display else '#0550ae'
    loc_pill = (f'<span style="background:{ctx_color}22;color:{ctx_color};padding:2px 10px;'
                f'border-radius:20px;font-size:0.72rem;font-weight:600">{selected_loc}</span>')
elif selected_region != 'ALL REGIONS':
    rname_display = selected_region.strip('[]')
    ctx_color = REGION_COLORS.get(rname_display, '#0550ae')
    nlocs = len(region_map.get(rname_display, []))
    loc_pill = (f'<span style="background:{ctx_color}22;color:{ctx_color};padding:2px 10px;'
                f'border-radius:20px;font-size:0.72rem;font-weight:600">'
                f'{rname_display} &nbsp;·&nbsp; {nlocs} labs</span>')
else:
    loc_pill = (f'<span class="pill">{len(all_locations)} locations</span> '
                f'<span class="pill pill-green">{len(region_map)} regions</span>')

date_filtered = start_idx != 0 or end_idx != NM - 1
date_badge = (f'<span class="date-range-badge">📅 {start_month} – {end_month}</span>'
              if date_filtered else '')

st.markdown(f"""
<div class="page-header">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:0.5rem">
    <div>
      <div class="page-title">CAPA · PTO Performance Dashboard</div>
      <div class="page-meta" style="margin-top:0.4rem">
        {loc_pill}
        <span class="dot">·</span>
        <span>{all_months[0]} – {all_months[-1]}</span>
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
            extra = [{'CAR ≥90': r.get('ov90_car', 0), 'PTO ≥90': r.get('ov90_pto', 0)}
                     for r in sliced_m]
        st.dataframe(monthly_table(sliced_m, sliced_w, extra_cols=extra),
                     use_container_width=True, hide_index=True,
                     height=min(380, 36 + len(slice_months) * 35))

    st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)

    # Build Top 20 dynamically from selected region/location
    stats_key = (metrics_key.replace('_metrics', '_stats'))
    stats     = D[stats_key]
    # Get locations in scope for current filter
    if data_key == 'ALL':
        scope_locs = D['all_locations']
    elif data_key.startswith('REGION:'):
        region = data_key[7:]
        scope_locs = [l for l in D['all_locations']
                      if l in D['region_map'].get(region, [])]
    else:
        scope_locs = [data_key] if data_key in stats else []
    ranked = sorted(scope_locs, key=lambda l: stats[l]['last_ov'], reverse=True)[:20]
    dynamic_top20 = [{'loc': l, 'last_ov': stats[l]['last_ov'],
                      'avg_ov': stats[l]['avg_ov'],
                      'total_closed': stats[l]['total_closed']} for l in ranked]

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
        cs = D['car_stats']; ps = D['pto_stats']
        return [{'CAR ≥90': cs[i['loc']]['last_ov'],
                 'PTO ≥90': ps[i['loc']]['last_ov']} for i in top20]
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
