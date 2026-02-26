import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
from datetime import datetime
import io

from data_engine import load_and_compute, REGION_ORDER, REGION_COLORS
from export_utils import export_excel

# ── Page config ───────────────────────────────────────────────────
st.set_page_config(
    page_title="CAPA · PTO Dashboard",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

section[data-testid="stSidebar"] { background:#0f1923; border-right:1px solid #1e2d3d; }
section[data-testid="stSidebar"] * { color:#c8d6e5 !important; }
section[data-testid="stSidebar"] label { color:#7f9ab5 !important; font-size:0.72rem !important; text-transform:uppercase; letter-spacing:0.08em; }

.main { background:#f0f4f8; }
.block-container { padding:1.2rem 1.8rem; }

.metric-card { background:white; border-radius:10px; padding:1rem 1.2rem; border-left:4px solid; box-shadow:0 1px 4px rgba(0,0,0,0.07); }
.metric-card .val { font-size:1.9rem; font-weight:700; line-height:1; }
.metric-card .lbl { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.08em; color:#6b7c93; margin-top:0.3rem; }
.metric-card .sub { font-size:0.75rem; color:#8899aa; margin-top:0.15rem; }

.section-hdr { font-size:0.68rem; font-weight:600; text-transform:uppercase; letter-spacing:0.12em; color:#6b7c93; border-bottom:1px solid #dde3ea; padding-bottom:0.3rem; margin-bottom:0.8rem; margin-top:1.2rem; }

.stTabs [data-baseweb="tab-list"] { gap:4px; background:#e4eaf1; padding:4px; border-radius:10px; }
.stTabs [data-baseweb="tab"] { border-radius:7px; padding:0.35rem 1.1rem; font-size:0.82rem; font-weight:500; color:#4a5568; background:transparent; border:none; }
.stTabs [aria-selected="true"] { background:white !important; color:#1a202c !important; box-shadow:0 1px 3px rgba(0,0,0,0.1); }

.region-badge { display:inline-block; padding:2px 10px; border-radius:20px; font-size:0.72rem; font-weight:600; margin-right:4px; margin-bottom:3px; }
.ov-pill { padding:2px 10px; border-radius:20px; font-size:0.78rem; font-weight:600; }

.stDownloadButton button { background:#1a202c; color:white; border:none; border-radius:7px; font-size:0.8rem; }
.stDownloadButton button:hover { background:#2d3748; }

.wavg-box { background:white; border-radius:8px; padding:0.7rem 1rem; border:1px solid #dde3ea; font-size:0.82rem; }
.wavg-box .yr { font-size:1.1rem; font-weight:700; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:0.8rem 0 1.2rem">
      <div style="font-size:1.05rem;font-weight:700;color:#e2e8f0;letter-spacing:-0.02em">CAPA · PTO</div>
      <div style="font-size:0.68rem;color:#4a6080;text-transform:uppercase;letter-spacing:0.1em;margin-top:2px">Performance Dashboard</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-hdr">Data Source</div>', unsafe_allow_html=True)
    uploaded_file = st.file_uploader("Upload Excel file", type=["xlsx"],
                                     label_visibility="collapsed")
    load_btn = st.button("⟳  Reload", use_container_width=True)

    st.markdown('<div class="section-hdr">Region Filter</div>', unsafe_allow_html=True)
    region_placeholder = st.empty()

    st.markdown('<div class="section-hdr">Location Filter</div>', unsafe_allow_html=True)
    location_placeholder = st.empty()

    st.markdown('<div class="section-hdr">Export</div>', unsafe_allow_html=True)
    export_btn = st.button("↓  Export to Excel", use_container_width=True)
    st.caption("PDF: use browser Print → Save as PDF (Ctrl+P)")

    st.markdown("---")
    loaded_at_placeholder = st.empty()

# ── Load data ─────────────────────────────────────────────────────
@st.cache_data(show_spinner="Computing metrics…")
def get_data(file_bytes):
    import io
    return load_and_compute(io.BytesIO(file_bytes))

if uploaded_file is not None:
    file_bytes = uploaded_file.read()
    file_hash  = hash(file_bytes)
    if "data" not in st.session_state or st.session_state.get("file_hash") != file_hash or load_btn:
        try:
            get_data.clear()
            st.session_state.data      = get_data(file_bytes)
            st.session_state.file_hash = file_hash
            st.session_state.filename  = uploaded_file.name
        except Exception as e:
            st.error(f"Error loading file: {e}")
            st.stop()
elif "data" not in st.session_state:
    st.info("👆 Upload your Excel file in the sidebar to get started.")
    st.stop()

D = st.session_state.data

with loaded_at_placeholder:
    fname = st.session_state.get("filename", "")
    st.markdown(f'<div style="font-size:0.68rem;color:#2d4a6a">Loaded: {D["loaded_at"]}<br>{fname}</div>',
                unsafe_allow_html=True)

# ── Region + Location filters ─────────────────────────────────────
region_map    = D['region_map']
all_locations = D['all_locations']

region_options = ['ALL REGIONS'] + [f'[{r}]' for r in REGION_ORDER if r in region_map]

with region_placeholder:
    selected_region = st.selectbox("Region", region_options, key="region_sel",
                                   label_visibility="collapsed")

# Determine which locations to show based on region
if selected_region == 'ALL REGIONS':
    filtered_locs = all_locations
    region_key    = 'ALL'
else:
    rname         = selected_region.strip('[]')
    filtered_locs = sorted(region_map.get(rname, []))
    region_key    = f'REGION:{rname}'

with location_placeholder:
    loc_options  = ['ALL'] + filtered_locs
    selected_loc = st.selectbox("Location", loc_options, key="loc_sel",
                                label_visibility="collapsed")

# Resolve the actual data key to use
if selected_loc == 'ALL':
    data_key = region_key   # either 'ALL' or 'REGION:xxx'
else:
    data_key = selected_loc

# ── Helpers ───────────────────────────────────────────────────────
def ov_color(val, t_hi, t_lo):
    if val > t_hi:   return '#ef4444', '#fff'
    elif val > t_lo: return '#f59e0b', '#1a1a1a'
    else:            return '#22c55e', '#fff'

THEME = {
    'car':      {'primary':'#2563eb','secondary':'#f97316','line':'#dc2626','wavg':'#64748b'},
    'pto':      {'primary':'#16a34a','secondary':'#fb923c','line':'#991b1b','wavg':'#64748b'},
    'combined': {'primary':'#7c3aed','secondary':'#f97316','line':'#dc2626','wavg':'#64748b'},
}

def get_metrics(metric_key):
    return D[metric_key].get(data_key, D[metric_key]['ALL'])

def get_wavg(wavg_key):
    return D[wavg_key].get(data_key, D[wavg_key]['ALL'])

# ── Scorecard ─────────────────────────────────────────────────────
def scorecard(metrics, wavg_vals, colors, closed_label, t_hi, t_lo):
    closed_list = [r['closed']   for r in metrics]
    avg_list    = [r['avg_days'] for r in metrics if r['closed'] > 0]
    ov_list     = [r['ov90']     for r in metrics]

    total_closed = sum(closed_list)
    avg_days_val = int(round(np.mean(avg_list))) if avg_list else 0
    avg_ov90     = int(round(np.mean(ov_list)))
    last_ov      = ov_list[-1]
    prev_ov      = ov_list[-2]
    ye2025       = wavg_vals[D['DEC2025_IDX']]
    ytd2026      = wavg_vals[-1]

    trend       = '▲ Worse' if last_ov > prev_ov else ('▼ Improved' if last_ov < prev_ov else '→ Flat')
    trend_color = '#ef4444' if '▲' in trend else ('#22c55e' if '▼' in trend else '#6b7c93')

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(f"""<div class="metric-card" style="border-color:{colors['primary']}">
            <div class="val" style="color:{colors['primary']}">{total_closed:,}</div>
            <div class="lbl">{closed_label}</div>
            <div class="sub">Full period total</div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="metric-card" style="border-color:#64748b">
            <div class="val" style="color:#1a202c">{avg_days_val}</div>
            <div class="lbl">Avg Days to Close</div>
            <div class="sub">Simple monthly avg</div></div>""", unsafe_allow_html=True)
    with c3:
        bg, fg = ov_color(avg_ov90, t_hi, t_lo)
        st.markdown(f"""<div class="metric-card" style="border-color:{bg}">
            <div class="val" style="color:{bg}">{avg_ov90}</div>
            <div class="lbl">Avg Open ≥90 Days</div>
            <div class="sub">Monthly average</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="metric-card" style="border-color:{trend_color}">
            <div class="val" style="color:{trend_color};font-size:1.3rem">{trend}</div>
            <div class="lbl">Feb vs Jan 2026</div>
            <div class="sub">Open ≥90 trend</div></div>""", unsafe_allow_html=True)
    with c5:
        st.markdown(f"""<div class="metric-card" style="border-color:{colors['wavg']}">
            <div class="wavg-box">
              <div style="font-size:0.68rem;text-transform:uppercase;letter-spacing:0.08em;color:#6b7c93">Wtd Avg Days Closed</div>
              <div style="margin-top:0.4rem">
                <span style="font-size:0.72rem;color:#6b7c93">2025 YE KPI &nbsp;</span>
                <span class="yr" style="color:{colors['primary']}">{ye2025}</span>
              </div>
              <div style="margin-top:0.2rem">
                <span style="font-size:0.72rem;color:#6b7c93">2026 YTD &nbsp;&nbsp;&nbsp;&nbsp;</span>
                <span class="yr" style="color:{colors['wavg']}">{ytd2026}</span>
              </div>
            </div></div>""", unsafe_allow_html=True)

# ── Chart ─────────────────────────────────────────────────────────
def build_chart(months, metrics, wavg_vals, colors, title, show_car_pto_split=False):
    closed   = [r['closed']   for r in metrics]
    avg_days = [r['avg_days'] for r in metrics]
    ov90     = [r['ov90']     for r in metrics]

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Bar(x=months, y=closed, name="Closed",
        marker_color=colors['primary'], opacity=0.85,
        hovertemplate="<b>%{x}</b><br>Closed: %{y}<extra></extra>"), secondary_y=False)

    if show_car_pto_split:
        ov_car = [r.get('ov90_car', 0) for r in metrics]
        ov_pto = [r.get('ov90_pto', 0) for r in metrics]
        fig.add_trace(go.Bar(x=months, y=ov_car, name="CARs Open ≥90",
            marker_color='#f97316', opacity=0.85,
            hovertemplate="<b>%{x}</b><br>CARs ≥90: %{y}<extra></extra>"), secondary_y=False)
        fig.add_trace(go.Bar(x=months, y=ov_pto, name="PTOs Open ≥90",
            marker_color='#fbbf24', opacity=0.7,
            hovertemplate="<b>%{x}</b><br>PTOs ≥90: %{y}<extra></extra>"), secondary_y=False)
    else:
        fig.add_trace(go.Bar(x=months, y=ov90, name="Open ≥90 Days",
            marker_color=colors['secondary'], opacity=0.85,
            hovertemplate="<b>%{x}</b><br>Open ≥90: %{y}<extra></extra>"), secondary_y=False)

    fig.add_trace(go.Scatter(x=months, y=avg_days, name="Avg Days to Close",
        mode="lines+markers", line=dict(color=colors['line'], width=2.5),
        marker=dict(size=6),
        hovertemplate="<b>%{x}</b><br>Avg Days: %{y:.0f}<extra></extra>"),
        secondary_y=True)

    fig.add_trace(go.Scatter(x=months, y=wavg_vals, name="Wtd Avg Days (YTD)",
        mode="lines+markers", line=dict(color=colors['wavg'], width=2, dash='dash'),
        marker=dict(size=5),
        hovertemplate="<b>%{x}</b><br>Wtd Avg: %{y:.0f}<extra></extra>"),
        secondary_y=True)

    # Year boundary annotation
    if 'Jan 2026' in months:
        jan26_x = months.index('Jan 2026')
        fig.add_vline(x=jan26_x - 0.5, line_dash='dot', line_color='#ef4444',
                      line_width=1.5, annotation_text='2026', annotation_position='top right',
                      annotation_font_size=10, annotation_font_color='#ef4444')

    fig.update_layout(
        title=dict(text=title, font=dict(size=13, family='DM Sans', color='#1a202c'), x=0),
        barmode='group', plot_bgcolor='white', paper_bgcolor='white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1,
                    font=dict(size=10)),
        margin=dict(l=0, r=0, t=40, b=0), hovermode='x unified',
        xaxis=dict(tickfont=dict(size=9), showgrid=False),
        yaxis=dict(title='Count', gridcolor='#f0f0f0', zeroline=False),
        yaxis2=dict(title='Days', gridcolor='#f0f0f0', zeroline=False),
        height=320,
    )
    return fig

# ── Monthly table ─────────────────────────────────────────────────
def monthly_table(months, metrics, wavg_vals, extra_cols=None):
    rows = []
    for i, (m, r, w) in enumerate(zip(months, metrics, wavg_vals)):
        prev_ov = metrics[i-1]['ov90'] if i > 0 else None
        ov = r['ov90']
        trend = '—' if prev_ov is None else ('▲' if ov > prev_ov else ('▼' if ov < prev_ov else '→'))
        row = {'Month': m, 'Closed': r['closed'],
               'Avg Days': r['avg_days'], 'Open ≥90': ov,
               'Wtd Avg': w, 'Trend': trend}
        if extra_cols:
            for k, v in extra_cols[i].items():
                row[k] = v
        rows.append(row)
    df = pd.DataFrame(rows)

    def color_trend(v):
        if v == '▲': return 'color: #ef4444; font-weight:600'
        if v == '▼': return 'color: #22c55e; font-weight:600'
        return 'color: #6b7c93'

    return df.style\
        .applymap(color_trend, subset=['Trend'])\
        .format({'Avg Days': '{:.0f}', 'Open ≥90': '{:.0f}', 'Wtd Avg': '{:.0f}'})\
        .set_properties(**{'font-size': '12px'})

# ── Top 20 table ──────────────────────────────────────────────────
def top20_table(top20_data, t_hi, t_lo, val_label, secondary_cols=None):
    st.markdown('<div class="section-hdr">⚠ Top 20 Locations — Open ≥90 Days (Feb 2026, most recent month)</div>',
                unsafe_allow_html=True)

    header_cols = ['#', 'Location', val_label, 'Avg Ov90', 'Total Closed']
    if secondary_cols:
        header_cols += list(secondary_cols[0].keys())

    widths = [0.3, 2.5, 1, 1, 1] + ([1] * len(secondary_cols[0]) if secondary_cols else [])
    hcols = st.columns(widths)
    for h, c in zip(header_cols, hcols):
        c.markdown(f"<div style='font-size:0.68rem;font-weight:600;color:#6b7c93;text-transform:uppercase'>{h}</div>",
                   unsafe_allow_html=True)

    for i, item in enumerate(top20_data, 1):
        bg, fg = ov_color(item['last_ov'], t_hi, t_lo)
        medal  = ['🥇','🥈','🥉'][i-1] if i <= 3 else str(i)
        row_bg = '#fafafa' if i % 2 == 0 else 'white'
        dcols  = st.columns(widths)

        dcols[0].markdown(f"<div style='font-size:0.82rem;padding:3px 0'>{medal}</div>", unsafe_allow_html=True)
        dcols[1].markdown(f"<div style='font-size:0.82rem;padding:3px 0;font-weight:500'>{item['loc']}</div>", unsafe_allow_html=True)
        dcols[2].markdown(f"<span class='ov-pill' style='background:{bg};color:{fg}'>{item['last_ov']}</span>", unsafe_allow_html=True)
        dcols[3].markdown(f"<div style='font-size:0.82rem;padding:3px 0;color:#4a5568'>{item['avg_ov']}</div>", unsafe_allow_html=True)
        dcols[4].markdown(f"<div style='font-size:0.82rem;padding:3px 0;color:#4a5568'>{item['total_closed']:,}</div>", unsafe_allow_html=True)

        if secondary_cols:
            for j, (k, v) in enumerate(secondary_cols[i-1].items(), 5):
                dcols[j].markdown(f"<div style='font-size:0.82rem;padding:3px 0;color:#4a5568'>{v}</div>", unsafe_allow_html=True)

# ── Region info bar ───────────────────────────────────────────────
def region_info_bar():
    if selected_region == 'ALL REGIONS' and selected_loc == 'ALL':
        st.markdown(f'<div style="font-size:0.78rem;color:#6b7c93;margin-bottom:0.8rem">Showing: <b>All Locations</b> ({len(all_locations)} labs)</div>',
                    unsafe_allow_html=True)
    elif selected_loc == 'ALL':
        rname  = selected_region.strip('[]')
        color  = REGION_COLORS.get(rname, '#888')
        nlocs  = len(region_map.get(rname, []))
        st.markdown(f'<div style="font-size:0.78rem;color:#6b7c93;margin-bottom:0.8rem">'
                    f'Region: <span style="background:{color};color:white;padding:1px 8px;border-radius:10px;font-weight:600">{rname}</span>'
                    f'  &nbsp;{nlocs} locations</div>', unsafe_allow_html=True)
    else:
        rname = selected_region.strip('[]') if selected_region != 'ALL REGIONS' else ''
        color = REGION_COLORS.get(rname, '#888')
        st.markdown(f'<div style="font-size:0.78rem;color:#6b7c93;margin-bottom:0.8rem">'
                    f'{"<span style=background:" + color + ";color:white;padding:1px 8px;border-radius:10px;font-weight:600>" + rname + "</span>&nbsp;→&nbsp;" if rname else ""}'
                    f'<b>{selected_loc}</b></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# MAIN LAYOUT
# ══════════════════════════════════════════════════════════════════
months = D['month_labels']

title_loc = selected_loc if selected_loc != 'ALL' else (
    selected_region if selected_region != 'ALL REGIONS' else 'All Locations')

st.markdown(f"""
<div style="display:flex;align-items:baseline;gap:1rem;margin-bottom:0.2rem">
  <h1 style="font-size:1.5rem;font-weight:700;color:#1a202c;margin:0;letter-spacing:-0.03em">
    CAPA · PTO Performance Dashboard
  </h1>
  <span style="font-size:0.75rem;color:#6b7c93">{title_loc}</span>
</div>
<div style="font-size:0.75rem;color:#8899aa;margin-bottom:1.2rem">
  Data as of 02/24/2026 &nbsp;·&nbsp; Jan 2025 – Feb 2026 &nbsp;·&nbsp;
  {len(all_locations)} locations &nbsp;·&nbsp; {len(region_map)} regions
</div>
""", unsafe_allow_html=True)

region_info_bar()

tab_car, tab_pto, tab_combined = st.tabs(["📘  CARs", "📗  PTOs", "📊  Combined"])

# ── CARs Tab ──────────────────────────────────────────────────────
with tab_car:
    metrics   = get_metrics('car_metrics')
    wavg_vals = get_wavg('car_wavg')
    t_hi, t_lo = D['car_t_hi'], D['car_t_lo']

    scorecard(metrics, wavg_vals, THEME['car'], 'CARs Closed', t_hi, t_lo)
    st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)

    col_chart, col_table = st.columns([2.2, 1])
    with col_chart:
        fig = build_chart(months, metrics, wavg_vals, THEME['car'],
                          f"CAR Performance — {title_loc}")
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    with col_table:
        st.markdown('<div class="section-hdr">Monthly Breakdown</div>', unsafe_allow_html=True)
        st.dataframe(monthly_table(months, metrics, wavg_vals),
                     use_container_width=True, hide_index=True, height=380)

    top20_table(D['car_top20'], t_hi, t_lo, 'CARs Ov90 (Feb)')

# ── PTOs Tab ──────────────────────────────────────────────────────
with tab_pto:
    metrics   = get_metrics('pto_metrics')
    wavg_vals = get_wavg('pto_wavg')
    t_hi, t_lo = D['pto_t_hi'], D['pto_t_lo']

    scorecard(metrics, wavg_vals, THEME['pto'], 'PTOs Closed', t_hi, t_lo)
    st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)

    col_chart, col_table = st.columns([2.2, 1])
    with col_chart:
        fig = build_chart(months, metrics, wavg_vals, THEME['pto'],
                          f"PTO Performance — {title_loc}")
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    with col_table:
        st.markdown('<div class="section-hdr">Monthly Breakdown</div>', unsafe_allow_html=True)
        st.dataframe(monthly_table(months, metrics, wavg_vals),
                     use_container_width=True, hide_index=True, height=380)

    top20_table(D['pto_top20'], t_hi, t_lo, 'PTOs Ov90 (Feb)')

# ── Combined Tab ──────────────────────────────────────────────────
with tab_combined:
    metrics   = get_metrics('cmb_metrics')
    wavg_vals = get_wavg('cmb_wavg')
    t_hi, t_lo = D['cmb_t_hi'], D['cmb_t_lo']

    scorecard(metrics, wavg_vals, THEME['combined'], 'Total Closed (CARs + PTOs)', t_hi, t_lo)
    st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)

    col_chart, col_table = st.columns([2.2, 1])
    with col_chart:
        fig = build_chart(months, metrics, wavg_vals, THEME['combined'],
                          f"CAR + PTO Performance — {title_loc}",
                          show_car_pto_split=True)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
    with col_table:
        st.markdown('<div class="section-hdr">Monthly Breakdown</div>', unsafe_allow_html=True)
        extra = [{'CAR ≥90': r.get('ov90_car', 0), 'PTO ≥90': r.get('ov90_pto', 0)}
                 for r in metrics]
        st.dataframe(monthly_table(months, metrics, wavg_vals, extra_cols=extra),
                     use_container_width=True, hide_index=True, height=380)

    # Combined top 20 with CAR/PTO split
    car_stats = D['car_stats']
    pto_stats = D['pto_stats']
    sec_cols  = [{'CAR ≥90': car_stats[item['loc']]['last_ov'],
                  'PTO ≥90': pto_stats[item['loc']]['last_ov']}
                 for item in D['cmb_top20']]
    top20_table(D['cmb_top20'], t_hi, t_lo, 'Total Ov90 (Feb)', secondary_cols=sec_cols)

# ── Export ────────────────────────────────────────────────────────
if export_btn:
    buf = export_excel(D, data_key, title_loc)
    st.sidebar.download_button(
        "⬇ Download Excel", buf,
        file_name=f"CAPA_Dashboard_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
