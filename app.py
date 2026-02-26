import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import io

from data_engine import load_and_compute, REGION_ORDER, REGION_COLORS
from export_utils import export_excel

st.set_page_config(page_title="CAPA · PTO Dashboard", page_icon="📋",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&display=swap');
html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
section[data-testid="stSidebar"] { background:#0f1923; border-right:1px solid #1e2d3d; }
section[data-testid="stSidebar"] * { color:#c8d6e5 !important; }
section[data-testid="stSidebar"] label { color:#7f9ab5 !important; font-size:0.72rem !important;
    text-transform:uppercase; letter-spacing:0.08em; }
.main { background:#f0f4f8; }
.block-container { padding:1.2rem 1.8rem; }
.metric-card { background:white; border-radius:10px; padding:1rem 1.2rem; border-left:4px solid;
    box-shadow:0 1px 4px rgba(0,0,0,0.07); height:100%; }
.metric-val  { font-size:1.9rem; font-weight:700; line-height:1; }
.metric-lbl  { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.08em;
    color:#6b7c93; margin-top:0.3rem; }
.metric-sub  { font-size:0.75rem; color:#8899aa; margin-top:0.15rem; }
.metric-ye   { margin-top:0.5rem; padding-top:0.5rem; border-top:1px solid #eee; }
.metric-ye-lbl { font-size:0.68rem; color:#8899aa; text-transform:uppercase; letter-spacing:0.06em; }
.metric-ye-val { font-size:0.95rem; font-weight:700; }
.section-hdr { font-size:0.68rem; font-weight:600; text-transform:uppercase; letter-spacing:0.12em;
    color:#6b7c93; border-bottom:1px solid #dde3ea; padding-bottom:0.3rem;
    margin-bottom:0.8rem; margin-top:1.2rem; }
.stTabs [data-baseweb="tab-list"] { gap:4px; background:#e4eaf1; padding:4px; border-radius:10px; }
.stTabs [data-baseweb="tab"] { border-radius:7px; padding:0.35rem 1.1rem; font-size:0.82rem;
    font-weight:500; color:#4a5568; background:transparent; border:none; }
.stTabs [aria-selected="true"] { background:white !important; color:#1a202c !important;
    box-shadow:0 1px 3px rgba(0,0,0,0.1); }
.ov-pill { padding:2px 10px; border-radius:20px; font-size:0.78rem; font-weight:600; }
.stDownloadButton button { background:#1a202c; color:white; border:none;
    border-radius:7px; font-size:0.8rem; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:0.8rem 0 1.2rem">
      <div style="font-size:1.05rem;font-weight:700;color:#e2e8f0">CAPA · PTO</div>
      <div style="font-size:0.68rem;color:#4a6080;text-transform:uppercase;letter-spacing:0.1em;margin-top:2px">
        Performance Dashboard</div>
    </div>""", unsafe_allow_html=True)

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

if selected_region == 'ALL REGIONS':
    filtered_locs = all_locations
    region_key    = 'ALL'
else:
    rname         = selected_region.strip('[]')
    filtered_locs = sorted(region_map.get(rname, []))
    region_key    = f'REGION:{rname}'

with location_placeholder:
    selected_loc = st.selectbox("Location", ['ALL'] + filtered_locs, key="loc_sel",
                                label_visibility="collapsed")

# ── Resolve data key ──────────────────────────────────────────────
# ALL + ALL REGIONS = global ALL
# ALL + specific region = that region aggregate
# specific loc = that location
if selected_loc == 'ALL':
    data_key = region_key
else:
    data_key = selected_loc

# ── Helpers ───────────────────────────────────────────────────────
def ov_color(val, t_hi, t_lo):
    if val > t_hi:   return '#ef4444', '#fff'
    elif val > t_lo: return '#f59e0b', '#1a1a1a'
    else:            return '#22c55e', '#fff'

THEME = {
    'car':      {'primary':'#2563eb', 'line':'#dc2626', 'wavg':'#64748b'},
    'pto':      {'primary':'#16a34a', 'line':'#991b1b', 'wavg':'#64748b'},
    'combined': {'primary':'#7c3aed', 'line':'#dc2626', 'wavg':'#64748b'},
}

def get_metrics(key): return D[key].get(data_key, D[key]['ALL'])
def get_wavg(key):    return D[key].get(data_key, D[key]['ALL'])

# ── Scorecard ─────────────────────────────────────────────────────
def scorecard(metrics, wavg_vals, colors, closed_label, t_hi, t_lo):
    months_list  = D['month_labels']
    NM           = len(months_list)
    last_dec_idx = D['last_dec_idx']
    last_dec_yr  = D['last_dec_year']
    last_month   = months_list[-1]
    prev_month   = months_list[-2] if NM > 1 else months_list[-1]
    ye_label     = f"{last_dec_yr} YE"

    closed_list = [r['closed']   for r in metrics]
    avg_list    = [r['avg_days'] for r in metrics if r['closed'] > 0]
    ov_list     = [r['ov90']     for r in metrics]

    total_closed = sum(closed_list)
    avg_days_val = int(round(np.mean(avg_list))) if avg_list else 0
    avg_ov90     = int(round(np.mean(ov_list)))
    last_ov      = ov_list[-1]
    prev_ov      = ov_list[-2] if NM > 1 else last_ov
    cur_wavg     = wavg_vals[-1]
    ye_wavg      = wavg_vals[last_dec_idx]

    ye_closed    = sum(r['closed']   for r in metrics[:last_dec_idx+1])
    ye_avg_list  = [r['avg_days']    for r in metrics[:last_dec_idx+1] if r['closed'] > 0]
    ye_avg_days  = int(round(np.mean(ye_avg_list))) if ye_avg_list else 0
    ye_avg_ov90  = int(round(np.mean([r['ov90'] for r in metrics[:last_dec_idx+1]])))
    ye_last_ov   = ov_list[last_dec_idx]

    trend        = '▲ Worse' if last_ov > prev_ov else ('▼ Improved' if last_ov < prev_ov else '→ Flat')
    trend_color  = '#ef4444' if '▲' in trend else ('#22c55e' if '▼' in trend else '#6b7c93')
    ye_ov_color  = ov_color(ye_last_ov, t_hi, t_lo)[0]

    def card(border, val_color, val_size, val, lbl, sub, ye_color, ye_val):
        return f"""
        <div class="metric-card" style="border-color:{border}">
          <div class="metric-val" style="color:{val_color};font-size:{val_size}">{val}</div>
          <div class="metric-lbl">{lbl}</div>
          <div class="metric-sub">{sub}</div>
          <div class="metric-ye">
            <span class="metric-ye-lbl">{ye_label} &nbsp;</span>
            <span class="metric-ye-val" style="color:{ye_color}">{ye_val}</span>
          </div>
        </div>"""

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(card(colors['primary'], colors['primary'], '1.9rem',
            f"{total_closed:,}", closed_label, "Full period total",
            colors['primary'], f"{ye_closed:,}"), unsafe_allow_html=True)
    with c2:
        st.markdown(card('#64748b', '#1a202c', '1.9rem',
            avg_days_val, "Avg Days to Close", "Simple monthly avg",
            '#64748b', ye_avg_days), unsafe_allow_html=True)
    with c3:
        bg, _ = ov_color(avg_ov90, t_hi, t_lo)
        st.markdown(card(bg, bg, '1.9rem',
            avg_ov90, "Avg Open ≥90 Days", "Monthly average",
            ye_ov_color, ye_avg_ov90), unsafe_allow_html=True)
    with c4:
        st.markdown(card(trend_color, trend_color, '1.3rem',
            trend, f"{last_month} vs {prev_month}", "Open ≥90 trend",
            ye_ov_color, f"Dec ov90: {ye_last_ov}"), unsafe_allow_html=True)
    with c5:
        st.markdown(f"""
        <div class="metric-card" style="border-color:{colors['wavg']}">
          <div class="metric-val" style="color:{colors['primary']};font-size:1.9rem">{cur_wavg}</div>
          <div class="metric-lbl">Wtd Avg Days Closed</div>
          <div class="metric-sub">{last_month} YTD running avg</div>
          <div class="metric-ye">
            <span class="metric-ye-lbl">{ye_label} &nbsp;</span>
            <span class="metric-ye-val" style="color:{colors['primary']}">{ye_wavg}</span>
          </div>
        </div>""", unsafe_allow_html=True)

# ── Chart ─────────────────────────────────────────────────────────
def build_chart(month_labels, metrics, wavg_vals, colors, title, show_split=False):
    closed   = [r['closed']   for r in metrics]
    avg_days = [r['avg_days'] for r in metrics]
    ov90     = [r['ov90']     for r in metrics]

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Bar(x=month_labels, y=closed, name="Closed",
        marker_color=colors['primary'], opacity=0.85,
        hovertemplate="<b>%{x}</b><br>Closed: %{y}<extra></extra>"), secondary_y=False)

    if show_split:
        fig.add_trace(go.Bar(x=month_labels, y=[r.get('ov90_car',0) for r in metrics],
            name="CARs Open ≥90", marker_color='#f97316', opacity=0.85,
            hovertemplate="<b>%{x}</b><br>CARs ≥90: %{y}<extra></extra>"), secondary_y=False)
        fig.add_trace(go.Bar(x=month_labels, y=[r.get('ov90_pto',0) for r in metrics],
            name="PTOs Open ≥90", marker_color='#fbbf24', opacity=0.7,
            hovertemplate="<b>%{x}</b><br>PTOs ≥90: %{y}<extra></extra>"), secondary_y=False)
    else:
        fig.add_trace(go.Bar(x=month_labels, y=ov90, name="Open ≥90 Days",
            marker_color='#f97316', opacity=0.85,
            hovertemplate="<b>%{x}</b><br>Open ≥90: %{y}<extra></extra>"), secondary_y=False)

    fig.add_trace(go.Scatter(x=month_labels, y=avg_days, name="Avg Days to Close",
        mode="lines+markers", line=dict(color=colors['line'], width=2.5), marker=dict(size=6),
        hovertemplate="<b>%{x}</b><br>Avg Days: %{y:.0f}<extra></extra>"), secondary_y=True)

    fig.add_trace(go.Scatter(x=month_labels, y=wavg_vals, name="Wtd Avg Days (YTD)",
        mode="lines+markers", line=dict(color=colors['wavg'], width=2, dash='dash'),
        marker=dict(size=5),
        hovertemplate="<b>%{x}</b><br>Wtd Avg: %{y:.0f}<extra></extra>"), secondary_y=True)

    # Year boundary lines
    for yr_idx in D['year_end_indices'].values():
        if yr_idx < len(month_labels) - 1:
            next_label = month_labels[yr_idx + 1]
            yr = D['month_labels'][yr_idx + 1][:4]
            fig.add_vline(x=yr_idx + 0.5, line_dash='dot', line_color='#ef4444',
                          line_width=1.5, annotation_text=yr,
                          annotation_position='top right',
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
def monthly_table(metrics, wavg_vals, extra_cols=None):
    rows = []
    for i, (m, r, w) in enumerate(zip(D['month_labels'], metrics, wavg_vals)):
        prev_ov = metrics[i-1]['ov90'] if i > 0 else None
        ov      = r['ov90']
        trend   = '—' if prev_ov is None else ('▲' if ov > prev_ov else ('▼' if ov < prev_ov else '→'))
        row = {'Month': m, 'Closed': r['closed'], 'Avg Days': r['avg_days'],
               'Open ≥90': ov, 'Wtd Avg': w, 'Trend': trend}
        if extra_cols:
            row.update(extra_cols[i])
        rows.append(row)
    df = pd.DataFrame(rows)
    def color_trend(v):
        if v == '▲': return 'color:#ef4444;font-weight:600'
        if v == '▼': return 'color:#22c55e;font-weight:600'
        return 'color:#6b7c93'
    return (df.style
              .applymap(color_trend, subset=['Trend'])
              .format({'Avg Days': '{:.0f}', 'Open ≥90': '{:.0f}', 'Wtd Avg': '{:.0f}'}))

# ── Top 20 ────────────────────────────────────────────────────────
def top20_table(top20_data, t_hi, t_lo, val_label, secondary_cols=None):
    st.markdown(f'<div class="section-hdr">⚠ Top 20 — Open ≥90 Days (most recent month)</div>',
                unsafe_allow_html=True)
    headers = ['#', 'Location', val_label, 'Avg Ov90', 'Total Closed']
    if secondary_cols: headers += list(secondary_cols[0].keys())
    widths = [0.3, 2.5, 1, 1, 1] + ([1] * len(secondary_cols[0]) if secondary_cols else [])
    hcols = st.columns(widths)
    for h, c in zip(headers, hcols):
        c.markdown(f"<div style='font-size:0.68rem;font-weight:600;color:#6b7c93;text-transform:uppercase'>{h}</div>",
                   unsafe_allow_html=True)
    for i, item in enumerate(top20_data, 1):
        bg, fg = ov_color(item['last_ov'], t_hi, t_lo)
        medal  = ['🥇','🥈','🥉'][i-1] if i <= 3 else str(i)
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
        st.markdown(f'<div style="font-size:0.78rem;color:#6b7c93;margin-bottom:0.8rem">'
                    f'Showing: <b>All Locations</b> ({len(all_locations)} labs)</div>',
                    unsafe_allow_html=True)
    elif selected_loc == 'ALL':
        rname = selected_region.strip('[]')
        color = REGION_COLORS.get(rname, '#888')
        nlocs = len(region_map.get(rname, []))
        st.markdown(f'<div style="font-size:0.78rem;color:#6b7c93;margin-bottom:0.8rem">'
                    f'Region: <span style="background:{color};color:white;padding:1px 8px;'
                    f'border-radius:10px;font-weight:600">{rname}</span> &nbsp;{nlocs} locations</div>',
                    unsafe_allow_html=True)
    else:
        rname = selected_region.strip('[]') if selected_region != 'ALL REGIONS' else ''
        color = REGION_COLORS.get(rname, '#888')
        prefix = (f'<span style="background:{color};color:white;padding:1px 8px;border-radius:10px;'
                  f'font-weight:600">{rname}</span> &nbsp;→&nbsp;') if rname else ''
        st.markdown(f'<div style="font-size:0.78rem;color:#6b7c93;margin-bottom:0.8rem">'
                    f'{prefix}<b>{selected_loc}</b></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# MAIN LAYOUT
# ══════════════════════════════════════════════════════════════════
month_labels = D['month_labels']
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
  Data as of {D['loaded_at'].split()[0]} &nbsp;·&nbsp;
  {month_labels[0]} – {month_labels[-1]} &nbsp;·&nbsp;
  {len(all_locations)} locations &nbsp;·&nbsp; {len(region_map)} regions
</div>
""", unsafe_allow_html=True)

region_info_bar()

tab_car, tab_pto, tab_combined = st.tabs(["📘  CARs", "📗  PTOs", "📊  Combined"])

with tab_car:
    metrics   = get_metrics('car_metrics')
    wavg_vals = get_wavg('car_wavg')
    t_hi, t_lo = D['car_t_hi'], D['car_t_lo']
    scorecard(metrics, wavg_vals, THEME['car'], 'CARs Closed', t_hi, t_lo)
    st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)
    col_chart, col_table = st.columns([2.2, 1])
    with col_chart:
        st.plotly_chart(build_chart(month_labels, metrics, wavg_vals, THEME['car'],
            f"CAR Performance — {title_loc}"), use_container_width=True,
            config={'displayModeBar': False})
    with col_table:
        st.markdown('<div class="section-hdr">Monthly Breakdown</div>', unsafe_allow_html=True)
        st.dataframe(monthly_table(metrics, wavg_vals),
                     use_container_width=True, hide_index=True, height=380)
    top20_table(D['car_top20'], t_hi, t_lo, 'CARs Ov90')

with tab_pto:
    metrics   = get_metrics('pto_metrics')
    wavg_vals = get_wavg('pto_wavg')
    t_hi, t_lo = D['pto_t_hi'], D['pto_t_lo']
    scorecard(metrics, wavg_vals, THEME['pto'], 'PTOs Closed', t_hi, t_lo)
    st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)
    col_chart, col_table = st.columns([2.2, 1])
    with col_chart:
        st.plotly_chart(build_chart(month_labels, metrics, wavg_vals, THEME['pto'],
            f"PTO Performance — {title_loc}"), use_container_width=True,
            config={'displayModeBar': False})
    with col_table:
        st.markdown('<div class="section-hdr">Monthly Breakdown</div>', unsafe_allow_html=True)
        st.dataframe(monthly_table(metrics, wavg_vals),
                     use_container_width=True, hide_index=True, height=380)
    top20_table(D['pto_top20'], t_hi, t_lo, 'PTOs Ov90')

with tab_combined:
    metrics   = get_metrics('cmb_metrics')
    wavg_vals = get_wavg('cmb_wavg')
    t_hi, t_lo = D['cmb_t_hi'], D['cmb_t_lo']
    scorecard(metrics, wavg_vals, THEME['combined'], 'Total Closed (CARs + PTOs)', t_hi, t_lo)
    st.markdown('<div style="margin-top:1rem"></div>', unsafe_allow_html=True)
    col_chart, col_table = st.columns([2.2, 1])
    with col_chart:
        st.plotly_chart(build_chart(month_labels, metrics, wavg_vals, THEME['combined'],
            f"CAR + PTO Performance — {title_loc}", show_split=True),
            use_container_width=True, config={'displayModeBar': False})
    with col_table:
        st.markdown('<div class="section-hdr">Monthly Breakdown</div>', unsafe_allow_html=True)
        extra = [{'CAR ≥90': r.get('ov90_car',0), 'PTO ≥90': r.get('ov90_pto',0)} for r in metrics]
        st.dataframe(monthly_table(metrics, wavg_vals, extra_cols=extra),
                     use_container_width=True, hide_index=True, height=380)
    car_stats = D['car_stats']; pto_stats = D['pto_stats']
    sec_cols = [{'CAR ≥90': car_stats[item['loc']]['last_ov'],
                 'PTO ≥90': pto_stats[item['loc']]['last_ov']} for item in D['cmb_top20']]
    top20_table(D['cmb_top20'], t_hi, t_lo, 'Total Ov90', secondary_cols=sec_cols)

if export_btn:
    buf = export_excel(D, data_key, title_loc)
    st.sidebar.download_button("⬇ Download Excel", buf,
        file_name=f"CAPA_Dashboard_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
