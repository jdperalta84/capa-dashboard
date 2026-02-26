"""
data_engine.py
Accepts either a file path string OR a BytesIO object (Streamlit Cloud uploads).
Auto-detects header row (row 1 or row 2) and CAR effectiveness column name.
"""

import io
import pandas as pd
import numpy as np
from pathlib import Path


REGION_ORDER = ['USWC', 'USGC', 'USNE, USMW & Canada', 'SE & Caribbean', 'Corporate', 'Calibration']

REGION_COLORS = {
    'USWC':                '#2E86C1',
    'USGC':                '#CA6F1E',
    'USNE, USMW & Canada': '#28B463',
    'SE & Caribbean':      '#A569BD',
    'Corporate':           '#566573',
    'Calibration':         '#C0392B',
}

SKIP_LOCS = ['A&B Labs', 'VOIDED', 'Extras', 'Warehouse', 'Additives',
             'Utah', 'Cameron', 'Specialty', 'Kenner', 'Santurce', 'Boucherville']


def _read_source(source, sheet_name, header=0):
    """Read excel, seeking BytesIO to 0 first."""
    if isinstance(source, io.BytesIO):
        source.seek(0)
    return pd.read_excel(source, sheet_name=sheet_name, header=header)


def load_and_compute(file_source) -> dict:
    # Normalise source
    if isinstance(file_source, (str, Path)):
        source      = str(file_source)
        source_name = Path(file_source).name
    else:
        file_source.seek(0)
        source      = io.BytesIO(file_source.read())
        source_name = "uploaded file"

    # ── List source (always header=0) ─────────────────────────────
    ls = _read_source(source, 'List source', header=0)

    # ── Auto-detect header row for CARs/PTOs ──────────────────────
    # Try header=0 first; if 'Location \n(drop-down)' not found, use header=1
    car_raw = _read_source(source, 'Data - CARs', header=0)
    car_raw.columns = car_raw.columns.str.strip()
    if 'Location \n(drop-down)' not in car_raw.columns:
        car_raw = _read_source(source, 'Data - CARs', header=1)
        car_raw.columns = car_raw.columns.str.strip()

    pto_raw = _read_source(source, 'Open data - PTOs', header=0)
    pto_raw.columns = pto_raw.columns.str.strip()
    if 'Location \n(drop-down)' not in pto_raw.columns:
        pto_raw = _read_source(source, 'Open data - PTOs', header=1)
        pto_raw.columns = pto_raw.columns.str.strip()

    # ── Auto-detect CAR effectiveness column name ─────────────────
    car_eff_cols = [c for c in car_raw.columns if 'Effectiveness' in str(c) or 'deemed' in str(c).lower()]
    if not car_eff_cols:
        raise ValueError(f"Cannot find effectiveness column in CARs sheet. Columns: {car_raw.columns.tolist()}")
    car_eff_col = car_eff_cols[0]

    pto_eff_cols = [c for c in pto_raw.columns if 'Effectiveness' in str(c) or 'deemed' in str(c).lower()]
    if not pto_eff_cols:
        raise ValueError(f"Cannot find effectiveness column in PTOs sheet. Columns: {pto_raw.columns.tolist()}")
    pto_eff_col = pto_eff_cols[0]

    # ── Auto-detect date range from data ──────────────────────────
    car_raw['_init'] = pd.to_datetime(car_raw['CAR initialized date'], errors='coerce')
    pto_raw['_init'] = pd.to_datetime(pto_raw['PTO initialized date'], errors='coerce')
    all_dates = pd.concat([car_raw['_init'].dropna(), pto_raw['_init'].dropna()])
    data_start = all_dates.min().to_period('M')
    # End: use close dates to find latest activity
    car_raw['_close'] = pd.to_datetime(car_raw[car_eff_col], errors='coerce')
    pto_raw['_close'] = pd.to_datetime(pto_raw[pto_eff_col], errors='coerce')
    all_close = pd.concat([car_raw['_close'].dropna(), pto_raw['_close'].dropna()])
    data_end = all_close.max().to_period('M')

    months       = pd.period_range(data_start, data_end, freq='M')
    month_labels = [m.strftime('%b %Y') for m in months]
    NM           = len(months)

    # Year-end indices
    year_end_indices = {}
    for i, m in enumerate(months):
        if m.month == 12:
            year_end_indices[m.year] = i
    # Use last December in data as primary KPI reference
    last_dec_year = max(year_end_indices.keys()) if year_end_indices else None
    last_dec_idx  = year_end_indices.get(last_dec_year, NM - 1)
    prev_dec_year = last_dec_year - 1 if last_dec_year else None
    prev_dec_idx  = year_end_indices.get(prev_dec_year, None)

    # ── Master location list ──────────────────────────────────────
    ls = ls[ls['Location'].notna() & ls['Area'].notna()].copy()
    ls['Location'] = ls['Location'].str.strip()
    ls['Area']     = ls['Area'].str.strip()
    ls = ls[~ls['Location'].apply(lambda x: any(s in str(x) for s in SKIP_LOCS))]
    ls = ls[ls['Area'] != 'VOID']
    region_map    = {}
    for _, row in ls.iterrows():
        region_map.setdefault(row['Area'], []).append(row['Location'])
    all_locations = sorted(ls['Location'].unique().tolist())

    # ── Prep CARs ─────────────────────────────────────────────────
    car = car_raw.copy()
    car['loc']        = car['Location \n(drop-down)'].astype(str).str.strip()
    car['init_date']  = pd.to_datetime(car['CAR initialized date'], errors='coerce')
    car['close_date'] = pd.to_datetime(car[car_eff_col], errors='coerce')
    car['days2close'] = pd.to_numeric(car['Days to close'], errors='coerce')
    car['status']     = car['Status'].astype(str).str.strip()
    car = car[car['init_date'].notna() & (car['loc'] != 'nan')]
    car_closed = car[car['status'] == 'CLOSED'].copy()
    car_open   = car[car['status'] == 'OPEN'].copy()
    car_closed['close_month'] = car_closed['close_date'].dt.to_period('M')

    # ── Prep PTOs ─────────────────────────────────────────────────
    pto = pto_raw.copy()
    pto['loc']        = pto['Location \n(drop-down)'].astype(str).str.strip()
    pto['init_date']  = pd.to_datetime(pto['PTO initialized date'], errors='coerce')
    pto['close_date'] = pd.to_datetime(pto[pto_eff_col], errors='coerce')
    pto['is_closed']  = pto['close_date'].notna()
    pto['days2close'] = (pto['close_date'] - pto['init_date']).dt.days
    pto = pto[pto['init_date'].notna() & (pto['loc'] != 'nan')]
    pto_closed = pto[pto['is_closed']].copy()
    pto_open   = pto[~pto['is_closed']].copy()
    pto_closed['close_month'] = pto_closed['close_date'].dt.to_period('M')

    # ── Filter helper ─────────────────────────────────────────────
    def filter_df(df, loc_key):
        if loc_key == 'ALL':
            return df
        if loc_key.startswith('REGION:'):
            return df[df['loc'].isin(region_map.get(loc_key[7:], []))]
        return df[df['loc'] == loc_key]

    # ── Core metric calc ──────────────────────────────────────────
    def calc(closed_df, open_df, loc_key):
        c = filter_df(closed_df, loc_key)
        o = filter_df(open_df,   loc_key)
        rows = []
        for m in months:
            mc  = c[c['close_month'] == m]
            cnt = len(mc)
            avg = int(round(mc['days2close'].mean())) if cnt > 0 else 0
            me  = m.to_timestamp('M')
            oe_c = c[(c['init_date'] <= me) & (c['close_date'] > me)]
            oe_o = o[o['init_date'] <= me]
            all_open = pd.concat([oe_c[['init_date']], oe_o[['init_date']]])
            all_open['days'] = (me - all_open['init_date']).dt.days
            ov90 = int((all_open['days'] >= 90).sum())
            rows.append({'closed': cnt, 'avg_days': avg, 'ov90': ov90, 'total_days': cnt * avg})
        return rows

    def calc_combined(loc_key):
        cd = car_metrics[loc_key]; pd_ = pto_metrics[loc_key]
        rows = []
        for i in range(NM):
            c, p  = cd[i], pd_[i]
            total = c['closed'] + p['closed']
            avg   = int(round((c['total_days'] + p['total_days']) / total)) if total > 0 else 0
            rows.append({'closed': total, 'avg_days': avg,
                         'ov90': c['ov90'] + p['ov90'],
                         'ov90_car': c['ov90'], 'ov90_pto': p['ov90'],
                         'total_days': c['total_days'] + p['total_days']})
        return rows

    def running_wavg(rows):
        result = []; cum_cls = 0; cum_days = 0; cur_year = months[0].year
        for i, m in enumerate(months):
            if m.year != cur_year:
                cum_cls = 0; cum_days = 0; cur_year = m.year
            cum_cls  += rows[i]['closed']
            cum_days += rows[i]['total_days']
            result.append(int(round(cum_days / cum_cls)) if cum_cls > 0 else 0)
        return result

    # ── Build all keys ────────────────────────────────────────────
    region_keys = [f'REGION:{r}' for r in REGION_ORDER if r in region_map]
    all_keys    = ['ALL'] + region_keys + all_locations

    car_metrics = {k: calc(car_closed, car_open, k) for k in all_keys}
    pto_metrics = {k: calc(pto_closed, pto_open, k) for k in all_keys}
    cmb_metrics = {k: calc_combined(k) for k in all_keys}

    car_wavg = {k: running_wavg(car_metrics[k]) for k in all_keys}
    pto_wavg = {k: running_wavg(pto_metrics[k]) for k in all_keys}
    cmb_wavg = {k: running_wavg(cmb_metrics[k]) for k in all_keys}

    # ── Per-location stats ────────────────────────────────────────
    last_idx = NM - 1

    def build_stats(metrics, wavg):
        stats = {}
        for loc in all_locations:
            d = metrics[loc]
            stats[loc] = {
                'last_ov':       d[last_idx]['ov90'],
                'avg_ov':        int(round(sum(r['ov90'] for r in d) / NM)),
                'total_closed':  sum(r['closed'] for r in d),
                'last_dec_wavg': wavg[loc][last_dec_idx],
                'prev_dec_wavg': wavg[loc][prev_dec_idx] if prev_dec_idx is not None else 0,
            }
        return stats

    car_stats = build_stats(car_metrics, car_wavg)
    pto_stats = build_stats(pto_metrics, pto_wavg)
    cmb_stats = build_stats(cmb_metrics, cmb_wavg)

    def thresholds(stats):
        vals = [stats[l]['last_ov'] for l in all_locations]
        return int(np.percentile(vals, 66)), int(np.percentile(vals, 33))

    car_t_hi, car_t_lo = thresholds(car_stats)
    pto_t_hi, pto_t_lo = thresholds(pto_stats)
    cmb_t_hi, cmb_t_lo = thresholds(cmb_stats)

    def top20(stats):
        ranked = sorted(all_locations, key=lambda l: stats[l]['last_ov'], reverse=True)[:20]
        return [{'loc': l, 'last_ov': stats[l]['last_ov'],
                 'avg_ov': stats[l]['avg_ov'],
                 'total_closed': stats[l]['total_closed']} for l in ranked]

    return {
        'car_metrics':   car_metrics,
        'pto_metrics':   pto_metrics,
        'cmb_metrics':   cmb_metrics,
        'car_wavg':      car_wavg,
        'pto_wavg':      pto_wavg,
        'cmb_wavg':      cmb_wavg,
        'car_stats':     car_stats,
        'pto_stats':     pto_stats,
        'cmb_stats':     cmb_stats,
        'car_t_hi': car_t_hi, 'car_t_lo': car_t_lo,
        'pto_t_hi': pto_t_hi, 'pto_t_lo': pto_t_lo,
        'cmb_t_hi': cmb_t_hi, 'cmb_t_lo': cmb_t_lo,
        'car_top20':     top20(car_stats),
        'pto_top20':     top20(pto_stats),
        'cmb_top20':     top20(cmb_stats),
        'month_labels':  month_labels,
        'last_dec_idx':  last_dec_idx,
        'last_dec_year': last_dec_year,
        'prev_dec_idx':  prev_dec_idx,
        'prev_dec_year': prev_dec_year,
        'year_end_indices': year_end_indices,
        'all_locations': all_locations,
        'region_map':    region_map,
        'region_order':  REGION_ORDER,
        'region_colors': REGION_COLORS,
        'loaded_at':     pd.Timestamp.now().strftime('%m/%d/%Y %I:%M %p'),
        'file_path':     source_name,
    }
