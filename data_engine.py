"""
data_engine.py
Loads the CAPA/PTO Excel file and computes all metrics.
- Uses master location list from 'List source' sheet (includes zero-activity locations)
- Computes metrics for ALL, each region aggregate, and each individual location
- Running weighted average days closed (resets each calendar year)
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path


REGION_ORDER = ['USWC', 'USGC', 'USNE, USMW & Canada', 'SE & Caribbean', 'Corporate', 'Calibration']

REGION_COLORS = {
    'USWC':                 '#2E86C1',
    'USGC':                 '#CA6F1E',
    'USNE, USMW & Canada':  '#28B463',
    'SE & Caribbean':       '#A569BD',
    'Corporate':            '#566573',
    'Calibration':          '#C0392B',
}

SKIP_LOCS = ['A&B Labs', 'VOIDED', 'Extras', 'Warehouse', 'Additives',
             'Utah', 'Cameron', 'Specialty', 'Kenner', 'Santurce', 'Boucherville']


def load_and_compute(file_path: str) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # ── Master location list from List source ─────────────────────
    ls = pd.read_excel(file_path, sheet_name='List source')
    ls = ls[ls['Location'].notna() & ls['Area'].notna()].copy()
    ls['Location'] = ls['Location'].str.strip()
    ls['Area']     = ls['Area'].str.strip()
    ls = ls[~ls['Location'].apply(lambda x: any(s in str(x) for s in SKIP_LOCS))]
    ls = ls[ls['Area'] != 'VOID']

    region_map = {}
    for _, row in ls.iterrows():
        region_map.setdefault(row['Area'], []).append(row['Location'])

    all_locations = sorted(ls['Location'].unique().tolist())

    # ── Raw data ──────────────────────────────────────────────────
    car_raw = pd.read_excel(file_path, sheet_name='Data - CARs')
    pto_raw = pd.read_excel(file_path, sheet_name='Open data - PTOs')

    months       = pd.period_range('2025-01', '2026-02', freq='M')
    month_labels = [m.strftime('%b %Y') for m in months]
    NM           = len(months)
    DEC2025_IDX  = month_labels.index('Dec 2025')

    # ── Prep CARs ─────────────────────────────────────────────────
    car = car_raw.copy()
    car['loc']        = car['Location \n(drop-down)'].str.strip()
    car['init_date']  = pd.to_datetime(car['CAR initialized date'])
    car['close_date'] = pd.to_datetime(car['Effectiveness Review & date deemed effective'])
    car['days2close'] = car['Days to close']
    car_closed = car[car['Status'] == 'CLOSED'].copy()
    car_open   = car[car['Status'] == 'OPEN'].copy()
    car_closed['close_month'] = car_closed['close_date'].dt.to_period('M')

    # ── Prep PTOs ─────────────────────────────────────────────────
    pto = pto_raw.copy()
    pto['loc']        = pto['Location \n(drop-down)'].str.strip()
    pto['init_date']  = pd.to_datetime(pto['PTO initialized date'])
    pto['close_date'] = pd.to_datetime(pto['Effectiveness Review & date deemed effective'], errors='coerce')
    pto['is_closed']  = pto['close_date'].notna()
    pto['days2close'] = (pto['close_date'] - pto['init_date']).dt.days
    pto_closed = pto[pto['is_closed']].copy()
    pto_open   = pto[~pto['is_closed']].copy()
    pto_closed['close_month'] = pto_closed['close_date'].dt.to_period('M')

    # ── Filter helper ─────────────────────────────────────────────
    def filter_df(df, loc_key):
        if loc_key == 'ALL':
            return df
        if loc_key.startswith('REGION:'):
            region_locs = region_map.get(loc_key[7:], [])
            return df[df['loc'].isin(region_locs)]
        return df[df['loc'] == loc_key]

    # ── Core metric calculator ────────────────────────────────────
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
            rows.append({'closed': cnt, 'avg_days': avg, 'ov90': ov90,
                         'total_days': cnt * avg})
        return rows

    def calc_combined(loc_key):
        cd = car_metrics[loc_key]
        pd_ = pto_metrics[loc_key]
        rows = []
        for i in range(NM):
            c, p = cd[i], pd_[i]
            total = c['closed'] + p['closed']
            avg   = int(round((c['total_days'] + p['total_days']) / total)) if total > 0 else 0
            rows.append({'closed': total, 'avg_days': avg,
                         'ov90': c['ov90'] + p['ov90'],
                         'ov90_car': c['ov90'], 'ov90_pto': p['ov90'],
                         'total_days': c['total_days'] + p['total_days']})
        return rows

    # ── Running weighted average (resets each Jan) ────────────────
    def running_wavg(rows):
        result = []; cum_cls = 0; cum_days = 0; cur_year = months[0].year
        for i, m in enumerate(months):
            if m.year != cur_year:
                cum_cls = 0; cum_days = 0; cur_year = m.year
            cum_cls  += rows[i]['closed']
            cum_days += rows[i]['total_days']
            result.append(int(round(cum_days / cum_cls)) if cum_cls > 0 else 0)
        return result

    # ── Build all filter keys ─────────────────────────────────────
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
                'last_ov':      d[last_idx]['ov90'],
                'avg_ov':       int(round(np.mean([r['ov90'] for r in d]))),
                'total_closed': sum(r['closed'] for r in d),
                'ye2025_wavg':  wavg[loc][DEC2025_IDX],
                'ytd2026_wavg': wavg[loc][-1],
            }
        return stats

    car_stats = build_stats(car_metrics, car_wavg)
    pto_stats = build_stats(pto_metrics, pto_wavg)
    cmb_stats = build_stats(cmb_metrics, cmb_wavg)

    # ── Thresholds (per-type, based on individual location distribution) ──
    def thresholds(stats):
        vals = [stats[l]['last_ov'] for l in all_locations]
        return int(np.percentile(vals, 66)), int(np.percentile(vals, 33))

    car_t_hi, car_t_lo = thresholds(car_stats)
    pto_t_hi, pto_t_lo = thresholds(pto_stats)
    cmb_t_hi, cmb_t_lo = thresholds(cmb_stats)

    # ── Top 20 per tab ────────────────────────────────────────────
    def top20(stats):
        ranked = sorted(all_locations, key=lambda l: stats[l]['last_ov'], reverse=True)[:20]
        return [{'loc': l, 'last_ov': stats[l]['last_ov'],
                 'avg_ov': stats[l]['avg_ov'],
                 'total_closed': stats[l]['total_closed']} for l in ranked]

    return {
        'car_metrics':  car_metrics,
        'pto_metrics':  pto_metrics,
        'cmb_metrics':  cmb_metrics,
        'car_wavg':     car_wavg,
        'pto_wavg':     pto_wavg,
        'cmb_wavg':     cmb_wavg,
        'car_stats':    car_stats,
        'pto_stats':    pto_stats,
        'cmb_stats':    cmb_stats,
        'car_t_hi': car_t_hi, 'car_t_lo': car_t_lo,
        'pto_t_hi': pto_t_hi, 'pto_t_lo': pto_t_lo,
        'cmb_t_hi': cmb_t_hi, 'cmb_t_lo': cmb_t_lo,
        'car_top20': top20(car_stats),
        'pto_top20': top20(pto_stats),
        'cmb_top20': top20(cmb_stats),
        'month_labels':   month_labels,
        'DEC2025_IDX':    DEC2025_IDX,
        'all_locations':  all_locations,
        'region_map':     region_map,
        'region_order':   REGION_ORDER,
        'region_colors':  REGION_COLORS,
        'loaded_at':      pd.Timestamp.now().strftime('%m/%d/%Y %I:%M %p'),
        'file_path':      str(path),
    }
