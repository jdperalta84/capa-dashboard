"""
data_engine.py
Auto-detects file format:
  - MASTER format: output of merge.py (Data - CARs / Data - PTOs, normalized columns)
  - PIVOT  format: legacy monthly snapshot file (Data - CARs / Open data - PTOs)
Both formats produce the identical output dict consumed by app.py.

EXCLUSION RULES (applied in both pipelines):
  - Only CLOSED dates drive all metrics (avg days, closed count, open >=90 snapshot)
  - VOID records excluded (description contains 'VOID')
  - Corporate, Agri, Environmental, Unassigned regions excluded entirely
  - PARs and CAFs excluded (CARs and PTOs only)
  - JN PTOs: toggled via exclude_jn parameter (default True), NOT hardcoded
  - Region map always read from List source sheet of uploaded file
"""

import io
import pandas as pd
import numpy as np
from pathlib import Path


# ── Region display order & colors ─────────────────────────────────
REGION_ORDER = ['USWC', 'USGC', 'USNE', 'USMW & River', 'USMA & Carib',
                'Canada', 'NAM/Chem', 'NAM/LPG', 'ADD/Calib',
                # Legacy region names from older pivot files
                'USNE, USMW & Canada', 'SE & Caribbean', 'Calibration']

REGION_COLORS = {
    'USWC':                '#2E86C1',
    'USGC':                '#CA6F1E',
    'USNE':                '#28B463',
    'USMW & River':        '#A569BD',
    'USMA & Carib':        '#16A085',
    'Canada':              '#8E44AD',
    'NAM/Chem':            '#D35400',
    'NAM/LPG':             '#27AE60',
    'ADD/Calib':           '#C0392B',
    # Legacy
    'USNE, USMW & Canada': '#28B463',
    'SE & Caribbean':      '#16A085',
    'Calibration':         '#C0392B',
}

# Regions excluded entirely from all metrics (checked case-insensitively)
EXCLUDE_REGIONS = {'Corporate', 'Agri', 'AGRI', 'Environmental', 'Unassigned', 'VOID'}

# Location name prefixes excluded regardless of their assigned region
EXCLUDE_LOC_PREFIXES = ('agri', 'corporate', 'environmental', 'unassigned', 'void')

# Fallback hardcoded location→region (only used if List source sheet is missing)
LOCATION_REGION = {
    "Albany, NY":                            "USNE",
    "Avenel (NYH), NJ":                      "USNE",
    "Bahamas (Freeport), GBI":               "USMA & Carib",
    "Baltimore (Glen Burnie), MD":           "USMA & Carib",
    "Baton Rouge (Gonzales), LA":            "USMW & River",
    "Baytown, TX":                           "USGC",
    "Belle Chasse, LA":                      "USMW & River",
    "Bellingham (Ferndale), WA":             "USWC",
    "Bostco, TX":                            "USGC",
    "Boston (Everett), MA":                  "USNE",
    "Brownsville, TX":                       "USGC",
    "Cape Canaveral, FL":                    "USMA & Carib",
    "Chicago, IL":                           "USMW & River",
    "Cincinnati (Erlanger), OH":             "USMW & River",
    "Collins (Purvis), MS":                  "USMW & River",
    "Corpus Christi, TX":                    "USGC",
    "Corpus Christi, TX (CITGO Lab)":        "USGC",
    "Cushing, OK":                           "USGC",
    "Decatur, AL":                           "USMW & River",
    "Freeport, TX":                          "USGC",
    "Ft Lauderdale (Davie), FL":             "USMA & Carib",
    "HOFTI / Channelview, TX":               "USGC",
    "HST Weights & Measures":               "ADD/Calib",
    "HTC LPG":                               "NAM/LPG",
    "Halifax (Dartmouth), NS":               "Canada",
    "Hamilton (Burlington), ON":             "Canada",
    "Houston (HTC), TX":                     "USGC",
    "Ingleside, TX":                         "USGC",
    "Kenai, AK":                             "USWC",
    "Lake Charles (Sulfur), LA":             "USGC",
    "Levis (Quebec City), Quebec":           "Canada",
    "Los Angeles (Signal Hill), CA":         "USWC",
    "Marcus Hook, PA":                       "NAM/LPG",
    "Memphis, TN":                           "USMW & River",
    "Mickleton (Philly), NJ":               "USMA & Carib",
    "Midland, TX":                           "USGC",
    "Minot, ND":                             "USGC",
    "Mobile, AL":                            "USMW & River",
    "Mont Belvieu, TX":                      "NAM/LPG",
    "Montreal, QC":                          "Canada",
    "New Haven, CT":                         "USNE",
    "New Orleans (Destrehan), LA":           "USMW & River",
    "Newfoundland (Arnold's Cove)":          "Canada",
    "Pecos (West Texas), TX":               "USGC",
    "Phoenix, AZ":                           "USWC",
    "Pittsburgh, PA":                        "USMW & River",
    "Port Arthur (Beaumont), TX":            "USGC",
    "Port Arthur (Sabine Blending Lab), TX": "USGC",
    "Port Lavaca, TX":                       "USGC",
    "Portland, ME":                          "USNE",
    "Providence, RI":                        "USNE",
    "Puerto Rico":                           "USMA & Carib",
    "San Francisco (Concord), CA":           "USWC",
    "Savannah, GA / Charleston, SC":         "USMA & Carib",
    "Seabrook (Chemicals), TX":              "NAM/Chem",
    "St Croix, USVI":                        "USMA & Carib",
    "St James, LA":                          "USMW & River",
    "St John, NB":                           "Canada",
    "St Louis, MO":                          "USMW & River",
    "Tacoma, WA":                            "USWC",
    "Tampa, FL":                             "USMA & Carib",
    "Texas City, TX":                        "USGC",
    "Valdez, AK":                            "USWC",
    "Yorktown (Norfolk), VA":                "USMA & Carib",
}


# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def _read_source(source, sheet_name, header=0):
    if isinstance(source, io.BytesIO):
        source.seek(0)
    return pd.read_excel(source, sheet_name=sheet_name, header=header)


def _detect_format(source):
    if isinstance(source, io.BytesIO):
        source.seek(0)
    xl     = pd.read_excel(source, sheet_name=None, nrows=2)
    sheets = list(xl.keys())

    # Master format: normalized merge output
    if 'Data - CARs' in sheets:
        cols = [str(c).strip().lower() for c in xl['Data - CARs'].columns]
        if 'car_number' in cols or 'location' in cols:
            return 'master'

    # Pivot format: legacy snapshot with separate open PTO sheet
    if 'Open data - PTOs' in sheets:
        return 'pivot'

    # Pivot format: year-file with sheets like "CAR '26", "PTO '26", "Assessment '26"
    has_car = any('CAR' in s.upper() for s in sheets)
    has_pto = any('PTO' in s.upper() for s in sheets)
    if has_car and has_pto:
        return 'pivot'

    if 'Data - CARs' in sheets:
        return 'pivot'

    raise ValueError(f"Unrecognised file format. Sheets: {sheets}")


def _read_list_source(source):
    """Read List source sheet.
    Returns (loc_region_dict, loc_id_dict) or (None, None) if absent/malformed.
    loc_region_dict: {location_name: region}
    loc_id_dict:     {location_name: location_id_str}
    """
    try:
        if isinstance(source, io.BytesIO):
            source.seek(0)
        ls = pd.read_excel(source, sheet_name='List source', header=0)
        loc_col  = next((c for c in ls.columns if str(c).strip() == 'Location'), None)                    or next((c for c in ls.columns
                            if 'Location' in str(c) and 'Area' not in str(c) and 'ID' not in str(c)), None)
        area_col = next((c for c in ls.columns if str(c).strip() == 'Area'), None)                    or next((c for c in ls.columns
                            if str(c).strip().endswith('Area')
                            and 'CAR' not in str(c) and 'PAR' not in str(c)), None)
        id_col   = next((c for c in ls.columns if str(c).strip() == 'Location ID'), None)                    or next((c for c in ls.columns if 'Location' in str(c) and 'ID' in str(c)), None)
        if not loc_col or not area_col:
            return None, None
        sub = ls[[loc_col, area_col] + ([id_col] if id_col else [])].copy()
        sub[loc_col]  = sub[loc_col].astype(str).str.strip()
        sub[area_col] = sub[area_col].astype(str).str.strip()
        sub = sub[sub[loc_col].ne('') & sub[loc_col].ne('nan')]
        sub = sub[~sub[area_col].str.upper().isin({r.upper() for r in EXCLUDE_REGIONS})]
        sub = sub[~sub[loc_col].str.lower().str.startswith(EXCLUDE_LOC_PREFIXES)]
        loc_region = dict(zip(sub[loc_col], sub[area_col]))
        loc_id = {}
        if id_col:
            sub[id_col] = sub[id_col].fillna('').astype(str).str.strip()
            sub[id_col] = sub[id_col].str.replace(r'\.0$', '', regex=True)  # drop .0 from numeric IDs
            loc_id = {row[loc_col]: row[id_col]
                      for _, row in sub.iterrows() if row[id_col] not in ('', 'nan')}
        return loc_region, loc_id
    except Exception:
        return None, None


def _build_region_map(locations, loc_region_lookup):
    """Map locations → regions, skipping EXCLUDE_REGIONS.
    loc_region_lookup: dict from List source (or fallback LOCATION_REGION)."""
    region_map = {}
    for loc in locations:
        if loc.lower().startswith(EXCLUDE_LOC_PREFIXES):
            continue
        region = loc_region_lookup.get(loc)
        if region is None:
            # Partial match fallback
            for k, v in loc_region_lookup.items():
                if k.lower() in loc.lower() or loc.lower() in k.lower():
                    region = v
                    break
        if region is None or region.upper() in {r.upper() for r in EXCLUDE_REGIONS}:
            continue
        region_map.setdefault(region, []).append(loc)
    return region_map


def _is_void(series):
    """Return boolean mask — True where record description contains VOID."""
    return series.astype(str).str.upper().str.contains('VOID', na=False)


def _is_jn(series):
    """Return boolean mask — True where initials are JN (case-insensitive)."""
    return series.astype(str).str.strip().str.upper() == 'JN'


# ══════════════════════════════════════════════════════════════════
# SHARED METRIC ENGINE
# ══════════════════════════════════════════════════════════════════
def _compute_metrics(car_closed, pto_closed, months, all_locations, region_map,
                     car_open=None, pto_open=None):
    """
    closed / avg_days / wavg : records whose close_date falls in that month.
    ov90 : point-in-time snapshot per month-end —
           record counted if: init_date <= (month_end - 90 days)
                          AND (close_date is NaT  OR  close_date > month_end)
           Uses ALL records (closed + open) so historical snapshots are correct.
           car_open / pto_open are currently-open records (no close_date).
           car_closed / pto_closed are closed records (have close_date).
           Combined into car_all / pto_all for ov90 evaluation.
    Running weighted avg resets each January.
    """
    from calendar import monthrange as _mr

    NM           = len(months)
    month_labels = [m.strftime('%b %Y') for m in months]

    _empty_df = pd.DataFrame(columns=['loc', 'init_date', 'close_date'])

    # Build "all records" DataFrames (closed + open) for ov90 snapshot
    def _build_all(closed_df, open_df):
        parts = []
        if closed_df is not None and len(closed_df) > 0:
            parts.append(closed_df[['loc', 'init_date', 'close_date']])
        if open_df is not None and len(open_df) > 0:
            o = open_df[['loc', 'init_date']].copy()
            o['close_date'] = pd.NaT
            parts.append(o)
        return pd.concat(parts, ignore_index=True) if parts else _empty_df

    car_all = _build_all(car_closed, car_open)
    pto_all = _build_all(pto_closed, pto_open)

    year_end_indices = {m.year: i for i, m in enumerate(months) if m.month == 12}
    last_dec_year    = max(year_end_indices.keys()) if year_end_indices else None
    last_dec_idx     = year_end_indices.get(last_dec_year, NM - 1)
    prev_dec_year    = last_dec_year - 1 if last_dec_year else None
    prev_dec_idx     = year_end_indices.get(prev_dec_year, None)

    def filter_df(df, loc_key):
        if df is None or len(df) == 0:
            return df
        if loc_key == 'ALL':
            return df
        if loc_key.startswith('REGION:'):
            return df[df['loc'].isin(region_map.get(loc_key[7:], []))]
        return df[df['loc'] == loc_key]

    def month_last_ts(m):
        last = _mr(m.year, m.month)[1]
        return pd.Timestamp(m.year, m.month, last, 23, 59, 59)

    def ov90_snapshot(all_df, loc_key, m):
        """
        Count records open as of month-end m that had been open > 90 days.
        A record was open at month-end if:
          - init_date <= (month_end - 90 days)
          - close_date is NaT  OR  close_date > month_end
        """
        df = filter_df(all_df, loc_key)
        if df is None or len(df) == 0:
            return 0
        me     = month_last_ts(m)
        cutoff = me - pd.Timedelta(days=91)   # >90 days: day 91+ is overdue
        mask_init  = df['init_date'] <= cutoff
        mask_open  = df['close_date'].isna() | (df['close_date'] > me)
        return int((mask_init & mask_open).sum())

    def calc(closed_df, all_df, loc_key):
        c    = filter_df(closed_df, loc_key)
        rows = []
        for m in months:
            mc          = c[c['close_month'] == m]
            cnt         = len(mc)
            avg         = int(round(mc['days2close'].mean())) if cnt > 0 else 0
            closed_ov90 = int((mc['days2close'] > 90).sum()) if cnt > 0 else 0
            ov90        = ov90_snapshot(all_df, loc_key, m)
            rows.append({'closed': cnt, 'avg_days': avg,
                         'closed_ov90': closed_ov90,   # closed records that took ≥90 days
                         'ov90': ov90,                  # open backlog ≥90 at month-end
                         'total_days': cnt * avg})
        return rows

    def calc_combined(loc_key):
        cd, pd_ = car_metrics[loc_key], pto_metrics[loc_key]
        rows = []
        for i, m in enumerate(months):
            c, p  = cd[i], pd_[i]
            total = c['closed'] + p['closed']
            avg   = int(round((c['total_days'] + p['total_days']) / total)) if total > 0 else 0
            car_ov = ov90_snapshot(car_all, loc_key, m)
            pto_ov = ov90_snapshot(pto_all, loc_key, m)
            rows.append({'closed':      total,
                         'avg_days':    avg,
                         'closed_ov90': c['closed_ov90'] + p['closed_ov90'],
                         'ov90':        car_ov + pto_ov,
                         'ov90_car':    car_ov,
                         'ov90_pto':    pto_ov,
                         'total_days':  c['total_days'] + p['total_days']})
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

    region_keys = [f'REGION:{r}' for r in region_map.keys()]
    all_keys    = ['ALL'] + region_keys + all_locations

    car_metrics = {k: calc(car_closed, car_all, k) for k in all_keys}
    pto_metrics = {k: calc(pto_closed, pto_all, k) for k in all_keys}
    cmb_metrics = {k: calc_combined(k)                 for k in all_keys}

    car_wavg = {k: running_wavg(car_metrics[k]) for k in all_keys}
    pto_wavg = {k: running_wavg(pto_metrics[k]) for k in all_keys}
    cmb_wavg = {k: running_wavg(cmb_metrics[k]) for k in all_keys}

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
        if not vals:
            return 10, 5
        return int(np.percentile(vals, 66)), int(np.percentile(vals, 33))

    car_t_hi, car_t_lo = thresholds(car_stats)
    pto_t_hi, pto_t_lo = thresholds(pto_stats)
    cmb_t_hi, cmb_t_lo = thresholds(cmb_stats)

    def top20(stats):
        ranked = sorted(all_locations, key=lambda l: stats[l]['last_ov'], reverse=True)[:20]
        return [{'loc': l, 'last_ov': stats[l]['last_ov'],
                 'avg_ov': stats[l]['avg_ov'],
                 'total_closed': stats[l]['total_closed']} for l in ranked]

    preferred = ['USWC', 'USGC', 'USNE', 'USMW & River', 'USMA & Carib',
                 'Canada', 'NAM/Chem', 'NAM/LPG', 'ADD/Calib',
                 'USNE, USMW & Canada', 'SE & Caribbean', 'Calibration']
    active_regions   = list(region_map.keys())
    region_order_out = [r for r in preferred if r in active_regions] + \
                       [r for r in active_regions if r not in preferred]

    return {
        'car_metrics': car_metrics, 'pto_metrics': pto_metrics, 'cmb_metrics': cmb_metrics,
        'car_wavg':    car_wavg,    'pto_wavg':    pto_wavg,    'cmb_wavg':    cmb_wavg,
        'car_stats':   car_stats,   'pto_stats':   pto_stats,   'cmb_stats':   cmb_stats,
        'car_t_hi': car_t_hi, 'car_t_lo': car_t_lo,
        'pto_t_hi': pto_t_hi, 'pto_t_lo': pto_t_lo,
        'cmb_t_hi': cmb_t_hi, 'cmb_t_lo': cmb_t_lo,
        'car_top20': top20(car_stats),
        'pto_top20': top20(pto_stats),
        'cmb_top20': top20(cmb_stats),
        'month_labels':     month_labels,
        'last_dec_idx':     last_dec_idx,
        'last_dec_year':    last_dec_year,
        'prev_dec_idx':     prev_dec_idx,
        'prev_dec_year':    prev_dec_year,
        'year_end_indices': year_end_indices,
        'all_locations':    all_locations,
        'region_map':       region_map,
        'region_order':     region_order_out,
        'region_colors':    REGION_COLORS,
    }




# ══════════════════════════════════════════════════════════════════
# SHARED PREP HELPERS
# ══════════════════════════════════════════════════════════════════
def _apply_exclusions(car_df, pto_df, loc_region, exclude_jn=False,
                      car_open_df=None, pto_open_df=None):
    """
    Apply all exclusion rules to normalized car and pto DataFrames.
    Both dfs must have: loc, init_date, close_date, days2close,
                        description (for VOID), initials (for JN PTOs)
    Returns (car_df, pto_df, car_open_df, pto_open_df, all_locations, region_map)
    """
    # 1. VOID already removed in prep_car/prep_pto — kept here as safety net
    car_df = car_df[~_is_void(car_df['description'])]
    pto_df = pto_df[~_is_void(pto_df['description'])]

    # 2. Remove JN PTOs if toggled
    if exclude_jn:
        pto_df = pto_df[~_is_jn(pto_df['initials'])]
        if pto_open_df is not None and len(pto_open_df):
            pto_open_df = pto_open_df[~_is_jn(pto_open_df['initials'])]

    # 3. Build region map
    all_locs_in_source = sorted(loc_region.keys())
    all_locs_in_source = [l for l in all_locs_in_source if l not in ('nan', '', 'None')]
    region_map = _build_region_map(all_locs_in_source, loc_region)

    # 4. Keep only valid locations
    valid_locs = set(l for locs in region_map.values() for l in locs)
    car_df = car_df[car_df['loc'].isin(valid_locs)]
    pto_df = pto_df[pto_df['loc'].isin(valid_locs)]
    if car_open_df is not None:
        car_open_df = car_open_df[car_open_df['loc'].isin(valid_locs)]
    if pto_open_df is not None:
        pto_open_df = pto_open_df[pto_open_df['loc'].isin(valid_locs)]

    all_locations = sorted(valid_locs)
    return car_df, pto_df, car_open_df, pto_open_df, all_locations, region_map


# ══════════════════════════════════════════════════════════════════
# MASTER FORMAT PIPELINE
# ══════════════════════════════════════════════════════════════════
def _load_master(source, exclude_jn=False):
    car_raw = _read_source(source, 'Data - CARs')
    pto_raw = _read_source(source, 'Data - PTOs')

    def prep(df):
        df = df.copy()
        df['loc']         = df['location'].astype(str).str.strip()
        df['init_date']   = pd.to_datetime(df['init_date'],   errors='coerce')
        df['close_date']  = pd.to_datetime(df['close_date'],  errors='coerce')
        df['days2close']  = (df['close_date'] - df['init_date']).dt.days
        df['description'] = df.get('description', pd.Series([''] * len(df))).fillna('')
        df['initials']    = df.get('initials',    pd.Series([''] * len(df))).fillna('')
        df = df[df['init_date'].notna() & df['close_date'].notna()]
        df = df[df['loc'].notna() & (df['loc'] != 'nan') & (df['loc'] != '')]
        return df

    car = prep(car_raw)
    pto = prep(pto_raw)

    _lr, _lid    = _read_list_source(source)
    loc_region   = _lr  or LOCATION_REGION
    loc_id_map   = _lid or {}
    car, pto, all_locations, region_map = _apply_exclusions(car, pto, loc_region, exclude_jn)

    car['close_month'] = car['close_date'].dt.to_period('M')
    pto['close_month'] = pto['close_date'].dt.to_period('M')

    all_closes = pd.concat([car['close_date'], pto['close_date']]).dropna()
    all_inits  = pd.concat([
        pd.to_datetime(car_raw['init_date'], errors='coerce'),
        pd.to_datetime(pto_raw['init_date'], errors='coerce')
    ]).dropna()
    data_start = all_inits.min().to_period('M')
    _max_close = all_closes.max()
    data_end   = (pd.Timestamp.now() if pd.isna(_max_close) else max(_max_close, pd.Timestamp.now())).to_period('M')
    months     = pd.period_range(data_start, data_end, freq='M')

    return car, pto, months, all_locations, region_map, loc_id_map


# ══════════════════════════════════════════════════════════════════
# PIVOT FORMAT PIPELINE
# ══════════════════════════════════════════════════════════════════
def _load_pivot(source, exclude_jn=False):
    # ── List source ───────────────────────────────────────────────
    _lr, _lid  = _read_list_source(source)
    loc_region = _lr  or LOCATION_REGION
    loc_id_map = _lid or {}

    # ── Detect sheet names dynamically ────────────────────────────
    if isinstance(source, io.BytesIO):
        source.seek(0)
    all_sheets = pd.ExcelFile(source).sheet_names

    def find_sheet(keyword, exclude_keywords=None):
        """Find sheet whose name contains keyword (case-insensitive),
        excluding sheets that contain any exclude_keywords."""
        exclude_keywords = exclude_keywords or []
        for s in all_sheets:
            su = s.upper()
            if keyword.upper() in su:
                if not any(e.upper() in su for e in exclude_keywords):
                    return s
        return None

    # CAR sheet: matches 'CAR' but not Assessment/PAR/CAF
    car_sheet = find_sheet('CAR', exclude_keywords=['ASSESSMENT', 'PAR', 'CAF', 'PIVOT'])
    # PTO sheet: matches 'PTO' but not pivot/open report
    pto_sheet = find_sheet('PTO', exclude_keywords=['PIVOT', 'REPORT', 'CAR_PTO', 'LOCATION'])

    if not car_sheet:
        raise ValueError(f"Cannot find CAR sheet. Sheets: {all_sheets}")
    if not pto_sheet:
        raise ValueError(f"Cannot find PTO sheet. Sheets: {all_sheets}")

    # ── CARs ──────────────────────────────────────────────────────
    car_raw = _read_source(source, car_sheet, header=0)
    car_raw.columns = car_raw.columns.str.strip()

    # Flexible column detection
    def _find_col(columns, *terms, exclude=None):
        """Find first column whose name contains ALL terms (case-insensitive).
        exclude: list of terms that must NOT appear in the column name."""
        for c in columns:
            cl = str(c).lower()
            if all(t.lower() in cl for t in terms):
                if exclude and any(e.lower() in cl for e in exclude):
                    continue
                return c
        return None

    car_num_col  = _find_col(car_raw.columns, 'car', '#', exclude=['notes', 'effective', 'new']) \
                   or _find_col(car_raw.columns, 'car #')
    loc_col      = _find_col(car_raw.columns, 'location', 'drop') \
                   or _find_col(car_raw.columns, 'location')
    init_col     = car_raw.columns[9]   # col J (index 9) — consistent across all years
    # Close date column varies by year:
    #   2024: col K (index 10) 'Effectiveness Review & date deemed effective'  ~510 dates
    #   2025: col M (index 12) 'Complete corrective actions (Approved Date)'   ~414 dates
    #   2026: col M (index 12) 'Corrective Action Approved Date'
    # Strategy: find all named candidates, pick whichever has the most valid dates.
    _close_candidates = [c for c in [
        _find_col(car_raw.columns, 'complete corrective', 'approved'),
        _find_col(car_raw.columns, 'corrective action approved'),
        _find_col(car_raw.columns, 'effectiveness review', 'date deemed'),
        _find_col(car_raw.columns, 'effectiveness'),
    ] if c is not None]
    def _date_count(col):
        return pd.to_datetime(car_raw[col], errors='coerce').notna().sum()
    close_col = max(_close_candidates, key=_date_count) if _close_candidates else car_raw.columns[12]
    desc_col     = _find_col(car_raw.columns, 'description') \
                   or _find_col(car_raw.columns, 'brief')
    initials_col = _find_col(car_raw.columns, 'initials', exclude=['date', 'initialized'])

    if not all([loc_col, init_col, close_col]):
        raise ValueError(f"Cannot find required CAR columns. Found: {car_raw.columns.tolist()}")

    def prep_car(raw):
        df = raw.copy()
        df['loc']        = df[loc_col].astype(str).str.strip()
        df['init_date']  = pd.to_datetime(df[init_col],  errors='coerce')
        df['close_date'] = pd.to_datetime(df[close_col], errors='coerce')
        df['record_num'] = df[car_num_col].astype(str).str.strip() if car_num_col else ''
        df['days2close']  = (df['close_date'] - df['init_date']).dt.days
        df['description'] = df[desc_col].fillna('') if desc_col else ''
        df['initials']    = df[initials_col].fillna('') if initials_col else ''
        df = df[df['init_date'].notna()]
        df = df[df['loc'].notna() & (df['loc'] != 'nan') & (df['loc'] != '')]
        df = df[~_is_void(df['description'])]
        closed = df[df['close_date'].notna()].copy()
        closed = closed[(closed['days2close'] >= 0) & (closed['days2close'] < 3650)]
        open_  = df[df['close_date'].isna()].copy()
        return closed, open_

    # ── PTOs ──────────────────────────────────────────────────────
    pto_raw = _read_source(source, pto_sheet, header=0)
    pto_raw.columns = pto_raw.columns.str.strip()

    pto_loc_col      = _find_col(pto_raw.columns, 'location', 'drop') \
                       or _find_col(pto_raw.columns, 'location')
    pto_num_col      = _find_col(pto_raw.columns, 'pto', '#') \
                       or _find_col(pto_raw.columns, 'pto #')
    pto_init_col     = pto_raw.columns[8]   # col I (index 8) — per column mapping
    pto_close_col    = pto_raw.columns[10]  # col K (index 10) — per column mapping
    pto_desc_col     = _find_col(pto_raw.columns, 'description') \
                       or _find_col(pto_raw.columns, 'brief')
    pto_initials_col = _find_col(pto_raw.columns, 'initials', exclude=['date', 'initialized'])

    if not all([pto_loc_col, pto_init_col, pto_close_col]):
        raise ValueError(f"Cannot find required PTO columns. Found: {pto_raw.columns.tolist()}")

    def prep_pto(raw):
        df = raw.copy()
        df['loc']         = df[pto_loc_col].astype(str).str.strip()
        df['record_num']  = df[pto_num_col].astype(str).str.strip() if pto_num_col else ''
        df['init_date']   = pd.to_datetime(df[pto_init_col],   errors='coerce')
        df['close_date']  = pd.to_datetime(df[pto_close_col],  errors='coerce')
        df['days2close']  = (df['close_date'] - df['init_date']).dt.days
        df['description'] = df[pto_desc_col].fillna('')     if pto_desc_col     else ''
        df['initials']    = df[pto_initials_col].fillna('') if pto_initials_col else ''
        df = df[df['init_date'].notna()]
        df = df[df['loc'].notna() & (df['loc'] != 'nan') & (df['loc'] != '')]
        df = df[~_is_void(df['description'])]
        closed = df[df['close_date'].notna()].copy()
        closed = closed[(closed['days2close'] >= 0) & (closed['days2close'] < 3650)]
        open_  = df[df['close_date'].isna()].copy()
        return closed, open_

    car_closed, car_open = prep_car(car_raw)
    pto_closed, pto_open = prep_pto(pto_raw)

    car_closed, pto_closed, car_open, pto_open, all_locations, region_map = \
        _apply_exclusions(car_closed, pto_closed, loc_region, exclude_jn,
                          car_open_df=car_open, pto_open_df=pto_open)

    car_closed['close_month'] = car_closed['close_date'].dt.to_period('M')
    pto_closed['close_month'] = pto_closed['close_date'].dt.to_period('M')

    all_closes = pd.concat([car_closed['close_date'], pto_closed['close_date']]).dropna()
    all_inits  = pd.concat([
        pd.to_datetime(car_raw[init_col],     errors='coerce'),
        pd.to_datetime(pto_raw[pto_init_col], errors='coerce')
    ]).dropna()
    data_start = all_inits.min().to_period('M')
    _max_close = all_closes.max()
    data_end   = (pd.Timestamp.now() if pd.isna(_max_close) else max(_max_close, pd.Timestamp.now())).to_period('M')
    months     = pd.period_range(data_start, data_end, freq='M')

    return car_closed, pto_closed, car_open, pto_open, months, all_locations, region_map, loc_id_map


# ══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════
def load_and_compute(file_source, exclude_jn=False) -> dict:
    if isinstance(file_source, (str, Path)):
        source      = str(file_source)
        source_name = Path(file_source).name
    else:
        file_source.seek(0)
        source      = io.BytesIO(file_source.read())
        source_name = 'uploaded file'

    fmt = _detect_format(source)

    if fmt == 'master':
        car, pto, months, all_locations, region_map, loc_id_map = _load_master(source, exclude_jn)
        car_open, pto_open = None, None
    else:
        car, pto, car_open, pto_open, months, all_locations, region_map, loc_id_map = _load_pivot(source, exclude_jn)

    result = _compute_metrics(car, pto, months, all_locations, region_map,
                              car_open=car_open, pto_open=pto_open)
    result['loc_id_map'] = loc_id_map
    result['loaded_at']   = pd.Timestamp.now().strftime('%m/%d/%Y %I:%M %p')
    result['file_path']   = source_name
    result['file_format'] = fmt
    result['exclude_jn']  = exclude_jn
    return result


# ══════════════════════════════════════════════════════════════════
# MULTI-FILE ENTRY POINT
# Accepts a list of file sources, combines CARs and PTOs across all,
# then runs the shared metric engine once on the combined dataset.
# ══════════════════════════════════════════════════════════════════
def load_and_compute_multi(file_sources, exclude_jn=False) -> dict:
    """Load and combine multiple year files, then compute metrics."""
    if not file_sources:
        raise ValueError("No files provided")

    if len(file_sources) == 1:
        return load_and_compute(file_sources[0], exclude_jn=exclude_jn)

    all_car, all_pto = [], []
    all_car_open, all_pto_open = [], []
    combined_loc_region = {}
    combined_loc_id = {}
    source_names = []

    for fs in file_sources:
        if isinstance(fs, (str, Path)):
            source      = str(fs)
            source_name = Path(fs).name
        else:
            fs.seek(0)
            source      = io.BytesIO(fs.read())
            source_name = 'uploaded file'
        source_names.append(source_name)

        fmt = _detect_format(source)
        if fmt == 'master':
            car, pto, _, _, _, _lid = _load_master(source, exclude_jn)
            car_open, pto_open = pd.DataFrame(columns=['loc','init_date']), pd.DataFrame(columns=['loc','init_date'])
        else:
            car, pto, car_open, pto_open, _, _, _, _lid = _load_pivot(source, exclude_jn)

        all_car.append(car)
        all_pto.append(pto)
        all_car_open.append(car_open)
        all_pto_open.append(pto_open)

        # Merge loc→region and loc→id from each file's List source
        _lr, _lid = _read_list_source(source)
        if _lr:
            combined_loc_region.update(_lr)
        if _lid:
            combined_loc_id.update(_lid)

    car_combined = pd.concat(all_car, ignore_index=True)
    pto_combined = pd.concat(all_pto, ignore_index=True)
    car_open_combined = pd.concat(all_car_open, ignore_index=True)
    pto_open_combined = pd.concat(all_pto_open, ignore_index=True)

    # Deduplicate closed records on record number
    if 'record_num' in car_combined.columns and car_combined['record_num'].ne('').any():
        car_combined = car_combined.drop_duplicates(subset=['record_num'])
    else:
        car_combined = car_combined.drop_duplicates(subset=['loc', 'init_date', 'close_date'])
    if 'record_num' in pto_combined.columns and pto_combined['record_num'].ne('').any():
        pto_combined = pto_combined.drop_duplicates(subset=['record_num'])
    else:
        pto_combined = pto_combined.drop_duplicates(subset=['loc', 'init_date', 'close_date'])

    # Deduplicate open records by record_num — same record in 2025 and 2026 file while still open
    car_open_combined = car_open_combined.drop_duplicates(subset=['record_num'], keep='last') \
        if 'record_num' in car_open_combined.columns and car_open_combined['record_num'].ne('').any() \
        else car_open_combined
    pto_open_combined = pto_open_combined.drop_duplicates(subset=['record_num'], keep='last') \
        if 'record_num' in pto_open_combined.columns and pto_open_combined['record_num'].ne('').any() \
        else pto_open_combined

    # Remove open records that were closed in another file (by record_num)
    if 'record_num' in car_combined.columns and 'record_num' in car_open_combined.columns:
        closed_car_nums = set(car_combined['record_num'])
        car_open_combined = car_open_combined[~car_open_combined['record_num'].isin(closed_car_nums)]
    if 'record_num' in pto_combined.columns and 'record_num' in pto_open_combined.columns:
        closed_pto_nums = set(pto_combined['record_num'])
        pto_open_combined = pto_open_combined[~pto_open_combined['record_num'].isin(closed_pto_nums)]

    # Rebuild region map from combined loc_region (all List source locs, not just data)
    if not combined_loc_region:
        combined_loc_region = LOCATION_REGION
    if not combined_loc_id:
        combined_loc_id = {}

    # Use all locations from List source so zero-activity labs appear in UI
    all_locs_in_source = sorted(k for k in combined_loc_region.keys()
                                if k not in ('nan', '', 'None'))
    region_map = _build_region_map(all_locs_in_source, combined_loc_region)

    valid_locs   = set(l for locs in region_map.values() for l in locs)
    car_combined = car_combined[car_combined['loc'].isin(valid_locs)]
    pto_combined = pto_combined[pto_combined['loc'].isin(valid_locs)]
    car_open_combined = car_open_combined[car_open_combined['loc'].isin(valid_locs)]
    pto_open_combined = pto_open_combined[pto_open_combined['loc'].isin(valid_locs)]
    all_locations = sorted(valid_locs)

    # Date range spanning all files
    all_closes = pd.concat([car_combined['close_date'], pto_combined['close_date']]).dropna()
    all_inits  = pd.concat([car_combined['init_date'],  pto_combined['init_date']]).dropna()
    data_start = all_inits.min().to_period('M')
    _max_close = all_closes.max()
    data_end   = (pd.Timestamp.now() if pd.isna(_max_close) else max(_max_close, pd.Timestamp.now())).to_period('M')
    months     = pd.period_range(data_start, data_end, freq='M')

    car_combined['close_month'] = car_combined['close_date'].dt.to_period('M')
    pto_combined['close_month'] = pto_combined['close_date'].dt.to_period('M')

    result = _compute_metrics(car_combined, pto_combined, months, all_locations, region_map,
                              car_open=car_open_combined, pto_open=pto_open_combined)
    result['loaded_at']   = pd.Timestamp.now().strftime('%m/%d/%Y %I:%M %p')
    result['file_path']   = ', '.join(source_names)
    result['file_format'] = 'multi'
    result['exclude_jn']  = exclude_jn
    result['loc_id_map']  = combined_loc_id
    return result


# ══════════════════════════════════════════════════════════════════
# INTERNATIONAL (INTL) FILE LOADER — OLE2/BIFF8 .xls parser
# ══════════════════════════════════════════════════════════════════
import struct

# Geographic mapping: lab name (from filename) → global region
INTL_GLOBAL_REGION = {
    'Australia': 'APAC', 'Singapore': 'APAC',
    'Amsterdam': 'EMEA', 'Belgium': 'EMEA', 'Italy': 'EMEA',
    'Morocco':   'EMEA', 'Portugal': 'EMEA', 'Rotterdam': 'EMEA',
    'Spain':     'EMEA', 'Sweden':   'EMEA', 'UAE': 'EMEA',
}
GLOBAL_REGION_ORDER  = ['NAM', 'EMEA', 'APAC']
GLOBAL_REGION_COLORS = {'NAM': '#0f1c2e', 'EMEA': '#1a3a5c', 'APAC': '#8C1D18'}

# Closed status values across all lab languages
_CLOSED_STATUSES = {'afgesloten', 'closed', 'cerrado', 'fermé', 'chiuso',
                    'abgeschlossen', 'fechado', 'stängd', 'مغلق', 'ferme'}


def _lab_name_from_source(source) -> str:
    """Extract lab name from filename: export_CAPA_Italy.xls -> Italy"""
    try:
        name = getattr(source, 'name', '') or ''
        stem = Path(name).stem
        parts = stem.split('_')
        return parts[-1] if parts else stem
    except Exception:
        return 'Unknown'


def _parse_ole2_workbook(data: bytes) -> bytes:
    """Extract Workbook/Book stream from OLE2 compound file."""
    sector_size = 1 << struct.unpack_from('<H', data, 0x1e)[0]
    num_fat     = struct.unpack_from('<I', data, 0x2c)[0]
    dir_sector  = struct.unpack_from('<I', data, 0x30)[0]

    def sector_offset(s):
        return 512 + s * sector_size

    def read_chain(start):
        fat_secs = []
        for i in range(min(num_fat, 109)):
            s = struct.unpack_from('<I', data, 0x4c + i * 4)[0]
            if s < 0xFFFFFFFD:
                fat_secs.append(s)
        fat = bytearray()
        for s in fat_secs:
            off = sector_offset(s)
            fat.extend(data[off:off + sector_size])
        result  = bytearray()
        s       = start
        visited = set()
        while s < 0xFFFFFFFD and s not in visited:
            visited.add(s)
            off = sector_offset(s)
            result.extend(data[off:off + sector_size])
            s = struct.unpack_from('<I', fat, s * 4)[0] if s * 4 + 4 <= len(fat) else 0xFFFFFFFE
        return bytes(result)

    dir_data = read_chain(dir_sector)
    for i in range(len(dir_data) // 128):
        entry    = dir_data[i * 128:(i + 1) * 128]
        name_len = struct.unpack_from('<H', entry, 0x40)[0]
        if 0 < name_len <= 64:
            name = entry[:name_len - 2].decode('utf-16-le', errors='ignore')
            if name.lower() in ('workbook', 'book') and entry[0x42] in (1, 2):
                start_sect = struct.unpack_from('<I', entry, 0x74)[0]
                size       = struct.unpack_from('<I', entry, 0x78)[0]
                return read_chain(start_sect)[:size]
    return b''


def _extract_biff8_sst(wb: bytes) -> list:
    """Parse Shared String Table from BIFF8 workbook stream."""
    strings = []
    i = 0
    while i < len(wb) - 4:
        rt = struct.unpack_from('<H', wb, i)[0]
        rl = struct.unpack_from('<H', wb, i + 2)[0]
        pl = wb[i + 4:i + 4 + rl]
        if rt == 0x00FC and len(pl) >= 8:
            n   = struct.unpack_from('<I', pl, 4)[0]
            pos = 8
            for _ in range(n):
                if pos >= len(pl):
                    break
                cc    = struct.unpack_from('<H', pl, pos)[0]
                flags = pl[pos + 2] if pos + 2 < len(pl) else 0
                pos  += 3
                if not (flags & 0x01):
                    s    = pl[pos:pos + cc].decode('latin-1', errors='ignore')
                    pos += cc
                else:
                    s    = pl[pos:pos + cc * 2].decode('utf-16-le', errors='ignore')
                    pos += cc * 2
                strings.append(s)
        i += 4 + rl
    return strings


def _extract_biff8_cells(wb: bytes, sst: list) -> dict:
    """Extract all cell values keyed by (sheet_index, row, col)."""
    cells     = {}
    i         = 0
    cur_sheet = 0
    while i < len(wb) - 4:
        rt = struct.unpack_from('<H', wb, i)[0]
        rl = struct.unpack_from('<H', wb, i + 2)[0]
        pl = wb[i + 4:i + 4 + rl]
        if rt == 0x0809:
            cur_sheet += 1
        elif rt == 0x00FD and len(pl) >= 10:  # LABELSST
            row = struct.unpack_from('<H', pl, 0)[0]
            col = struct.unpack_from('<H', pl, 2)[0]
            idx = struct.unpack_from('<I', pl, 6)[0]
            if idx < len(sst):
                cells[(cur_sheet, row, col)] = sst[idx]
        elif rt == 0x0203 and len(pl) >= 14:  # NUMBER
            row = struct.unpack_from('<H', pl, 0)[0]
            col = struct.unpack_from('<H', pl, 2)[0]
            cells[(cur_sheet, row, col)] = struct.unpack_from('<d', pl, 6)[0]
        i += 4 + rl
    return cells


def _parse_intl_date(val) -> 'pd.Timestamp':
    """Parse DD/MM/YYYY string → Timestamp. Returns NaT on failure."""
    if not val or not isinstance(val, str):
        return pd.NaT
    try:
        return pd.to_datetime(val.strip(), dayfirst=True, errors='coerce')
    except Exception:
        return pd.NaT


def load_intl_file(source):
    """
    Parse one international CAPA .xls file.
    Returns (closed_df, open_df) each with columns:
      loc, global_region, init_date, close_date, days2close
    Deduplicates by CAPA ID: earliest init_date, latest close_date.
    """
    lab_name      = _lab_name_from_source(source)
    global_region = INTL_GLOBAL_REGION.get(lab_name, 'EMEA')

    raw = source.read() if hasattr(source, 'read') else open(source, 'rb').read()
    wb  = _parse_ole2_workbook(raw)
    empty = pd.DataFrame(columns=['loc', 'global_region', 'init_date', 'close_date', 'days2close'])
    if not wb:
        return empty, empty

    sst   = _extract_biff8_sst(wb)
    cells = _extract_biff8_cells(wb, sst)

    # Find data sheet: has 'Number', 'Date of notification', 'Date closed' in row 0
    main_cells = {}
    col_map    = {}
    for sheet in sorted(set(s for s, r, c in cells)):
        sc   = {(r, c): v for (s, r, c), v in cells.items() if s == sheet}
        row0 = {c: sc.get((0, c), '') for c in range(20)}
        vals = list(row0.values())
        if 'Number' in vals and 'Date of notification' in vals and 'Date closed' in vals:
            main_cells = sc
            col_map    = {str(v).strip(): c for c, v in row0.items() if str(v).strip()}
            break

    if not main_cells:
        return empty, empty

    id_col     = col_map.get('Number', 0)
    init_col   = col_map.get('Date of notification', 1)
    close_col  = col_map.get('Date closed', 9)
    status_col = col_map.get('Status', 8)
    max_row    = max((r for r, c in main_cells), default=0)

    raw_rows = []
    for row in range(1, max_row + 1):
        capa_id = str(main_cells.get((row, id_col), '')).strip()
        if not capa_id or capa_id in ('nan', ''):
            continue
        init_dt  = _parse_intl_date(str(main_cells.get((row, init_col), '')))
        close_dt = _parse_intl_date(str(main_cells.get((row, close_col), '')))
        status   = str(main_cells.get((row, status_col), '')).strip()
        raw_rows.append({'capa_id': capa_id, 'init_dt': init_dt,
                         'close_dt': close_dt, 'status': status})

    if not raw_rows:
        return empty, empty

    df = pd.DataFrame(raw_rows)

    # Deduplicate: earliest init, latest close per CAPA ID
    df_dedup = df.groupby('capa_id', sort=False).agg(
        init_dt  = ('init_dt',  'min'),
        close_dt = ('close_dt', 'max'),
        status   = ('status',   'last'),
    ).reset_index()

    closed_mask = df_dedup['status'].str.lower().isin(_CLOSED_STATUSES)

    # Closed records
    df_c = df_dedup[closed_mask & df_dedup['close_dt'].notna() & df_dedup['init_dt'].notna()].copy()
    df_c['loc']           = lab_name
    df_c['global_region'] = global_region
    df_c['init_date']     = df_c['init_dt']
    df_c['close_date']    = df_c['close_dt']
    df_c['days2close']    = (df_c['close_date'] - df_c['init_date']).dt.days.clip(lower=0)
    closed_out = df_c[['loc', 'global_region', 'init_date', 'close_date', 'days2close']].copy()

    # Open records
    open_mask = ~closed_mask | df_dedup['close_dt'].isna()
    df_o = df_dedup[open_mask & df_dedup['init_dt'].notna()].copy()
    df_o['loc']           = lab_name
    df_o['global_region'] = global_region
    df_o['init_date']     = df_o['init_dt']
    df_o['close_date']    = pd.NaT
    df_o['days2close']    = np.nan
    open_out = df_o[['loc', 'global_region', 'init_date', 'close_date', 'days2close']].copy()

    return closed_out, open_out


def load_and_compute_global(nam_sources, intl_sources, exclude_jn=True):
    """
    Compute unified global metrics combining NAM + international files.
    Returns standard D dict plus:
      has_intl, intl_locations, intl_region_map, intl_lab_region, global_scope
    """
    from calendar import monthrange as _mr

    D_nam = load_and_compute_multi(nam_sources, exclude_jn=exclude_jn)

    if not intl_sources:
        D_nam['has_intl']        = False
        D_nam['intl_locations']  = []
        D_nam['intl_region_map'] = {}
        D_nam['intl_lab_region'] = {}
        D_nam['global_scope']    = 'NAM'
        return D_nam

    # Load all international files
    intl_closed_parts = []
    intl_open_parts   = []
    intl_lab_region   = {}

    for src in intl_sources:
        try:
            src.seek(0)
            closed_df, open_df = load_intl_file(src)
            if len(closed_df):
                intl_closed_parts.append(closed_df)
                intl_lab_region[closed_df['loc'].iloc[0]] = closed_df['global_region'].iloc[0]
            if len(open_df):
                intl_open_parts.append(open_df)
                lab = open_df['loc'].iloc[0]
                if lab not in intl_lab_region:
                    intl_lab_region[lab] = open_df['global_region'].iloc[0]
        except Exception:
            pass

    _empty = pd.DataFrame(columns=['loc', 'global_region', 'init_date', 'close_date', 'days2close'])
    intl_closed = pd.concat(intl_closed_parts, ignore_index=True) if intl_closed_parts else _empty.copy()
    intl_open   = pd.concat(intl_open_parts,   ignore_index=True) if intl_open_parts   else _empty.copy()

    # Build intl_region_map
    intl_region_map = {}
    for lab, grgn in intl_lab_region.items():
        intl_region_map.setdefault(grgn, [])
        if lab not in intl_region_map[grgn]:
            intl_region_map[grgn].append(lab)
    for grgn in intl_region_map:
        intl_region_map[grgn] = sorted(intl_region_map[grgn])

    # Unified month timeline
    D_nam_months = D_nam['month_labels']
    all_dates = []
    for df in [intl_closed, intl_open]:
        for col in ['init_date', 'close_date']:
            if col in df.columns:
                all_dates.append(df[col].dropna())

    if all_dates:
        all_ts       = pd.concat(all_dates)
        intl_min     = all_ts.min()
        nam_start    = pd.Period(D_nam_months[0], 'M')
        global_start = min(intl_min.to_period('M'), nam_start)
    else:
        global_start = pd.Period(D_nam_months[0], 'M')

    global_end       = pd.Period(D_nam_months[-1], 'M')
    months_ext       = pd.period_range(global_start, global_end, freq='M')
    month_labels_ext = [m.strftime('%b %Y') for m in months_ext]
    NM               = len(months_ext)
    nam_offset       = NM - len(D_nam_months)

    # Pad NAM metric lists to match extended timeline
    def pad_rows(rows, n):
        if not rows or n <= 0:
            return list(rows)
        zero = {k: 0 for k in rows[0]}
        return [dict(zero) for _ in range(n)] + list(rows)

    def pad_wavg(vals, n):
        return [0] * n + list(vals)

    if nam_offset > 0:
        for key in ('car_metrics', 'pto_metrics', 'cmb_metrics'):
            for loc_key in D_nam[key]:
                D_nam[key][loc_key] = pad_rows(D_nam[key][loc_key], nam_offset)
        for key in ('car_wavg', 'pto_wavg', 'cmb_wavg'):
            for loc_key in D_nam[key]:
                D_nam[key][loc_key] = pad_wavg(D_nam[key][loc_key], nam_offset)
        if D_nam['last_dec_idx'] is not None:
            D_nam['last_dec_idx'] += nam_offset
        if D_nam['prev_dec_idx'] is not None:
            D_nam['prev_dec_idx'] += nam_offset
        D_nam['year_end_indices'] = {yr: idx + nam_offset
                                     for yr, idx in D_nam.get('year_end_indices', {}).items()}

    D_nam['month_labels'] = month_labels_ext

    # Build intl "all records" df (closed + open) for ov90 snapshots
    intl_all_parts = []
    if len(intl_closed):
        intl_all_parts.append(intl_closed[['loc', 'global_region', 'init_date', 'close_date']])
    if len(intl_open):
        o = intl_open[['loc', 'global_region', 'init_date']].copy()
        o['close_date'] = pd.NaT
        intl_all_parts.append(o)
    intl_all = pd.concat(intl_all_parts, ignore_index=True) if intl_all_parts else \
        pd.DataFrame(columns=['loc', 'global_region', 'init_date', 'close_date'])

    def _me(m):
        from calendar import monthrange as _mr2
        last = _mr2(m.year, m.month)[1]
        return pd.Timestamp(m.year, m.month, last, 23, 59, 59)

    def intl_ov90(df_all, lab_keys, m):
        if df_all is None or len(df_all) == 0:
            return 0
        df = df_all[df_all['loc'].isin(lab_keys)] if lab_keys is not None else df_all
        me     = _me(m)
        cutoff = me - pd.Timedelta(days=91)
        mask   = (df['init_date'] <= cutoff) & (df['close_date'].isna() | (df['close_date'] > me))
        return int(mask.sum())

    def intl_calc(closed_df, all_df, lab_keys):
        c    = closed_df[closed_df['loc'].isin(lab_keys)] if lab_keys and len(closed_df) else \
               (closed_df if lab_keys is None else closed_df.iloc[0:0])
        all_ = all_df[all_df['loc'].isin(lab_keys)]      if lab_keys and len(all_df) else \
               (all_df if lab_keys is None else all_df.iloc[0:0])
        c = c.copy()
        if len(c):
            c['close_month'] = c['close_date'].dt.to_period('M')
        rows = []
        for m in months_ext:
            mc  = c[c['close_month'] == m] if len(c) else c
            cnt = len(mc)
            avg = int(round(mc['days2close'].mean())) if cnt > 0 else 0
            ov  = intl_ov90(all_, lab_keys, m)
            rows.append({'closed': cnt, 'avg_days': avg,
                         'ov90': ov,
                         'closed_ov90': int((mc['days2close'] > 90).sum()) if cnt > 0 else 0,
                         'total_days': cnt * avg})
        return rows

    def intl_running_wavg(rows):
        result = []; cum_cls = 0; cum_days = 0; cur_year = months_ext[0].year
        for i, m in enumerate(months_ext):
            if m.year != cur_year:
                cum_cls = 0; cum_days = 0; cur_year = m.year
            cum_cls  += rows[i]['closed']
            cum_days += rows[i]['total_days']
            result.append(int(round(cum_days / cum_cls)) if cum_cls > 0 else 0)
        return result

    intl_labs = sorted(intl_lab_region.keys())

    # Per-lab metrics
    for lab in intl_labs:
        rows = intl_calc(intl_closed, intl_all, [lab])
        D_nam['car_metrics'][lab] = rows
        D_nam['car_wavg'][lab]    = intl_running_wavg(rows)

    # Per global-region metrics
    for grgn, labs in intl_region_map.items():
        rows = intl_calc(intl_closed, intl_all, labs)
        D_nam['car_metrics'][f'GLOBAL:{grgn}'] = rows
        D_nam['car_wavg'][f'GLOBAL:{grgn}']    = intl_running_wavg(rows)

    # All intl combined
    rows_intl_all = intl_calc(intl_closed, intl_all, None)
    D_nam['car_metrics']['INTL:ALL'] = rows_intl_all
    D_nam['car_wavg']['INTL:ALL']    = intl_running_wavg(rows_intl_all)

    # GLOBAL:NAM = NAM combined (CARs + PTOs)
    D_nam['car_metrics']['GLOBAL:NAM'] = D_nam['cmb_metrics']['ALL']
    D_nam['car_wavg']['GLOBAL:NAM']    = D_nam['cmb_wavg']['ALL']

    # GLOBAL:ALL = NAM combined + all intl per month
    nam_cmb = D_nam['cmb_metrics']['ALL']
    global_all_rows = []
    for i in range(NM):
        n  = nam_cmb[i]
        il = rows_intl_all[i]
        tot = n['closed'] + il['closed']
        avg = int(round((n['total_days'] + il['total_days']) / tot)) if tot > 0 else 0
        global_all_rows.append({
            'closed':      tot,
            'avg_days':    avg,
            'ov90':        n['ov90'] + il['ov90'],
            'closed_ov90': n.get('closed_ov90', 0) + il['closed_ov90'],
            'total_days':  n['total_days'] + il['total_days'],
        })
    D_nam['car_metrics']['GLOBAL:ALL'] = global_all_rows
    D_nam['car_wavg']['GLOBAL:ALL']    = intl_running_wavg(global_all_rows)

    D_nam['has_intl']        = True
    D_nam['intl_locations']  = intl_labs
    D_nam['intl_region_map'] = intl_region_map
    D_nam['intl_lab_region'] = intl_lab_region
    D_nam['global_scope']    = 'ALL'

    return D_nam
