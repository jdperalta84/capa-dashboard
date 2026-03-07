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

# Regions excluded entirely from all metrics
EXCLUDE_REGIONS = {'Corporate', 'Agri', 'Environmental', 'Unassigned', 'VOID'}

# Fallback hardcoded location→region (only used if List source sheet is missing)
LOCATION_REGION = {
    "AGRI (HTC)":                            "USGC",
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
        sub = sub[sub[area_col].ne('VOID')]
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
        region = loc_region_lookup.get(loc)
        if region is None:
            # Partial match fallback
            for k, v in loc_region_lookup.items():
                if k.lower() in loc.lower() or loc.lower() in k.lower():
                    region = v
                    break
        if region is None or region in EXCLUDE_REGIONS:
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
def _compute_metrics(car_closed, pto_closed, months, all_locations, region_map):
    """
    All metrics derived from CLOSED records only.
      - avg_days / total_closed: records whose close_date falls in that month
      - ov90: records closed in that month whose days2close >= 90
              (i.e. took 90+ days from init to close)
    Running weighted avg resets each January.
    """
    NM           = len(months)
    month_labels = [m.strftime('%b %Y') for m in months]

    year_end_indices = {m.year: i for i, m in enumerate(months) if m.month == 12}
    last_dec_year    = max(year_end_indices.keys()) if year_end_indices else None
    last_dec_idx     = year_end_indices.get(last_dec_year, NM - 1)
    prev_dec_year    = last_dec_year - 1 if last_dec_year else None
    prev_dec_idx     = year_end_indices.get(prev_dec_year, None)

    def filter_df(df, loc_key):
        if loc_key == 'ALL':
            return df
        if loc_key.startswith('REGION:'):
            return df[df['loc'].isin(region_map.get(loc_key[7:], []))]
        return df[df['loc'] == loc_key]

    def calc(closed_df, loc_key):
        c    = filter_df(closed_df, loc_key)
        rows = []
        for m in months:
            mc   = c[c['close_month'] == m]
            cnt  = len(mc)
            avg  = int(round(mc['days2close'].mean())) if cnt > 0 else 0
            # ov90: closed this month AND took >= 90 days
            ov90 = int((mc['days2close'] >= 90).sum()) if cnt > 0 else 0
            rows.append({'closed': cnt, 'avg_days': avg, 'ov90': ov90,
                         'total_days': cnt * avg})
        return rows

    def calc_combined(loc_key):
        cd, pd_ = car_metrics[loc_key], pto_metrics[loc_key]
        rows = []
        for i in range(NM):
            c, p  = cd[i], pd_[i]
            total = c['closed'] + p['closed']
            avg   = int(round((c['total_days'] + p['total_days']) / total)) if total > 0 else 0
            rows.append({'closed': total, 'avg_days': avg,
                         'ov90':     c['ov90'] + p['ov90'],
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

    # Build region keys from actual region_map (not hardcoded REGION_ORDER)
    region_keys = [f'REGION:{r}' for r in region_map.keys()]
    all_keys    = ['ALL'] + region_keys + all_locations

    car_metrics = {k: calc(car_closed, k) for k in all_keys}
    pto_metrics = {k: calc(pto_closed, k) for k in all_keys}
    cmb_metrics = {k: calc_combined(k)    for k in all_keys}

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

    # Dynamic region_order: only regions present in data, in preferred order
    preferred = ['USWC', 'USGC', 'USNE', 'USMW & River', 'USMA & Carib',
                 'Canada', 'NAM/Chem', 'NAM/LPG', 'ADD/Calib',
                 'USNE, USMW & Canada', 'SE & Caribbean', 'Calibration']
    active_regions  = list(region_map.keys())
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
def _apply_exclusions(car_df, pto_df, loc_region, exclude_jn=True):
    """
    Apply all exclusion rules to normalized car and pto DataFrames.
    Both dfs must have: loc, init_date, close_date, days2close,
                        description (for VOID), initials (for JN PTOs)
    Returns (car_df, pto_df, all_locations, region_map)
    all_locations includes EVERY valid location from List source (even zero-activity).
    """
    # 1. Remove VOID records
    car_df = car_df[~_is_void(car_df['description'])]
    pto_df = pto_df[~_is_void(pto_df['description'])]

    # 2. Remove JN PTOs if toggled
    if exclude_jn:
        pto_df = pto_df[~_is_jn(pto_df['initials'])]

    # 3. Build region map from ALL locations in List source (not just data)
    #    This ensures zero-activity labs still appear in dropdowns and exports
    all_locs_in_source = sorted(loc_region.keys())
    all_locs_in_source = [l for l in all_locs_in_source if l not in ('nan', '', 'None')]
    region_map = _build_region_map(all_locs_in_source, loc_region)

    # 4. Keep only locations that have a valid (non-excluded) region in the region_map
    valid_locs = set(l for locs in region_map.values() for l in locs)
    car_df = car_df[car_df['loc'].isin(valid_locs)]
    pto_df = pto_df[pto_df['loc'].isin(valid_locs)]

    all_locations = sorted(valid_locs)
    return car_df, pto_df, all_locations, region_map


# ══════════════════════════════════════════════════════════════════
# MASTER FORMAT PIPELINE
# ══════════════════════════════════════════════════════════════════
def _load_master(source, exclude_jn=True):
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
def _load_pivot(source, exclude_jn=True):
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
    init_col     = _find_col(car_raw.columns, 'initialized', 'date') \
                   or _find_col(car_raw.columns, 'car', 'date', exclude=['close','eff','deemed'])
    # Close date priority: Effectiveness Review (closed date) > Corrective Action Approved Date
    close_col    = _find_col(car_raw.columns, 'effectiveness')                    or _find_col(car_raw.columns, 'deemed')                    or _find_col(car_raw.columns, 'approved', 'date')
    desc_col     = _find_col(car_raw.columns, 'description') \
                   or _find_col(car_raw.columns, 'brief')
    initials_col = _find_col(car_raw.columns, 'initials', exclude=['date', 'initialized'])

    if not all([loc_col, init_col, close_col]):
        raise ValueError(f"Cannot find required CAR columns. Found: {car_raw.columns.tolist()}")

    # For CARs: close date column priority order
    # 1. Effectiveness Review (closed date) — 2025 style
    # 2. Corrective Action Approved Date — 2026 style (when Effectiveness is empty)
    # NOTE: 'Complete corrective actions (Approved Date)' is an INTERMEDIATE step, not close date
    #       We explicitly exclude 'complete' to avoid picking it up
    _eff_col      = _find_col(car_raw.columns, 'effectiveness')
    _approved_col = _find_col(car_raw.columns, 'corrective action approved', exclude=['complete'])                     or _find_col(car_raw.columns, 'approved', 'date', exclude=['complete', 'action plan'])
    car_close_candidates = [c for c in [_eff_col, _approved_col] if c is not None]
    # Deduplicate preserving order
    seen = set()
    car_close_candidates = [c for c in car_close_candidates
                             if not (c in seen or seen.add(c))]

    def prep_car(raw):
        df = raw.copy()
        df['loc']        = df[loc_col].astype(str).str.strip()
        df['init_date']  = pd.to_datetime(df[init_col], errors='coerce')
        df['record_num'] = df[car_num_col].astype(str).str.strip() if car_num_col else ''
        # Use first close date column that has a value for each row
        close_series = pd.Series(pd.NaT, index=df.index)
        for col in car_close_candidates:
            parsed = pd.to_datetime(df[col], errors='coerce')
            close_series = close_series.where(close_series.notna(), parsed)
        df['close_date']  = close_series
        df['days2close']  = (df['close_date'] - df['init_date']).dt.days
        df['description'] = df[desc_col].fillna('') if desc_col else ''
        df['initials']    = df[initials_col].fillna('') if initials_col else ''
        df = df[df['init_date'].notna() & df['close_date'].notna()]
        # Exclude obviously wrong days2close (formula errors: e.g. TODAY()-init for open records)
        df = df[(df['days2close'] >= 0) & (df['days2close'] < 3650)]
        df = df[df['loc'].notna() & (df['loc'] != 'nan') & (df['loc'] != '')]
        return df

    # ── PTOs ──────────────────────────────────────────────────────
    pto_raw = _read_source(source, pto_sheet, header=0)
    pto_raw.columns = pto_raw.columns.str.strip()

    pto_loc_col      = _find_col(pto_raw.columns, 'location', 'drop') \
                       or _find_col(pto_raw.columns, 'location')
    pto_num_col      = _find_col(pto_raw.columns, 'pto', '#') \
                       or _find_col(pto_raw.columns, 'pto #')
    pto_init_col     = _find_col(pto_raw.columns, 'initialized', 'date') \
                       or _find_col(pto_raw.columns, 'pto', 'date', exclude=['close','eff','deemed'])
    pto_close_col    = _find_col(pto_raw.columns, 'effectiveness') \
                       or _find_col(pto_raw.columns, 'deemed')
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
        df = df[df['init_date'].notna() & df['close_date'].notna()]
        df = df[(df['days2close'] >= 0) & (df['days2close'] < 3650)]
        df = df[df['loc'].notna() & (df['loc'] != 'nan') & (df['loc'] != '')]
        return df

    car = prep_car(car_raw)
    pto = prep_pto(pto_raw)

    car, pto, all_locations, region_map = _apply_exclusions(car, pto, loc_region, exclude_jn)

    car['close_month'] = car['close_date'].dt.to_period('M')
    pto['close_month'] = pto['close_date'].dt.to_period('M')

    all_closes = pd.concat([car['close_date'], pto['close_date']]).dropna()
    all_inits  = pd.concat([
        pd.to_datetime(car_raw[init_col],     errors='coerce'),
        pd.to_datetime(pto_raw[pto_init_col], errors='coerce')
    ]).dropna()
    data_start = all_inits.min().to_period('M')
    _max_close = all_closes.max()
    data_end   = (pd.Timestamp.now() if pd.isna(_max_close) else max(_max_close, pd.Timestamp.now())).to_period('M')
    months     = pd.period_range(data_start, data_end, freq='M')

    return car, pto, months, all_locations, region_map, loc_id_map


# ══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════
def load_and_compute(file_source, exclude_jn=True) -> dict:
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
    else:
        car, pto, months, all_locations, region_map, loc_id_map = _load_pivot(source, exclude_jn)

    result = _compute_metrics(car, pto, months, all_locations, region_map)
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
def load_and_compute_multi(file_sources, exclude_jn=True) -> dict:
    """Load and combine multiple year files, then compute metrics."""
    if not file_sources:
        raise ValueError("No files provided")

    if len(file_sources) == 1:
        return load_and_compute(file_sources[0], exclude_jn=exclude_jn)

    all_car, all_pto = [], []
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
        else:
            car, pto, _, _, _, _lid = _load_pivot(source, exclude_jn)

        all_car.append(car)
        all_pto.append(pto)

        # Merge loc→region and loc→id from each file's List source
        _lr, _lid = _read_list_source(source)
        if _lr:
            combined_loc_region.update(_lr)
        if _lid:
            combined_loc_id.update(_lid)

    car_combined = pd.concat(all_car, ignore_index=True)
    pto_combined = pd.concat(all_pto, ignore_index=True)

    # Deduplicate on record number (CAR # / PTO #) — globally unique across years
    # Fall back to loc+init+close only if record_num column is missing/empty
    if 'record_num' in car_combined.columns and car_combined['record_num'].ne('').any():
        car_combined = car_combined.drop_duplicates(subset=['record_num'])
    else:
        car_combined = car_combined.drop_duplicates(subset=['loc', 'init_date', 'close_date'])
    if 'record_num' in pto_combined.columns and pto_combined['record_num'].ne('').any():
        pto_combined = pto_combined.drop_duplicates(subset=['record_num'])
    else:
        pto_combined = pto_combined.drop_duplicates(subset=['loc', 'init_date', 'close_date'])

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

    result = _compute_metrics(car_combined, pto_combined, months, all_locations, region_map)
    result['loaded_at']   = pd.Timestamp.now().strftime('%m/%d/%Y %I:%M %p')
    result['file_path']   = ', '.join(source_names)
    result['file_format'] = 'multi'
    result['exclude_jn']  = exclude_jn
    result['loc_id_map']  = combined_loc_id
    return result
