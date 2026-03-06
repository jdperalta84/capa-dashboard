"""
data_engine.py
Auto-detects file format:
  - MASTER format: output of merge.py (Data - CARs / Data - PTOs with normalized columns)
  - PIVOT  format: legacy monthly snapshot file (Data - CARs / Open data - PTOs)
Both formats produce the identical output dict consumed by app.py.
"""

import io
import pandas as pd
import numpy as np
from pathlib import Path


REGION_ORDER = ['USWC', 'USGC', 'USNE', 'USMW & River', 'USMA & Carib', 'Canada',
                'NAM/Chem', 'NAM/LPG', 'Corporate', 'Environmental', 'ADD/Calib', 'Agri']

REGION_COLORS = {
    'USWC':          '#2E86C1',
    'USGC':          '#CA6F1E',
    'USNE':          '#28B463',
    'USMW & River':  '#A569BD',
    'USMA & Carib':  '#16A085',
    'Canada':        '#8E44AD',
    'NAM/Chem':      '#D35400',
    'NAM/LPG':       '#27AE60',
    'Corporate':     '#566573',
    'Environmental': '#1ABC9C',
    'ADD/Calib':     '#C0392B',
    'Agri':          '#F39C12',
}

# Full location → region lookup (updated from List source v3)
# Add new locations here as labs expand internationally
LOCATION_REGION = {
    "A&B Labs - Baton Rouge, LA":          "Environmental",
    "A&B Labs - Houston, TX":              "Environmental",
    "A&B Labs - Nederland, TX":            "Environmental",
    "A&B Labs - Tempe, AZ":               "Environmental",
    "AGRI (HTC)":                          "USGC",
    "Additives East Coast":                "ADD/Calib",
    "Additives Gulf Coast":                "ADD/Calib",
    "Albany, NY":                          "USNE",
    "Avenel (NYH), NJ":                    "USNE",
    "Bahamas (Freeport), GBI":             "USMA & Carib",
    "Baltimore (Glen Burnie), MD":         "USMA & Carib",
    "Baton Rouge (Gonzales), LA":          "USMW & River",
    "Baytown, TX":                         "USGC",
    "Belle Chasse, LA":                    "USMW & River",
    "Bellingham (Ferndale), WA":           "USWC",
    "Bostco, TX":                          "USGC",
    "Boston (Everett), MA":                "USNE",
    "Boucherville, QC":                    "Agri",
    "Brownsville, TX":                     "USGC",
    "CORPORATE, NJ":                       "Corporate",
    "Cameron LNG":                         "Unassigned",
    "Cape Canaveral, FL":                  "USMA & Carib",
    "Chicago, IL":                         "USMW & River",
    "Cincinnati (Erlanger), OH":           "USMW & River",
    "Collins (Purvis), MS":                "USMW & River",
    "Corpus Christi, TX":                  "USGC",
    "Corpus Christi, TX (CITGO Lab)":      "USGC",
    "Cushing, OK":                         "USGC",
    "Decatur, AL":                         "USMW & River",
    "Freeport, TX":                        "USGC",
    "Ft Lauderdale (Davie), FL":           "USMA & Carib",
    "HOFTI / Channelview, TX":             "USGC",
    "HST Weights & Measures":              "ADD/Calib",
    "HTC LPG":                             "NAM/LPG",
    "Halifax (Dartmouth), NS":             "Canada",
    "Hamilton (Burlington), ON":           "Canada",
    "Houston (HTC), TX":                   "USGC",
    "Ingleside, TX":                       "USGC",
    "Kenai, AK":                           "USWC",
    "Kenner, LA":                          "Agri",
    "Lake Charles (Sulfur), LA":           "USGC",
    "Levis (Quebec City), Quebec":         "Canada",
    "Los Angeles (Signal Hill), CA":       "USWC",
    "Marcus Hook, PA":                     "NAM/LPG",
    "Memphis, TN":                         "USMW & River",
    "Mickleton (Philly), NJ":              "USMA & Carib",
    "Midland, TX":                         "USGC",
    "Minot, ND":                           "USGC",
    "Mobile, AL":                          "USMW & River",
    "Mont Belvieu, TX":                    "NAM/LPG",
    "Montreal, QC":                        "Canada",
    "New Haven, CT":                       "USNE",
    "New Orleans (Destrehan), LA":         "USMW & River",
    "Newfoundland (Arnold's Cove)":        "Canada",
    "Pecos (West Texas), TX":              "USGC",
    "Phoenix, AZ":                         "USWC",
    "Pittsburgh, PA":                      "USMW & River",
    "Port Arthur (Beaumont), TX":          "USGC",
    "Port Arthur (Sabine Blending Lab), TX": "USGC",
    "Port Lavaca, TX":                     "USGC",
    "Portland, ME":                        "USNE",
    "Providence, RI":                      "USNE",
    "Puerto Rico":                         "USMA & Carib",
    "San Francisco (Concord), CA":         "USWC",
    "Santurce, Puerto Rico":               "Unassigned",
    "Savannah, GA / Charleston, SC":       "USMA & Carib",
    "Seabrook (Chemicals), TX":            "NAM/Chem",
    "Specialty":                           "Unassigned",
    "St Croix, USVI":                      "USMA & Carib",
    "St James, LA":                        "USMW & River",
    "St John, NB":                         "Canada",
    "St Louis, MO":                        "USMW & River",
    "Tacoma, WA":                          "USWC",
    "Tampa, FL":                           "USMA & Carib",
    "Texas City, TX":                      "USGC",
    "Utah":                                "Unassigned",
    "Valdez, AK":                          "USWC",
    "Warehouse":                           "ADD/Calib",
    "Yorktown (Norfolk), VA":              "USMA & Carib",
}

SKIP_LOCS = ['A&B Labs', 'VOIDED', 'Extras', 'Warehouse', 'Additives',
             'Utah', 'Cameron', 'Specialty', 'Kenner', 'Santurce', 'Boucherville']

# Regions excluded from CARs and PTOs metrics entirely
EXCLUDE_REGIONS = {'Corporate'}


def _read_source(source, sheet_name, header=0):
    if isinstance(source, io.BytesIO):
        source.seek(0)
    return pd.read_excel(source, sheet_name=sheet_name, header=header)


def _detect_format(source):
    """Return 'master' or 'pivot' based on sheet names and column structure."""
    if isinstance(source, io.BytesIO):
        source.seek(0)
    xl = pd.read_excel(source, sheet_name=None, nrows=2)
    sheets = list(xl.keys())
    # Master format has 'Data - CARs' with normalized col 'car_number'
    if 'Data - CARs' in sheets:
        cols = [str(c).strip() for c in xl['Data - CARs'].columns]
        if 'car_number' in cols or 'location' in cols:
            return 'master'
    # Pivot format has 'Open data - PTOs'
    if 'Open data - PTOs' in sheets:
        return 'pivot'
    # Fallback: if Data - CARs has pivot-style columns
    if 'Data - CARs' in sheets:
        cols = [str(c).strip() for c in xl['Data - CARs'].columns]
        if any('Location' in c for c in cols):
            return 'pivot'
    raise ValueError(f"Unrecognised file format. Sheets found: {sheets}")


def _read_list_source(source):
    """Read List source sheet if present, return {location: region} dict or None."""
    try:
        if isinstance(source, io.BytesIO):
            source.seek(0)
        ls = pd.read_excel(source, sheet_name='List source', header=0)
        # Find location and area columns flexibly
        loc_col  = next((c for c in ls.columns if 'Location' in str(c)), None)
        area_col = next((c for c in ls.columns if 'Area' in str(c)), None)
        if not loc_col or not area_col:
            return None
        ls = ls[[loc_col, area_col]].dropna()
        ls[loc_col]  = ls[loc_col].astype(str).str.strip()
        ls[area_col] = ls[area_col].astype(str).str.strip()
        ls = ls[ls[area_col] != 'VOID']
        ls = ls[~ls[loc_col].apply(lambda x: any(s in str(x) for s in SKIP_LOCS))]
        return dict(zip(ls[loc_col], ls[area_col]))
    except Exception:
        return None


def _build_region_map(locations, loc_region_override=None):
    """Build region_map from List source override or LOCATION_REGION fallback.
    Excludes any region in EXCLUDE_REGIONS."""
    lookup = loc_region_override if loc_region_override else LOCATION_REGION
    region_map = {}
    for loc in locations:
        region = lookup.get(loc)
        if region is None:
            # Try partial match for flexibility
            for known_loc, known_region in lookup.items():
                if known_loc.lower() in loc.lower() or loc.lower() in known_loc.lower():
                    region = known_region
                    break
        if region is None:
            region = 'Other'
        if region in EXCLUDE_REGIONS:
            continue  # Skip excluded regions entirely
        region_map.setdefault(region, []).append(loc)
    return region_map


# ══════════════════════════════════════════════════════════════════
# SHARED METRIC ENGINE
# (used by both format pipelines once data is normalised)
# ══════════════════════════════════════════════════════════════════
def _compute_metrics(car_closed, car_open, pto_closed, pto_open,
                     months, all_locations, region_map):

    NM = len(months)
    month_labels = [m.strftime('%b %Y') for m in months]

    # Year-end indices
    year_end_indices = {m.year: i for i, m in enumerate(months) if m.month == 12}
    last_dec_year = max(year_end_indices.keys()) if year_end_indices else None
    last_dec_idx  = year_end_indices.get(last_dec_year, NM - 1)
    prev_dec_year = last_dec_year - 1 if last_dec_year else None
    prev_dec_idx  = year_end_indices.get(prev_dec_year, None)

    def filter_df(df, loc_key):
        if loc_key == 'ALL':
            return df
        if loc_key.startswith('REGION:'):
            return df[df['loc'].isin(region_map.get(loc_key[7:], []))]
        return df[df['loc'] == loc_key]

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

    region_keys = [f'REGION:{r}' for r in REGION_ORDER if r in region_map]
    all_keys    = ['ALL'] + region_keys + all_locations

    car_metrics = {k: calc(car_closed, car_open, k) for k in all_keys}
    pto_metrics = {k: calc(pto_closed, pto_open, k) for k in all_keys}
    cmb_metrics = {k: calc_combined(k) for k in all_keys}

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
        'car_metrics': car_metrics, 'pto_metrics': pto_metrics, 'cmb_metrics': cmb_metrics,
        'car_wavg':    car_wavg,    'pto_wavg':    pto_wavg,    'cmb_wavg':    cmb_wavg,
        'car_stats':   car_stats,   'pto_stats':   pto_stats,   'cmb_stats':   cmb_stats,
        'car_t_hi': car_t_hi, 'car_t_lo': car_t_lo,
        'pto_t_hi': pto_t_hi, 'pto_t_lo': pto_t_lo,
        'cmb_t_hi': cmb_t_hi, 'cmb_t_lo': cmb_t_lo,
        'car_top20': top20(car_stats),
        'pto_top20': top20(pto_stats),
        'cmb_top20': top20(cmb_stats),
        'month_labels':      month_labels,
        'last_dec_idx':      last_dec_idx,
        'last_dec_year':     last_dec_year,
        'prev_dec_idx':      prev_dec_idx,
        'prev_dec_year':     prev_dec_year,
        'year_end_indices':  year_end_indices,
        'all_locations':     all_locations,
        'region_map':        region_map,
        'region_order':      REGION_ORDER,
        'region_colors':     REGION_COLORS,
    }


# ══════════════════════════════════════════════════════════════════
# MASTER FORMAT PIPELINE
# ══════════════════════════════════════════════════════════════════
def _load_master(source):
    car_raw = _read_source(source, 'Data - CARs')
    pto_raw = _read_source(source, 'Data - PTOs')

    def prep(df):
        df = df.copy()
        df['loc']        = df['location'].astype(str).str.strip()
        df['init_date']  = pd.to_datetime(df['init_date'], errors='coerce')
        df['close_date'] = pd.to_datetime(df['close_date'], errors='coerce')
        df['days2close'] = (df['close_date'] - df['init_date']).dt.days
        df['is_closed']  = df['close_date'].notna()
        df = df[df['init_date'].notna() & (df['loc'] != 'nan') & (df['loc'] != '')]
        df = df[~df['loc'].apply(lambda x: any(s in str(x) for s in SKIP_LOCS))]
        return df

    car = prep(car_raw)
    pto = prep(pto_raw)

    car_closed = car[car['is_closed']].copy()
    car_open   = car[~car['is_closed']].copy()
    pto_closed = pto[pto['is_closed']].copy()
    pto_open   = pto[~pto['is_closed']].copy()

    car_closed['close_month'] = car_closed['close_date'].dt.to_period('M')
    pto_closed['close_month'] = pto_closed['close_date'].dt.to_period('M')

    # Date range: init start → max close or today
    all_inits  = pd.concat([car['init_date'], pto['init_date']]).dropna()
    all_closes = pd.concat([car_closed['close_date'], pto_closed['close_date']]).dropna()
    data_start = all_inits.min().to_period('M')
    data_end   = max(all_closes.max(), pd.Timestamp.now()).to_period('M')
    months     = pd.period_range(data_start, data_end, freq='M')

    # Read List source for region assignments if present
    loc_region_override = _read_list_source(source)

    # Locations + region map
    all_locs_raw  = sorted(set(car['loc'].unique()) | set(pto['loc'].unique()))
    all_locations = [l for l in all_locs_raw
                     if not any(s in l for s in SKIP_LOCS) and l not in ('nan','')]
    region_map    = _build_region_map(all_locations, loc_region_override)

    # Exclude locations that belong to excluded regions
    excluded_locs = set(all_locs_raw) - set(l for locs in region_map.values() for l in locs)
    if excluded_locs:
        car_closed = car_closed[~car_closed['loc'].isin(excluded_locs)]
        car_open   = car_open[~car_open['loc'].isin(excluded_locs)]
        pto_closed = pto_closed[~pto_closed['loc'].isin(excluded_locs)]
        pto_open   = pto_open[~pto_open['loc'].isin(excluded_locs)]
    all_locations = [l for l in all_locations if l not in excluded_locs]

    return car_closed, car_open, pto_closed, pto_open, months, all_locations, region_map


# ══════════════════════════════════════════════════════════════════
# PIVOT FORMAT PIPELINE  (legacy)
# ══════════════════════════════════════════════════════════════════
def _load_pivot(source):
    # List source for region map
    ls = _read_source(source, 'List source', header=0)
    ls = ls[ls['Location'].notna() & ls['Area'].notna()].copy()
    ls['Location'] = ls['Location'].str.strip()
    ls['Area']     = ls['Area'].str.strip()
    ls = ls[~ls['Location'].apply(lambda x: any(s in str(x) for s in SKIP_LOCS))]
    ls = ls[ls['Area'] != 'VOID']
    ls = ls[~ls['Area'].isin(EXCLUDE_REGIONS)]  # exclude Corporate etc.
    region_map    = {}
    for _, row in ls.iterrows():
        region_map.setdefault(row['Area'], []).append(row['Location'])
    all_locations = sorted(ls['Location'].unique().tolist())

    # CARs
    car_raw = _read_source(source, 'Data - CARs', header=0)
    car_raw.columns = car_raw.columns.str.strip()
    if 'Location \n(drop-down)' not in car_raw.columns:
        car_raw = _read_source(source, 'Data - CARs', header=1)
        car_raw.columns = car_raw.columns.str.strip()

    # PTOs
    pto_raw = _read_source(source, 'Open data - PTOs', header=0)
    pto_raw.columns = pto_raw.columns.str.strip()
    if 'Location \n(drop-down)' not in pto_raw.columns:
        pto_raw = _read_source(source, 'Open data - PTOs', header=1)
        pto_raw.columns = pto_raw.columns.str.strip()

    # CAR effectiveness col
    car_eff_col = next((c for c in car_raw.columns
                        if 'Effectiveness' in str(c) or 'deemed' in str(c).lower()), None)
    if not car_eff_col:
        raise ValueError(f"Cannot find effectiveness column in CARs. Columns: {car_raw.columns.tolist()}")

    pto_eff_col = next((c for c in pto_raw.columns
                        if 'Effectiveness' in str(c) or 'deemed' in str(c).lower()), None)
    if not pto_eff_col:
        raise ValueError(f"Cannot find effectiveness column in PTOs. Columns: {pto_raw.columns.tolist()}")

    def prep_pivot(raw, loc_col, init_col, eff_col):
        df = raw.copy()
        df['loc']        = df[loc_col].astype(str).str.strip()
        df['init_date']  = pd.to_datetime(df[init_col], errors='coerce')
        df['close_date'] = pd.to_datetime(df[eff_col],  errors='coerce')
        df['days2close'] = (df['close_date'] - df['init_date']).dt.days
        df['is_closed']  = df['close_date'].notna()
        df = df[df['init_date'].notna() & (df['loc'] != 'nan')]
        return df

    car = prep_pivot(car_raw, 'Location \n(drop-down)', 'CAR initialized date', car_eff_col)
    pto = prep_pivot(pto_raw, 'Location \n(drop-down)', 'PTO initialized date', pto_eff_col)

    car_closed = car[car['is_closed']].copy()
    car_open   = car[~car['is_closed']].copy()
    pto_closed = pto[pto['is_closed']].copy()
    pto_open   = pto[~pto['is_closed']].copy()

    car_closed['close_month'] = car_closed['close_date'].dt.to_period('M')
    pto_closed['close_month'] = pto_closed['close_date'].dt.to_period('M')

    all_inits  = pd.concat([car['init_date'], pto['init_date']]).dropna()
    all_closes = pd.concat([car_closed['close_date'], pto_closed['close_date']]).dropna()
    data_start = all_inits.min().to_period('M')
    data_end   = all_closes.max().to_period('M')
    months     = pd.period_range(data_start, data_end, freq='M')

    # Exclude Corporate and other excluded-region locations from data
    excluded_locs = set()
    for region in EXCLUDE_REGIONS:
        for loc in ls[ls['Area'] == region]['Location'].tolist() if 'Area' in ls.columns else []:
            excluded_locs.add(loc)
    if excluded_locs:
        car_closed = car_closed[~car_closed['loc'].isin(excluded_locs)]
        car_open   = car_open[~car_open['loc'].isin(excluded_locs)]
        pto_closed = pto_closed[~pto_closed['loc'].isin(excluded_locs)]
        pto_open   = pto_open[~pto_open['loc'].isin(excluded_locs)]

    return car_closed, car_open, pto_closed, pto_open, months, all_locations, region_map


# ══════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════
def load_and_compute(file_source) -> dict:
    if isinstance(file_source, (str, Path)):
        source      = str(file_source)
        source_name = Path(file_source).name
    else:
        file_source.seek(0)
        source      = io.BytesIO(file_source.read())
        source_name = "uploaded file"

    fmt = _detect_format(source)

    if fmt == 'master':
        car_closed, car_open, pto_closed, pto_open, months, all_locations, region_map = \
            _load_master(source)
    else:
        car_closed, car_open, pto_closed, pto_open, months, all_locations, region_map = \
            _load_pivot(source)

    result = _compute_metrics(car_closed, car_open, pto_closed, pto_open,
                              months, all_locations, region_map)
    result['loaded_at']  = pd.Timestamp.now().strftime('%m/%d/%Y %I:%M %p')
    result['file_path']  = source_name
    result['file_format'] = fmt
    return result
