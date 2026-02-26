"""export_utils.py — Excel export for the dashboard."""
import io
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


def export_excel(D: dict, data_key: str, label: str) -> bytes:
    wb  = Workbook()
    months = D['month_labels']
    DEC = D['DEC2025_IDX']

    def hfill(h): return PatternFill('solid', start_color=h)
    def mfont(bold=False, color='000000', size=10):
        return Font(bold=bold, color=color, size=size, name='Arial')
    def mkborder():
        s = Side(style='thin', color='BFBFBF')
        return Border(left=s, right=s, top=s, bottom=s)
    CA = Alignment(horizontal='center', vertical='center', wrap_text=True)

    TABS = [
        ('CARs',     'car_metrics', 'car_wavg',  '1F4E79', 'D6E4F0',
         ['Month','CARs Closed','Avg Days','Open ≥90','Wtd Avg Days','Trend']),
        ('PTOs',     'pto_metrics', 'pto_wavg',  '375623', 'D9EAD3',
         ['Month','PTOs Closed','Avg Days','Open ≥90','Wtd Avg Days','Trend']),
        ('Combined', 'cmb_metrics', 'cmb_wavg',  '4A235A', 'E8D5F5',
         ['Month','Total Closed','Avg Days','Total ≥90','CARs ≥90','PTOs ≥90','Wtd Avg Days','Trend']),
    ]

    first = True
    for tab_name, met_key, wav_key, dark, light, headers in TABS:
        ws = wb.active if first else wb.create_sheet(tab_name)
        if first: ws.title = tab_name; first = False

        n = len(headers)
        ws.merge_cells(f'A1:{get_column_letter(n)}1')
        ws['A1'] = f'{tab_name} — {label} — {datetime.now().strftime("%m/%d/%Y")}'
        ws['A1'].font = mfont(bold=True, color='FFFFFF', size=12)
        ws['A1'].fill = hfill(dark); ws['A1'].alignment = CA
        ws.row_dimensions[1].height = 24

        for ci, h in enumerate(headers, 1):
            c = ws.cell(row=2, column=ci, value=h)
            c.font = mfont(bold=True, color='FFFFFF', size=10)
            c.fill = hfill(dark); c.alignment = CA
        ws.row_dimensions[2].height = 20

        metrics   = D[met_key].get(data_key, D[met_key]['ALL'])
        wavg_vals = D[wav_key].get(data_key, D[wav_key]['ALL'])
        alt1 = hfill(light); alt2 = hfill('FFFFFF')

        for i, (m_label, row, wv) in enumerate(zip(months, metrics, wavg_vals)):
            r    = 3 + i
            fill = alt1 if i % 2 == 0 else alt2
            prev_ov = metrics[i-1]['ov90'] if i > 0 else None
            trend   = '—' if prev_ov is None else ('▲' if row['ov90'] > prev_ov else ('▼' if row['ov90'] < prev_ov else '→'))

            vals = [m_label, row['closed'], row['avg_days'], row['ov90']]
            if tab_name == 'Combined':
                vals += [row.get('ov90_car', 0), row.get('ov90_pto', 0)]
            vals += [wv, trend]

            for ci, v in enumerate(vals, 1):
                c = ws.cell(row=r, column=ci, value=v)
                c.fill = fill; c.font = mfont(size=10)
                c.border = mkborder(); c.alignment = CA

            # Year reset marker
            if m_label == 'Jan 2026':
                for ci in range(1, n+1):
                    ws.cell(row=r, column=ci).border = Border(
                        top=Side(style='medium', color='C00000'),
                        left=Side(style='thin', color='BFBFBF'),
                        right=Side(style='thin', color='BFBFBF'),
                        bottom=Side(style='thin', color='BFBFBF'))

        # Scorecard row
        sc_row = 3 + len(months)
        ws.cell(row=sc_row, column=1, value='TOTALS / AVGS').fill = hfill(dark)
        ws.cell(row=sc_row, column=1).font = mfont(bold=True, color='FFFFFF', size=10)
        ws.cell(row=sc_row, column=1).alignment = CA

        ye_wavg  = wavg_vals[DEC]
        ytd_wavg = wavg_vals[-1]
        sc_vals  = [sum(r['closed'] for r in metrics),
                    round(sum(r['avg_days'] for r in metrics if r['closed']>0) /
                          max(sum(1 for r in metrics if r['closed']>0), 1)),
                    round(sum(r['ov90'] for r in metrics) / len(metrics))]
        if tab_name == 'Combined':
            sc_vals += [round(sum(r.get('ov90_car',0) for r in metrics)/len(metrics)),
                        round(sum(r.get('ov90_pto',0) for r in metrics)/len(metrics))]
        sc_vals += [f'{ye_wavg} (YE25) / {ytd_wavg} (YTD26)', '']

        for ci, v in enumerate(sc_vals, 2):
            c = ws.cell(row=sc_row, column=ci, value=v)
            c.fill = hfill(dark); c.font = mfont(bold=True, color='FFFFFF', size=10)
            c.alignment = CA

        for ci in range(1, n+1):
            ws.column_dimensions[get_column_letter(ci)].width = 16
        ws.column_dimensions['A'].width = 14

    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    return buf.read()
