import io
import unittest
from datetime import datetime, timedelta

import openpyxl

from data_engine import load_and_compute, load_and_compute_multi
from export_utils import export_regional_summary


def workbook_bytes():
    wb = openpyxl.Workbook()
    ls = wb.active
    ls.title = 'List source'
    ls.append(['Location', 'Location ID', 'Area'])
    ls.append(['Hamilton (Burlington), ON', 189, 'USNE'])
    ls.append(['Portland, ME', 142, 'USNE'])
    car = wb.create_sheet("CAR '26")
    car.append(['Location (drop-down)', 'Location ID', '#', 'CAR #', 'CAR type',
                'Organization', 'BRIEF DESCRIPTION OF NC', 'Area', 'QA initials',
                'CAR initialized date', 'Submitted', 'Completion',
                'Corrective Action Approved Date'])
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    car.append(['Hamilton (Burlington), ON', 189, 1, 'C1', '', '', 'issue', '', 'A',
                now-timedelta(days=140), '', '', now-timedelta(days=5)])
    car.append(['Portland, ME', 142, 2, 'C2', '', '', 'issue', '', 'A',
                now-timedelta(days=140), '', '', None])
    pto = wb.create_sheet("PTO '26")
    pto.append(['Location (drop-down)', 'Location ID', '#', 'PTO #', 'BRIEF DESCRIPTION',
                'PT Program', 'Parameter', 'Z-score', 'PTO initialized date',
                'QA initials', 'Effectiveness Review & date deemed effective'])
    pto.append(['Hamilton (Burlington), ON', 189, 1, 'P1', 'issue', '', '', '',
                now-timedelta(days=140), 'A', None])
    output = io.BytesIO()
    wb.save(output)
    return output.getvalue()


class CanadaSplitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = workbook_bytes()
        cls.unsplit = load_and_compute(io.BytesIO(cls.data), split_canada=False)
        cls.split = load_and_compute(io.BytesIO(cls.data), split_canada=True)
        cls.multi = load_and_compute_multi([io.BytesIO(cls.data), io.BytesIO(cls.data)],
                                          split_canada=True)

    def test_split_reassigns_locations_without_changing_nam(self):
        a, b = self.unsplit, self.split
        self.assertEqual(set(a['region_map']['USNE']), {'Hamilton (Burlington), ON', 'Portland, ME'})
        self.assertEqual(b['region_map']['Canada'], ['Hamilton (Burlington), ON'])
        self.assertEqual(b['region_map']['USNE'], ['Portland, ME'])
        for key in ('car_metrics', 'pto_metrics', 'cmb_metrics', 'car_wavg', 'pto_wavg', 'cmb_wavg'):
            self.assertEqual(a[key]['ALL'], b[key]['ALL'])
        for key in ('car_metrics', 'pto_metrics', 'cmb_metrics'):
            for original, canada, usne in zip(a[key]['REGION:USNE'],
                                               b[key]['REGION:Canada'], b[key]['REGION:USNE']):
                self.assertEqual(original['closed'], canada['closed'] + usne['closed'])
                self.assertEqual(original['ov90'], canada['ov90'] + usne['ov90'])

    def test_excel_export_matches_grouping_and_metrics(self):
        for result in (self.unsplit, self.split, self.multi):
            wb = openpyxl.load_workbook(io.BytesIO(export_regional_summary(result)), data_only=True)
            self.assertEqual(wb.sheetnames, ['Explanation', 'CARs', 'PTOs', 'Combined',
                                             'Completed Records', 'Open Over 90 Days'])
            for name, metric in [('CARs', 'car_metrics'), ('PTOs', 'pto_metrics'), ('Combined', 'cmb_metrics')]:
                rows = {str(row[0]).strip(): row for row in wb[name].iter_rows(values_only=True)
                        if row[0] is not None}
                self.assertEqual('Canada' in rows, result['split_canada'])
                for region in result['region_map']:
                    self.assertEqual(rows[region][2], result[metric]['REGION:' + region][-1]['ov90'])
            self.assertEqual(result['cmb_metrics']['ALL'], self.split['cmb_metrics']['ALL'])

            completed = list(wb['Completed Records'].iter_rows(min_row=5, values_only=True))
            aged = list(wb['Open Over 90 Days'].iter_rows(min_row=5, values_only=True))
            self.assertEqual([(r[0], r[4]) for r in completed], [('CAR', 'C1')])
            self.assertEqual({(r[0], r[4]) for r in aged}, {('CAR', 'C2'), ('PTO', 'P1')})
            current_year = result['month_labels'][-1].split()[-1]
            ytd = [row for label, row in zip(result['month_labels'], result['cmb_metrics']['ALL'])
                   if label.endswith(current_year)]
            self.assertEqual(len(completed), sum(r['closed'] for r in ytd))
            self.assertEqual(len(aged), result['cmb_metrics']['ALL'][-1]['ov90'])

    def test_master_format_retains_open_records(self):
        wb = openpyxl.Workbook()
        car = wb.active
        car.title = 'Data - CARs'
        car.append(['location', 'init_date', 'close_date', 'description', 'initials', 'car_number'])
        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        car.append(['Hamilton (Burlington), ON', now - timedelta(days=120), None, 'issue', 'A', 'C1'])
        car.append(['Portland, ME', now - timedelta(days=120), now - timedelta(days=1), 'issue', 'A', 'C2'])
        car.append(['Hamilton (Burlington), ON', now - timedelta(days=90), None, 'issue', 'A', 'C3'])
        car.append(['Hamilton (Burlington), ON', now - timedelta(days=91), None, '=not a formula', 'A', 'C4'])
        car.cell(car.max_row, 4).data_type = 's'
        pto = wb.create_sheet('Data - PTOs')
        pto.append(['location', 'init_date', 'close_date', 'description', 'initials', 'pto_number'])
        pto.append(['Hamilton (Burlington), ON', now - timedelta(days=120), None, 'issue', 'JN', 'P1'])
        pto.append(['Hamilton (Burlington), ON', now - timedelta(days=120), None, 'VOID', 'A', 'P2'])
        ls = wb.create_sheet('List source')
        ls.append(['Location', 'Location ID', 'Area'])
        ls.append(['Hamilton (Burlington), ON', 189, 'USNE'])
        ls.append(['Portland, ME', 142, 'USNE'])
        stream = io.BytesIO()
        wb.save(stream)
        result = load_and_compute(io.BytesIO(stream.getvalue()), exclude_jn=True, split_canada=True)
        self.assertEqual(result['car_metrics']['REGION:Canada'][-1]['ov90'], 2)
        self.assertEqual(result['pto_metrics']['REGION:Canada'][-1]['ov90'], 0)
        report = openpyxl.load_workbook(io.BytesIO(export_regional_summary(result)), data_only=False)
        rows = list(report['Open Over 90 Days'].iter_rows(min_row=5, values_only=True))
        self.assertEqual({row[4] for row in rows}, {'C1', 'C4'})
        self.assertEqual(next(row[8] for row in rows if row[4] == 'C4'), "'=not a formula")


if __name__ == '__main__':
    unittest.main()
