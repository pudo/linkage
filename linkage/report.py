import re
import xlsxwriter

from datetime import datetime

COLOR = '#982022'


class ExcelReport(object):

    def __init__(self, linkage):
        self.linkage = linkage
        self.workbook = xlsxwriter.Workbook(self.linkage.report)
        self.header_format = self.workbook.add_format({
            'bold': True,
            'fg_color': COLOR,
            'font_color': 'white'
        })
        self.link_format = self.workbook.add_format({
            'font_color': 'blue',
            'underline': 1
        })
        self.title_format = self.workbook.add_format({
            'bold': True,
            'font_size': 20,
            'font_color': COLOR
        })
        self.subtitle_format = self.workbook.add_format({
            'bold': True,
            'font_size': 14,
            'font_color': '#666666'
        })
        self.muted_format = self.workbook.add_format({
            'font_color': '#666666'
        })

    def generate(self):
        self.generate_overview()
        self.generate_crossrefs()
        self.workbook.close()

    def generate_overview(self):
        worksheet = self.workbook.add_worksheet('Overview')
        worksheet.set_zoom(125)
        widths = {}
        offset = 1
        worksheet.write(offset, 0, "Dataset Cross-Referencing Report",
                        self.title_format)
        offset += 1
        last_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        worksheet.write(offset, 0, "Last update: %s" % last_update)
        offset += 2
        worksheet.write(offset, 0, "Matches found between datasets",
                        self.subtitle_format)
        offset += 1
        for i, header in enumerate(['Dataset A', 'Dataset B',
                                    'Matches', 'Details']):
            worksheet.write(offset, i, header, self.header_format)

        offset += 1
        for crossref in self.linkage.crossrefs:
            if not crossref.ignore:
                worksheet.write(offset, 0, crossref.left.label)
                widths[0] = max(len(crossref.left.label), widths.get(0, 1))
                worksheet.write(offset, 1, crossref.right.label)
                widths[1] = max(len(crossref.right.label), widths.get(1, 1))
                worksheet.write(offset, 2, len(crossref))
                url = "internal:'%s'!B3" % self.sheet_name(crossref)
                worksheet.write_url(offset, 3, url, self.link_format,
                                    'See matches')
                offset += 1

        offset += 3
        worksheet.write(offset, 0, "No matches found",
                        self.subtitle_format)
        offset += 1
        for i, header in enumerate(['Dataset A', 'Dataset B']):
            worksheet.write(offset, i, header, self.header_format)

        offset += 1
        for crossref in self.linkage.crossrefs:
            if crossref.ignore:
                worksheet.write(offset, 0, crossref.left.label,
                                self.muted_format)
                widths[0] = max(len(crossref.left.label), widths.get(0, 1))
                worksheet.write(offset, 1, crossref.right.label,
                                self.muted_format)
                widths[1] = max(len(crossref.right.label), widths.get(1, 1))
                offset += 1

        for idx, max_len in widths.items():
            max_len = min(200, max(7, max_len + 1))
            worksheet.set_column(idx, idx, float(max_len))
        worksheet.set_column(3, 3, 15)

    def generate_crossrefs(self):
        for crossref in self.linkage.crossrefs:
            if not crossref.ignore:
                self.generate_crossref(crossref)

    def generate_crossref(self, crossref):
        label = self.sheet_name(crossref)

        worksheet = self.workbook.add_worksheet(label)
        worksheet.set_zoom(125)
        self.generate_crossref_header(worksheet, crossref)
        widths = {}

        for i, row in enumerate(crossref.results):
            j = 0
            worksheet.write(i + 2, j, row['score'])
            for view in [crossref.left, crossref.right]:
                for field in view.fields:
                    j += 1
                    value = row[field.column_ref]
                    worksheet.write(i + 2, j, value)
                    widths[j] = max(len(unicode(value)), widths.get(j, 1))

        for idx, max_len in widths.items():
            max_len = min(50, max(7, max_len + 1))
            worksheet.set_column(idx, idx, float(max_len))

        worksheet.set_column(0, 0, 5)
        worksheet.conditional_format(2, 0, 2 + len(crossref), 0,
                                     {'type': 'data_bar'})
        worksheet.autofilter(1, 1, len(crossref) + 2, j)

    def generate_crossref_header(self, worksheet, crossref):
        worksheet.freeze_panes(2, 0)
        worksheet.merge_range(0, 0, 1, 0, 'Score', self.header_format)
        width = len(crossref.left.fields)
        if width == 1:
            worksheet.write(0, 1, crossref.left.label, self.header_format)
        else:
            worksheet.merge_range(0, 1, 0, width,
                                  crossref.left.label,
                                  self.header_format)
        offset_left = width + 1
        width = len(crossref.right.fields)
        if width == 1:
            worksheet.write(0, offset_left, crossref.right.label,
                            self.header_format)
        else:
            worksheet.merge_range(0, offset_left, 0, offset_left + width - 1,
                                  crossref.right.label, self.header_format)

        j = 0
        for view in [crossref.left, crossref.right]:
            for field in view.fields:
                j += 1
                worksheet.write(1, j, field.label, self.header_format)

    def sheet_name(self, crossref):
        name = '%s-%s' % (crossref.left.name, crossref.right.name)
        name = re.sub(r'[\[\]\\\/\:\?]*', '', name)
        return name[:31]
