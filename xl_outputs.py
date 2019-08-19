# project selection and functions to read and write database and Excel data

import os
import getpass
import string
from math import floor

from pandas import ExcelWriter

# stores dataframe results and prints to multiple tabs of Excel spreadhseet
class Excelerator:
    def __init__(self, path=None, filename='results', extension='xlsx'):
        self.path = self.get_desktop() if path is None else path
        self.filename = filename
        self.extension = extension
        
        self.writer = None

        self.dataframes = {}
        self.charts = []
        self.formats = []

    # get windows username to print to desktop by default
    def get_desktop(self):
        username = getpass.getuser()
        desktop = r'c:\users\{}\Desktop\bpm results'.format(username)
        self.ensure_folder(desktop)

        return desktop

    # make sure folder exists and create if it doesn't
    def ensure_folder(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)
        return

    # find next available filename to avoid overwrite errors
    def next_file(folder, filename, extension):
        next_name = '{}'.format(filename)
        c = 0
        while '{}.{}'.format(next_name, extension) in os.listdir(folder):
            c += 1
            next_name = '{}_{}'.format(filename, c)
        return next_name

    def get_excel_row(self, row):
        xl_row = row + 1
        return xl_row

    def get_excel_column(self, column):
        columns = string.ascii_uppercase
        xl_column = columns[column % len(columns)]
        if column >= len(columns):
            xl_column = columns[floor(column/len(columns) - 1) % len(columns)] + xl_column

        if column >= len(columns) ** 2:
            xl_column = columns[floor(column/len(columns)**2 - 1) % len(columns)] + xl_column

        return xl_column

    # get the excel $C$R format of a position
    def get_excel_address(self, row, column, fix=False):
        xl_row = self.get_excel_row(row)
        xl_column = self.get_excel_column(column)
        
        fixed = '$' if fix else ''
        address = '{}{}{}{}'.format(fixed, xl_column, fixed, xl_row)

        return address

    # rows and columns to apply formatting
    def get_indices(self, sheetname, columns, rows=None):
        df = self.dataframes[sheetname]

        if type(df) is list:
            df = df[0]

        r, c = df.shape

        if rows is None:
            row_indices = None
        else:
            r_start = 0
            if rows == 'all':
                row_indices = range(r_start, r_start + r + 1)
            elif type(rows) is list:
                row_indices = [df.iloc[:, 0].index.index(row) + r_start for row in rows]
        
        if columns is None:
            column_indices = range(1, c + 1)
        else:
            column_indices = [df.columns.to_list().index(c) for c in columns]
        return row_indices, column_indices

    # store sheet value data
    def store_data(self, data):
        self.dataframes = data
        return

    def store_formats(self, formats):
        for style in formats:
            sheetname = style['sheetname']
            rows, columns = self.get_indices(sheetname, style['columns'], style.get('rows'))
            self.formats.append({'sheetname': sheetname, 'style': style.get('style'), 'columns': columns,
                                 'rows': rows, 'width': style.get('width')})
        return

    # store chart data
    def store_charts(self, charts):
        for chart in charts:
            sheetname = chart['sheetname']
            df = self.dataframes[sheetname]

            chart_columns = chart.get('columns')
            _, column_indices = self.get_indices(sheetname, chart_columns)
            chart_zip = zip(column_indices, chart['colors'], chart['dashes'], chart['weights'])
            columns = {column_index: {'color': color, 'dash': dash, 'weight': weight} for (column_index, color, dash, weight) in chart_zip}

            max_rows, max_columns = df.shape
            chart_sheet_name = chart.get('chart sheet name')
            offset = None if chart_sheet_name else [chart.get('header row', 0) + 1, max_columns + 1]

            self.charts.append({'sheetname': sheetname, 'columns': columns, 'rows': max_rows,
                                'offset': offset, 'chart sheet name': chart_sheet_name,
                                'y-axis': chart.get('y-axis'), 'constants': chart.get('constants')})
        return

    # add values to an output sheet
    def add_data(self, writer, sheetname):
        dfs = self.dataframes[sheetname]

        if type(dfs) is not list:
            dfs = [dfs]

        workbook = writer.book
        worksheet = workbook.add_worksheet(sheetname)
        writer.sheets[sheetname] = worksheet
        row = 0
        for df in dfs:
            df.to_excel(writer, sheet_name=sheetname, index=False, startrow=row, startcol=0)
            row += df.shape[0] + 2 # header + space
        return

    def add_format(self, writer, sheetname, style, columns, table=None, rows=None, width=None):
        workbook = writer.book
        worksheet = writer.sheets[sheetname]
        if style:
            format_style = workbook.add_format({'num_format': style})
        else:
            format_style = None

        for column in columns:
            #xl_column = self.get_excel_column(column)
            #xl_range = '{}:{}'.format(xl_column, xl_column)
            worksheet.set_column(column, column, width, format_style if not rows else None)
            #if rows:
            #    df = self.dataframes[sheetname]
            #    if table:
            #        df = df[table]
                
            #    for row in rows:
            #        worksheet.write(row, column, df.loc[row, column], format_style)
     
    # add a chart to the output
    def add_chart(self, writer, chart):
        sheetname = chart['sheetname']
        columns = chart['columns']
        rows = chart['rows']
        offset = chart.get('offset')
        chart_sheet_name = chart.get('chart sheet name')
        y_axis = chart.get('y-axis')
        constants = chart.get('constants')
        chart_type = chart.get('type', 'line')
        header_row = chart.get('header_row', 0)
        category_column = chart.get('category column', 0)
        
        if not (offset or chart_sheet_name):
            offset = [1, 1]
        workbook = writer.book
        worksheet = writer.sheets[sheetname]
        chart = workbook.add_chart({'type': chart_type})

        if chart_type in ['line', 'scatter']:
            for column in columns:
                chart.add_series({'name': [sheetname, header_row, column],
                                  'categories': [sheetname, 1, header_row, rows, category_column],
                                  'values': [sheetname, 1, column, rows, column],
                                  'border': {'color': columns[column]['color'], 'dash_type': columns[column]['dash'], 'width': columns[column]['weight']}
                                  })

            if constants:
                for constant in constants:
                    if constant['value']:
                        values = '={' + ','.join([str(constant['value'])] * (rows - header_row)) + '}'
                        chart.add_series({'name': constant['name'],
                                          'categories': [sheetname, 1, header_row, rows, category_column],
                                          'values': values,
                                          'border': {'color': constant['color'], 'dash_type': constant['dash']}})
               
        if y_axis:
            chart.set_y_axis(y_axis)

        if offset:
            insert_address = self.get_excel_address(offset[0], offset[1])
            worksheet.insert_chart(insert_address, chart)
        else:
            chartsheet = workbook.add_chartsheet(chart_sheet_name)
            chartsheet.set_chart(chart)

    # print output to Excel file and open
    def to_excel(self, start=False):
        next_file = Excelerator.next_file(self.path, self.filename, self.extension)
        outpath = r'{}\{}.{}'.format(self.path, next_file, self.extension)
        writer = ExcelWriter(outpath, engine='xlsxwriter')

        # assemble outputs
        for sheetname in self.dataframes:
            self.add_data(writer, sheetname)
        for style in self.formats:
            self.add_format(writer, style['sheetname'], style['style'], style['columns'],
                            rows=style.get('rows'), width=style.get('width'))
        for chart in self.charts:
            self.add_chart(writer, chart)

        writer.save()

        if start:
            os.startfile(outpath)
        return

class ExcelePaint:
    percent_values = ['TMO', 'eff']
    comma_values = ['power', 'fuel', 'ceiling loss']
    date_values = ['date']

    ranges = {'C': '#2E86C1', 'W': '#E74C3C', 'P': '#28B463'}
    ranges_lite = {'C': '#85C1E9', 'W': '#F1948A', 'P': '#82E0AA'}
    bounds = {'_max': 'dash', '': 'solid', '_min': 'dash', '_25': 'solid', '_75': 'solid'}
    bounds_lite = {'_limit': 'round_dot'}
    weights = {'_25': 1, '_75': 1}

    num_styles = {'percent': '0.00%',
                  'comma': '#,##0',
                  'date': 'mm/yyyy',
                  'money': '_($* #,##0_);_($* (#,##0);_($* "-"_);_(@_)'}
    widths = {'date': 12,
              'input': 50,
              'value': 12,
              'action': 18,
              'money': 10}
    heights = {'costs': 'all'}

    def get_paints(windowed, limits, inputs, performance, costs, power, efficiency, transactions):
        ranges = ExcelePaint.ranges.copy()

        if not windowed:
            ranges.pop('W')

        columns = {'percent': ['{}{}{}'.format(r, v, b) for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds],
                   'comma': ['{}{}'.format(v, b) for v in ExcelePaint.comma_values for b in ExcelePaint.bounds],
                   'date': ExcelePaint.date_values}

        styles = {'performance': {ns: columns[ns] for ns in ExcelePaint.num_styles if ns in columns},
                  'power': {'date': ['date'], 'comma': None},
                  'efficiency': {'date': ['date'], 'percent': None},
                  'inputs': {col: [col] for col in ['input', 'value']},
                  'transactions': {'date': ['date'], 'value': ['serial', 'mark'], 'action': ['action'], 'money': ['service cost'],
                                   'comma': ['power'], 'percent': ['efficiency']},
                  'costs': {'action': None}}

        data = ExcelePaint._get_data(inputs, performance, costs, power, efficiency, transactions)
        formats = ExcelePaint._get_formats(windowed, styles)
        charts = ExcelePaint._get_charts(windowed, limits, performance, columns['percent'], ranges)

        return data, formats, charts

    def _get_data(inputs, performance, costs, power, efficiency, transactions):
        data = {'inputs': inputs,
                'performance': performance, 'costs': costs,
                'power': power, 'efficiency': efficiency,
                'transactions': transactions}

        return data

    def _get_formats(windowed, styles):
        formats = [{'sheetname': sheetname, 'columns': cols,
                    'style': ExcelePaint.num_styles.get(style),
                    'width': ExcelePaint.widths.get(style),
                    'rows': ExcelePaint.heights.get(sheetname)} for sheetname in styles \
            for (cols, style) in zip(styles[sheetname].values(), styles[sheetname].keys())]


        return formats

    def _get_charts(windowed, limits, performance, chart_columns, ranges):
        chart_colors = [ranges[r] for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds]
        chart_dashes = [ExcelePaint.bounds.get(b, 'solid') for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds]
        chart_weights = [ExcelePaint.weights.get(b, 2.25) for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds]
        chart_y_axis = {'max': 1.0, 'min': floor(10*performance[chart_columns].min().min())/10,
                        'major_gridlines': {'visible': True, 'line': {'color': '#F4F6F6'}}}
        chart_constants = [{'name': '{}{}{}'.format(r, v, b),
                            'value': limits['{}{}'.format(r, v)],
                            'color': ExcelePaint.ranges_lite[r],
                            'dash': ExcelePaint.bounds_lite[b],
                            } for r in ranges for v in ExcelePaint.percent_values for b in ExcelePaint.bounds_lite]
        charts = [{'sheetname': 'performance', 'type': 'line', 'columns': chart_columns,
                   'colors': chart_colors, 'dashes': chart_dashes, 'weights': chart_weights,
                   'chart sheet name': 'graph', 'y-axis': chart_y_axis, 'constants': chart_constants},
                  ]

        return charts