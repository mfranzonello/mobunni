# project selection and functions to read and write database and Excel data

import os
import getpass
import string
from math import floor, ceil

from pandas import ExcelWriter

# stores dataframe results and prints to multiple tabs of Excel spreadhseet
class Excelerator:
    def __init__(self, path=None, filename='results', extension='xlsx'):
        self.path = self.get_desktop() if path is None else path
        self.filename = filename
        self.extension = extension
        
        self.writer = None

        self.dataframes = {}
        self.print_index = {}
        self.charts = {}
        self.secondary_charts = {}
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
    def store_data(self, data, print_index={}):
        self.dataframes = data
        self.print_index = print_index

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
            dfs = self.dataframes[sheetname]

            if type(dfs) is not list:
                dfs = [dfs]

            df = dfs[chart.get('table number', 0)]

            max_rows, max_columns = df.shape
            rows = max_rows - chart.get('totals row', 0) - chart.get('extra row', 0)
            chart_sheet_name = chart.get('chart sheet name')

            chart_columns = chart.get('columns')
            _, column_indices = self.get_indices(sheetname, chart_columns)
            #chart_zip = zip(column_indices, chart['colors'], chart['dashes'], chart['weights'])

            items = {'colors': 'color',
                     'dashes': 'dash',
                     'weights': 'weight'}

            columns = {column_indices[i]: {items[item]: chart.get(item)[i] for item in items if chart.get(item) is not None} for i in range(len(column_indices))}

            if chart_sheet_name not in self.charts:
                # primary chart
                offset = None if chart_sheet_name else [chart.get('header row', 0) + 1, max_columns + 1]
                self.charts[chart_sheet_name] = {'sheetname': sheetname, 'columns': columns, 'rows': rows, 'header row': chart.get('header row'),
                                                 'type': chart.get('type'), 'subtype': chart.get('subtype'),
                                                 'offset': offset, 'chart sheet name': chart_sheet_name, 'constants': chart.get('constants'),
                                                 'y-axis': chart.get('y-axis'), 'y2-axis': chart.get('y2-axis'),
                                                 'x-axis': chart.get('x-axis'), 'x2-axis': chart.get('x2-axis')}

            else:
                # secondary chart
                self.secondary_charts[chart_sheet_name] = {'sheetname': sheetname, 'columns': columns, 'rows': rows, 'header row': chart.get('header row')}
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
            df.to_excel(writer, sheet_name=sheetname, index=self.print_index.get(sheetname, False), startrow=row, startcol=0)
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
    def add_chart(self, writer, chart, secondary_chart=None):
        sheetname = chart['sheetname']
        columns = chart['columns']
        rows = chart['rows']
        offset = chart.get('offset')
        chart_sheet_name = chart.get('chart sheet name')
        axes = {xy: chart.get('{}-axis'.format(xy)) for xy in ['y', 'y2', 'x', 'x2']}
        constants = chart.get('constants')
        chart_type = chart.get('type', 'line')
        header_row = chart.get('header_row', 0)
        category_column = chart.get('category column', 0)
        
        if not (offset or chart_sheet_name):
            offset = [1, 1]
        workbook = writer.book
        worksheet = writer.sheets[sheetname]
        added_chart = workbook.add_chart({'type': chart_type})

        # add line chart
        if chart_type in ['line', 'scatter']:
            for column in columns:
                added_chart.add_series({'name': [sheetname, header_row, column],
                                        'categories': [sheetname, header_row + 1, category_column, header_row + rows, category_column],
                                        'values': [sheetname, header_row + 1, column, header_row + rows, column],
                                        'border': {'color': columns[column]['color'], 'dash_type': columns[column]['dash'], 'width': columns[column]['weight']}
                                        })

            if constants:
                for constant in constants:
                    if constant['value']:
                        values = '={' + ','.join([str(constant['value'])] * (rows - header_row)) + '}'
                        added_chart.add_series({'name': constant['name'],
                                                'categories': [sheetname, header_row + 1, category_column, header_row + rows, category_column],
                                                'values': values,
                                                'border': {'color': constant['color'], 'dash_type': constant['dash']}})

        # add second bar chart
        if secondary_chart is not None:
            sheetname_2 = secondary_chart['sheetname']
            chart_type_2 = secondary_chart.get('type', 'column')
            chart_subtype_2 = secondary_chart.get('subtype')
            added_chart_2 = workbook.add_chart({'type': chart_type_2, 'subtype': chart_subtype_2})
            if chart_type_2 in ['column']:
                columns_2 = secondary_chart['columns']
                rows_2 = secondary_chart['rows']
                header_row_2 = secondary_chart.get('header row', 0)
                category_column_2 = secondary_chart.get('category column', 0)

                for column_2 in columns_2:
                    added_chart_2.add_series({'name': [sheetname_2, header_row_2, column_2],
                                              'categories': [sheetname_2, header_row_2 + 1, category_column_2, header_row_2 + rows_2, category_column_2],
                                              'values': [sheetname_2, header_row_2 + 1, column_2, header_row_2 + rows_2, column_2],
                                              'fill': {'color': columns_2[column_2]['color']},
                                              'y2_axis': axes['y2'] is not None,
                                              'x2_axis': axes['x2'] is not None})
            
                # set secondary axes
                if axes['y2']:
                    added_chart_2.set_y2_axis(axes['y2'])
                if axes['x2']:
                    added_chart_2.set_x2_axis(axes['x2'])

                # combine charts
                added_chart.combine(added_chart_2)
               
        # set primary axes
        if axes['y']:
            added_chart.set_y_axis(axes['y'])
        if axes['x']:
            added_chart.set_x_axis(axes['x'])

        # paste chart
        if offset:
            # paste on same sheet as data
            insert_address = self.get_excel_address(offset[0], offset[1])
            worksheet.insert_chart(insert_address, added_chart)
        else:
            # paste on different sheet than data
            chartsheet = workbook.add_chartsheet(chart_sheet_name)
            chartsheet.set_chart(added_chart)

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
        for chart_sheet_name in self.charts:
            chart = self.charts[chart_sheet_name]
            secondary_chart = self.secondary_charts.get(chart_sheet_name)
            self.add_chart(writer, chart, secondary_chart=secondary_chart)

        writer.save()

        if start:
            os.startfile(outpath)
        return

class ExcelePaint:
    percent_values = ['TMO', 'eff']
    comma_values = ['power', 'fuel', 'ceiling loss']
    date_values = ['date']
    quant_values = ['created FRU']

    ranges = {'C': '#2E86C1', 'W': '#E74C3C', 'P': '#28B463'}
    ranges_lite = {'C': '#85C1E9', 'W': '#F1948A', 'P': '#82E0AA'}
    
    ranges_2 = {'created FRU': '#C43F35'}

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
              'money': 10,
              'cash': 25}
    heights = {'costs': 'all'}

    def get_paints(windowed, limits, inputs, performance, cost_tables, power, efficiency, transactions, cash_flow):
        ranges = ExcelePaint.ranges.copy()

        if not windowed:
            ranges.pop('W')

        columns = {'percent': ['{}{}{}'.format(r, v, b) for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds],
                   'comma': ['{}{}'.format(v, b) for v in ExcelePaint.comma_values for b in ExcelePaint.bounds],
                   'date': ExcelePaint.date_values}
        columns_2 = [r for r in ExcelePaint.ranges_2]

        styles = {'performance': {ns: columns[ns] for ns in ExcelePaint.num_styles if ns in columns},
                  'power': {'date': ['date'], 'comma': None},
                  'efficiency': {'date': ['date'], 'percent': None},
                  'inputs': {col: [col] for col in ['input', 'value']},
                  'transactions': {'date': ['date'], 'value': ['serial', 'mark'], 'action': ['action'], 'money': ['service cost'],
                                   'comma': ['power'], 'percent': ['efficiency']},
                  'costs': {'action': None}}

        indices = ['cash flow']

        data = ExcelePaint._get_data(inputs, performance, cost_tables, power, efficiency, transactions, cash_flow)
        print_index = ExcelePaint._get_print_index(indices)
        formats = ExcelePaint._get_formats(windowed, styles)
        charts = ExcelePaint._get_charts(windowed, limits, performance, cost_tables, columns['percent'], columns_2, ranges)

        return data, print_index, formats, charts

    def _get_data(inputs, performance, cost_tables, power, efficiency, transactions, cash_flow):
        data = {'inputs': inputs,
                'performance': performance, 'costs': [cost_tables[c] for c in cost_tables],
                'power': power, 'efficiency': efficiency,
                'transactions': transactions,
                'cash flow': cash_flow}

        return data

    def _get_print_index(indices):
        print_index = {idx: True for idx in indices}
        return print_index

    def _get_formats(windowed, styles):
        formats = [{'sheetname': sheetname, 'columns': cols,
                    'style': ExcelePaint.num_styles.get(style),
                    'width': ExcelePaint.widths.get(style),
                    'rows': ExcelePaint.heights.get(sheetname)} for sheetname in styles \
            for (cols, style) in zip(styles[sheetname].values(), styles[sheetname].keys())]

        return formats

    def _get_charts(windowed, limits, performance, cost_tables, chart_columns, chart_columns_2, ranges):
        key_1, key_2 = list(cost_tables.keys())[0:2]
        cost_table = cost_tables[key_2]
        header_row_2 = len(cost_tables[key_1]) + 2

        chart_colors = [ranges[r] for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds]
        chart_dashes = [ExcelePaint.bounds.get(b, 'solid') for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds]
        chart_weights = [ExcelePaint.weights.get(b, 2.25) for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds]

        chart_2_colors = [ExcelePaint.ranges_2[r] for r in ExcelePaint.ranges_2]

        chart_y_axis = {'name': 'performance',
                        'max': 1.0, 'min': floor(10*performance[chart_columns].min().min())/10,
                        'major_gridlines': {'visible': True, 'line': {'color': '#F4F6F6'}}}
        chart_y2_axis = {'name': 'FRU replacements',
                         'max': ceil(cost_table[chart_columns_2[0]][0:-1].max() * 4), 'min': 0}
        chart_x_axis = {'name': 'date', 'visible': True}
        chart_x2_axis = {'visible': True}

        chart_constants = [{'name': '{}{}{}'.format(r, v, b),
                            'value': limits['{}{}'.format(r, v)],
                            'color': ExcelePaint.ranges_lite[r],
                            'dash': ExcelePaint.bounds_lite[b],
                            } for r in ranges for v in ExcelePaint.percent_values for b in ExcelePaint.bounds_lite]
        charts = [{'sheetname': 'performance', 'type': 'line', 'columns': chart_columns,
                   'colors': chart_colors, 'dashes': chart_dashes, 'weights': chart_weights,
                   'chart sheet name': 'graph', 'constants': chart_constants,
                   'y-axis': chart_y_axis, 'y2-axis': chart_y2_axis, 'x-axis': chart_x_axis, 'x2-axis': chart_x2_axis},
                  {'sheetname': 'costs', 'type': 'column', 'subtype': 'stacked', 'columns': chart_columns_2,
                   'colors': chart_2_colors,
                   'header row': header_row_2, 'table number': -1, 'totals row': True, 'extra row': True,
                   'chart sheet name': 'graph'},
                  ]

        return charts