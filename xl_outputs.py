# project selection and functions to read and write database and Excel data

# built-in imports
import os
import getpass
import string
from math import floor, ceil

# add-on imports
from pandas import ExcelWriter, DataFrame

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
        self.tabs = {}

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
        elif columns == 0:
            column_indices = [0]
        else:
            column_indices = [df.columns.to_list().index(c) + self.print_index.get(sheetname, 0) for c in columns]
        return row_indices, column_indices

    # store sheet value data
    def store_data(self, data, print_index={}):
        self.dataframes = data
        self.print_index = print_index

        return

    def store_formats(self, formats):
        for style in formats:
            sheetname = style['sheetname']
            rows, columns = self.get_indices(sheetname, style['columns'], rows=style.get('rows'))
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

            items = {'colors': 'color',
                     'dashes': 'dash',
                     'weights': 'weight'}

            columns = {column_indices[i]: {items[item]: chart.get(item)[i] for item in items if chart.get(item) is not None} for i in range(len(column_indices))}

            if chart_sheet_name not in self.charts:
                # primary chart
                offset = None if chart_sheet_name else [chart.get('header row', 0) + 1, max_columns + 1]
                self.charts[chart_sheet_name] = {'sheetname': sheetname, 'columns': columns, 'rows': rows, 'header row': chart.get('header row', 0),
                                                 'type': chart.get('type'), 'subtype': chart.get('subtype'),
                                                 'offset': offset, 'chart sheet name': chart_sheet_name, 'constants': chart.get('constants'),
                                                 'y-axis': chart.get('y-axis'), 'y2-axis': chart.get('y2-axis'),
                                                 'x-axis': chart.get('x-axis'), 'x2-axis': chart.get('x2-axis')}

            else:
                # secondary chart
                self.secondary_charts[chart_sheet_name] = {'sheetname': sheetname, 'columns': columns, 'rows': rows, 'header row': chart.get('header row'),
                                                           'type': chart.get('type'), 'subtype': chart.get('subtype')}
        return

    # add tab colors to workbook
    def store_tabs(self, tabs):
        self.tabs = tabs

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
            worksheet.set_column(column, column, width, format_style if not rows else None)
            #if rows:
            #    df = self.dataframes[sheetname]
            #    if table:
            #        df = df[table]
                
            #    for row in rows:
            #        worksheet.write(row, column, df.loc[row, column], format_style)
     
    # add a chart to the output
    def add_chart(self, writer, chart, secondary_chart=None):
        chart_sheet_name = chart.get('chart sheet name', chart['sheetname'])
        offset = chart.get('offset')

        if not (offset or chart_sheet_name):
            offset = [1, 1]

        workbook = writer.book

        # create primary chart
        added_chart = self.create_chart(workbook, chart)     
        axes = {xy: chart.get('{}-axis'.format(xy)) for xy in ['y', 'y2', 'x', 'x2']}

        # create secondary chart and combine
        if secondary_chart is not None:
            added_chart_2 = self.create_chart(workbook, secondary_chart, secondary=True)

            # set secondary axes
            if axes['x2']: added_chart_2.set_x2_axis(axes['x2'])
            if axes['y2']: added_chart_2.set_y2_axis(axes['y2'])

            # combine charts
            added_chart.combine(added_chart_2)
               
        # set primary axes
        if axes['x']: added_chart.set_x_axis(axes['x'])
        if axes['y']: added_chart.set_y_axis(axes['y'])

        # paste chart
        if chart_sheet_name not in writer.sheets:
            chartsheet = workbook.add_chartsheet(chart_sheet_name)
        else:
            chartsheet = writer.sheets[chart_sheet_name]

        if offset:
            # paste on same sheet as data
            insert_address = self.get_excel_address(offset[0], offset[1])
            chartsheet.insert_chart(insert_address, added_chart)
        else:
            # paste on different sheet than data
            chartsheet.set_chart(added_chart)

    # create chart object
    def create_chart(self, workbook, chart, secondary=False):
        sheetname = chart['sheetname']
        columns = chart['columns']
        rows = chart['rows']
        
        chart_type = chart.get('type', 'line')
        chart_subtype = chart.get('subtype')

        header_row = chart.get('header row', 0)
        category_column = chart.get('category column', 0)

        chart_parameters = {'type': chart_type,
                            }
        if secondary:
            chart_parameters.update({'subtype': chart_subtype,
                                     })
        added_chart = workbook.add_chart(chart_parameters)

        categories = [sheetname, header_row + 1, category_column, header_row + rows, category_column]

        added_chart = self.create_series(added_chart, chart['columns'], sheetname, categories, category_column, rows, header_row, chart_type,
                                         chart_subtype=chart_subtype, secondary=secondary)
        if chart.get('constants') is not None:
            added_chart = self.create_series(added_chart, chart['constants'], sheetname, categories, category_column, rows, header_row, chart_type,
                                             constant_lines=True)

        return added_chart

    # create series for chart object
    def create_series(self, added_chart, columns, sheetname, categories, category_column, rows, header_row, chart_type,
                      chart_subtype=None, constant_lines=False, secondary=False):
        for column in columns:
            if constant_lines:
                name = column['name']
                values = '={' + ','.join([str(column['value'])] * (rows - header_row)) + '}'
            else:
                name = [sheetname, header_row, column]
                values = [sheetname, header_row + 1, column, header_row + rows, column]

            series_parameters = {'name': name,
                                 'categories': categories,
                                 'values': values,
                                 }                            

            if constant_lines:
                # add constant value line
                series_parameters.update({'border': {'color': column['color'], 'dash_type': column['dash']},
                                          })
            elif chart_type in ['line', 'scatter']:
                # add line chart
                series_parameters.update({'border': {'color': columns[column]['color'], 'dash_type': columns[column]['dash'], 'width': columns[column]['weight']},
                                          })
            elif chart_type in ['column']:
                # add bar chart
                series_parameters.update({'fill': {'color': columns[column]['color']},
                                          'overlap': 100 if chart_subtype == 'stacked' else 0,
                                          })

            if secondary:
                # move to secondary axes
                series_parameters.update({'{}2_axis'.format(xy): True for xy in ['x', 'y']})

            added_chart.add_series(series_parameters)

        return added_chart

    # color tab
    def color_tab(self, writer, sheet_name):

        #if self.tabs[sheet_name] in writer.sheets:
        workbook = writer.book
        worksheet = workbook.get_worksheet_by_name(sheet_name)
        worksheet.set_tab_color(self.tabs[sheet_name])
        #else:
        #    print("can't find {} tab that should be colored {}".format(sheet_name, self.tabs[sheet_name]))

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

        for sheet_name in self.tabs:
            self.color_tab(writer, sheet_name)

        writer.save()

        if start:
            os.startfile(outpath)
        return

class ExcelePaint:
    percent_values = ['TMO', 'eff']
    comma_values = ['power', 'fuel', 'ceiling loss']
    date_values = ['date']
    quant_values = ['created FRU']

    ranges = {'C': '#2E86C1', 'W': '#C43F35', 'P': '#28B463'}
    ranges_lite = {'C': '#85C1E9', 'W': '#FA9696', 'P': '#82E0AA'}
    
    ranges_2 = {'created FRU': '#F5A142', 'deployed FRU': '#FFBE0D', 'stored FRU': '#702CE6'}

    bounds = {'_max': 'dash', '': 'solid', '_min': 'dash', '_25': 'solid', '_75': 'solid'}
    bounds_lite = {'_limit': 'round_dot'}
    weights = {w: 1 for w in ['_25', '_75', '_min', '_max']}

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

    tabs = {'inputs': {'symbol': '⌨',
                       'color': '#511849'},
            'performance': {'symbol': '🌠',
                            'color': '#FF8D1A'},
            'costs': {'symbol': '💰',
                      'color': '#900C3F'},
            'power': {'symbol': '🔌',
                      'color': '#57C785'},
            'efficiency': {'symbol': '⛽️',
                           'color': '#ADD45C'},
            'transactions': {'symbol': '📒',
                             'color': '#FFC300'},
            'cash flow': {'symbol': '💵',
                          'color': '#3D3D6B'},
            'graph': {'symbol': '📊',
                      'color': '#2A7B9B'},
            }

    def get_paints(windowed, limits, inputs, performance, cost_tables, power, efficiency, transactions, cash_flow):
        ranges = ExcelePaint.ranges.copy()

        if not windowed:
            ranges.pop('W')

        columns = {'percent': ['{}{}{}'.format(r, v, b) for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds],
                   'comma': ['{}{}'.format(v, b) for v in ExcelePaint.comma_values for b in ExcelePaint.bounds],
                   'date': ExcelePaint.date_values,
                   }

        cost_key = 'power'
        columns_2 = [r for r in ExcelePaint.ranges_2 if r in cost_tables[cost_key].columns]

        styles = {'performance': {ns: columns[ns] for ns in ExcelePaint.num_styles if ns in columns},
                  'power': {'date': ['date'], 'comma': None},
                  'efficiency': {'date': ['date'], 'percent': None},
                  'inputs': {col: [col] for col in ['input', 'value']},
                  'transactions': {'date': ['date'], 'value': ['serial', 'model number'], 'action': ['action'], 'money': ['service cost'],
                                   'comma': ['power'], 'percent': ['efficiency']},
                  'costs': {'action': None},
                  'cash flow': {'cash': 0},
                  }

        indices = ['cash flow']

        data = ExcelePaint._get_data(inputs, performance, cost_tables, power, efficiency, transactions, cash_flow)
        print_index = ExcelePaint._get_print_index(indices)
        formats = ExcelePaint._get_formats(styles)
        charts = ExcelePaint._get_charts(limits, ranges, performance, cost_tables, columns['percent'], columns_2, cost_key=cost_key)
        tabs = ExcelePaint._get_tabs()

        return data, print_index, formats, charts, tabs

    def _get_data(inputs, performance, cost_tables, power, efficiency, transactions, cash_flow):
        data = {ExcelePaint._get_symbol('inputs'): inputs,
                ExcelePaint._get_symbol('performance'): performance,
                ExcelePaint._get_symbol('costs'): [cost_tables[c] for c in cost_tables],
                ExcelePaint._get_symbol('power'): power,
                ExcelePaint._get_symbol('efficiency'): efficiency,
                ExcelePaint._get_symbol('transactions'): transactions,
                ExcelePaint._get_symbol('cash flow'): cash_flow,
                }

        return data

    def _get_print_index(indices):
        print_index = {ExcelePaint._get_symbol(idx): True for idx in indices}
        return print_index

    def _get_formats(styles):
        formats = [{'sheetname': ExcelePaint._get_symbol(sheetname), 'columns': cols,
                    'style': ExcelePaint.num_styles.get(style),
                    'width': ExcelePaint.widths.get(style),
                    'rows': ExcelePaint.heights.get(sheetname)} for sheetname in styles \
                    for (cols, style) in zip(styles[sheetname].values(), styles[sheetname].keys())]

        return formats

    def _get_charts(limits, ranges, performance, cost_tables, chart_columns, chart_columns_2, cost_key):
        # find cost table to use for bar graph
        cost_keys = list(cost_tables.keys())
        cost_table = cost_tables[cost_key][chart_columns_2].iloc[0:-2] ## -2 = -1 for totals row -1 for last year storage
        header_row_2 = sum(len(cost_tables[c_k]) + 2 for c_k in cost_keys[0:cost_keys.index(cost_key)])

        chart_splitter = 1 / min(l for l in [limits['{}{}'.format(r, v)] for v in ExcelePaint.percent_values for r in ranges] if l is not None) + 1

        chart_colors = [ranges[r] for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds]
        chart_dashes = [ExcelePaint.bounds.get(b, 'solid') for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds]
        chart_weights = [ExcelePaint.weights.get(b, 2.25) for v in ExcelePaint.percent_values for r in ranges for b in ExcelePaint.bounds]

        chart_colors_2 = [ExcelePaint.ranges_2[r] for r in chart_columns_2]

        chart_y_axis = {'name': 'performance',
                        'max': 1.0, 'min': 0,
                        'major_gridlines': {'visible': True, 'line': {'color': '#F4F6F6'}}}
        chart_y2_axis = {'name': 'FRU replacement kW',
                         'max': ceil(cost_table.abs().sum(axis='columns').max() * chart_splitter / 100) * 100,
                         'min': floor(cost_table.where(cost_table < 0).sum(axis='columns').min() / 100) * 100,
                         }
        chart_x_axis = {'name': 'date', 'visible': True, 'major_unit': 1, 'major_unit_type': 'years'}
        chart_x2_axis = {'visible': True}

        chart_constants = [{'name': '{}{}{}'.format(r, v, b),
                            'value': limits['{}{}'.format(r, v)],
                            'color': ExcelePaint.ranges_lite[r],
                            'dash': ExcelePaint.bounds_lite[b],
                            } for r in ranges for v in ExcelePaint.percent_values for b in ExcelePaint.bounds_lite]
        charts = [{'sheetname': ExcelePaint._get_symbol('performance'), 'type': 'line', 'columns': chart_columns,
                   'colors': chart_colors, 'dashes': chart_dashes, 'weights': chart_weights,
                   'chart sheet name': ExcelePaint._get_symbol('graph'), 'constants': chart_constants,
                   'y-axis': chart_y_axis, 'y2-axis': chart_y2_axis, 'x-axis': chart_x_axis, 'x2-axis': chart_x2_axis},
                  {'sheetname': ExcelePaint._get_symbol('costs'), 'type': 'column', 'subtype': 'stacked', 'columns': chart_columns_2,
                   'colors': chart_colors_2,
                   'header row': header_row_2, 'table number': -1, 'totals row': True, 'extra row': True,
                   'chart sheet name': ExcelePaint._get_symbol('graph')},
                  ]

        return charts

    def _get_tabs():
        tabs = {ExcelePaint.tabs[t]['symbol']: ExcelePaint.tabs[t]['color'] for t in ExcelePaint.tabs}
        return tabs

    def _get_symbol(sheetname):
        symbol = ExcelePaint.tabs[sheetname]['symbol']
        return symbol