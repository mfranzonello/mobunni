# project selection and functions to read and write database and Excel data

# built-in imports
import string
import zipfile
import fnmatch
import io
from xml.etree import ElementTree

# add-on imports
from pandas import ExcelFile, to_numeric
import xlrd
import requests

# object that finds and returns tables by name in Excel
class ExcelSeer:
    def __init__(self,file):
        self.file = file
        self.tables = {}
        self.data = {}

        self.sheet_names = []

        self._find_link()
        self._unzip()
        self._get_table_info()
        self._read_tables()
        self._name_sheets()
        self._get_named_ranges()

    def __repr__(self):
        string_filename = 'Filename: {}\n'.format(self.file)
        string_tables = 'Tables:\n' + ''.join(' {}: sheet "{}", range "{}"\n'.format(self.tables[t]['name'],
                                                                                 self.tables[t]['sheet_name'],
                                                                                 self.tables[t]['ref']) for t in self.tables)
        string_named_ranges = 'Named ranges:\n' + ''.join(' {}: sheet "{}", value {}\n'.format(r_n, s_n, self.data[(s_n, r_n)]) for (s_n, r_n) in self.data)
        repr_string = string_filename + string_tables + string_named_ranges
        return repr_string

    def __getitem__(self, location):
        sheet_name, item_name = location

        if sheet_name is None:
            matches = [(sheet, name) for (sheet, name) in self.data if item_name.lower() in name.lower()]
        else:
            matches = [(sheet, name) for (sheet, name) in self.data if (sheet.lower() == sheet_name.lower()) and (item_name.lower() in name.lower())]
        
        value = self.data[matches[0]] if len(matches) else None
        return value

    # return openable table
    def _find_link(self):
        if fnmatch.fnmatch(self.file, 'http*:*'):
            r = requests.get(self.file, stream=True)
            self.link = io.BytesIO(r.content)
        else:
            self.link = self.file

    # pull from local file or web and read as zip file
    def _unzip(self):
        self.xl = zipfile.ZipFile(self.link)

    # get all files in zip of Excel file 
    def _get_table_info(self):
        namelist = self.xl.namelist()
        # get all tables
        table_list = fnmatch.filter(namelist, 'xl/tables/*.xml')
        # get all sheet relationships
        sheet_list = fnmatch.filter(namelist, 'xl/worksheets/_rels/*.xml.rels')
        # set up matching
        tables = {}
        for table in table_list:
            root = ElementTree.parse(self.xl.open(table)).getroot()
            # assign name and range to table id
            tables[self._strip_table(table, 'xml')] = {attribute: root.get(attribute) for attribute in ['name', 'ref']}
        for sheet in sheet_list:
            # get all relationships and keep sheet info
            relationships = ElementTree.parse(self.xl.open(sheet)).findall('*')
            for relationship in relationships:
                items = relationship.items()
                found = False
                for item in items:
                    for i in item:
                        if fnmatch.fnmatch(i, '*table*.xml'):
                            found = True
                            tables[self._strip_table(i, 'rels')]['sheet'] = self._strip_sheet(sheet)
                            break
                    if found:
                        break
        self.tables = tables

    # get table number from XML data
    def _strip_table(self, table_name, source):
        if source=='xml':
            strip_part = 'xl'
        elif source=='rels':
            strip_part = '..'
        stripped = int(table_name.replace('{}/tables/table'.format(strip_part), '').replace('.xml', ''))
        return stripped

    # get sheet number from XML data (0-indexed)
    def _strip_sheet(self, sheet_name):
        stripped = int(sheet_name.replace('xl/worksheets/_rels/sheet', '').replace('.xml.rels', ''))-1
        return stripped

    # read each named table to a dictionary of dataframes
    def _read_tables(self):
        xl_file = ExcelFile(self.file)
        self.sheet_names = xl_file.sheet_names

        dataframes = {}
        for table in self.tables:
            xl_range = self.tables[table]['ref']
            sheet_num = self.tables[table]['sheet']
            parse_c, skip_r, height = self._split_range(xl_range)

            # read in data
            df = xl_file.parse(sheet_name=sheet_num, skiprows=skip_r, usecols=parse_c).iloc[0:height]
            
            # rename mangled dupe column names
            df.columns = xl_file.parse(sheet_name=sheet_num, skiprows=skip_r, usecols=parse_c, header=None).iloc[0].values

            dataframes[(self.sheet_names[sheet_num], self.tables[table]['name'].lower())] = df
        self.data = dataframes

    # translate Excel reference to pandas numbers
    def _split_range(self, string_value):
        left = string_value[0:string_value.index(':')]
        right = string_value[string_value.index(':')+1:]
        letters = []
        numbers = []
        for side in [left, right]:
            letter = ''.join(s for s in side if not s.isdigit())
            number = int(''.join(s for s in side if s.isdigit()))
            letters += [letter]
            numbers += [number]
        parse_c = '{}:{}'.format(letters[0], letters[1])
        skip_r = numbers[0]-1
        height = numbers[1]-numbers[0]
        return parse_c, skip_r, height

    # give Excel name to sheets
    def _name_sheets(self):
        for table in self.tables:
            sheetnum = self.tables[table]['sheet']
            self.tables[table]['sheet_name'] = sheetnum

    # find all named range values (single cell)
    def _get_named_ranges(self):
        workbook = xlrd.open_workbook(self.file)

        for range_name, sheet_num in workbook.name_and_scope_map:
            cell_obj = workbook.name_and_scope_map[(range_name, sheet_num)]

            if ('#REF!' not in cell_obj.formula_text) and ('!' in cell_obj.formula_text):
                sheet_name, ref = cell_obj.formula_text.split('!')
                
                if (sheet_name[0] == "'") and (sheet_name[-1] == "'"):
                        sheet_name = sheet_name[1:-1]

                anchors = ref.split(':')
                _, col_str_1, row_str_1 = anchors[0].split('$')
                _, col_str_2, row_str_2 = anchors[-1].split('$')
                row_range = range(self.get_xl_row(row_str_1), self.get_xl_row(row_str_2) + 1)
                col_range = range(self.get_xl_col(col_str_1), self.get_xl_col(col_str_2) + 1)

                if (len(row_range) == 1) and (len(col_range) == 1):
                    # only one cell
                    values = workbook.sheet_by_name(sheet_name).cell(row_range[0], col_range[0]).value

                elif len(row_range) == 1:
                    # vertical array
                    values = [self.get_xl_value(workbook, sheet_name, row_range[0], col) for col in col_range]

                elif len(col_range) == 1:
                    # horizontal array
                    values = [self.get_xl_value(workbook, sheet_name, row, col_range[0]) for row in row_range]

                else:
                    # rectangular area
                    values = [[self.get_xl_value(workbook, sheet_name, row, col) for row in row_range] for col in col_range]
                
                self.data[(sheet_name, range_name.lower())] = values

        return

    def get_xl_value(self, workbook, sheet_name:str, row:int, col:int) -> str:
        value = workbook.sheet_by_name(sheet_name).cell(row, col).value
        return value

    def get_xl_row(self, row_str:str) -> int:
        row = int(row_str) - 1
        return row

    def get_xl_col(self, col_str:str) -> int:
        alphabet_length = len(string.ascii_uppercase)
        col = sum(alphabet_length**(len(col_str) - i - 1) * (string.ascii_uppercase.index(col_str[i]) + 1) for i in range(len(col_str))) - 1
        return col

# read Excel data into SQL database
class ExcelSQL:
    def __init__(self, path, sql_db):
        self.path = path
        self.sql_db = sql_db
        
        self.excel_seer = ExcelSeer(path)

    # import new curves to database
    def import_curves(self, curve_name, period):
        table_name = {'power': {'quarter': 'PowerCurveQ', 'month': 'PowerCurveM'},
                      'efficiency': {'month': 'EfficiencyCurve'}}[curve_name][period]

        curves = self.excel_seer[(None, table_name)]

        # remove values without model and mark
        if curves is not None:
            value_name = {'power': 'kw',
                          'efficiency': 'pct'}[curve_name]

            curves = curves.dropna(how='any', subset=['model', 'mark'])

            value_vars = [int(s) for s in curves.columns if (type(s) is str) and (s.isdigit())]
            renames = {str(i): i for i in value_vars}
            curves.rename(columns=renames, inplace=True)
            curves_melted = curves.melt(id_vars=['model', 'mark', 'percentile'],
                                        value_vars=value_vars,
                                        var_name=period, value_name=value_name)
            
            curves_melted.loc[:, value_name] = curves_melted[value_name].apply(to_numeric, errors='coerce')
            curves_melted.drop_duplicates(inplace=True)

        else:
            curves_melted = None

        return curves_melted

    # import new power curves to database
    def import_power_curves(self):
        for period in ['month', 'quarter']:
            power_curves = self.import_curves('power', period)
            if power_curves is not None:
                self.sql_db.write_power_curves(power_curves)
        return

    # import new efficiency curves to database
    def import_efficiency_curves(self):
        for period in ['month']:
            efficiency_curves = self.import_curves('efficiency', period)
            if efficiency_curves is not None:
                self.sql_db.write_efficiency_curves(efficiency_curves)
        return

    # import all new curves to database
    def import_all_curves(self):
        self.import_power_curves()
        self.import_efficiency_curves()

# read in Excel file for project inputs
class ExcelInt:
    def __init__(self, project, details_sheet='⚙', scenarios_sheet='📃'):
        self.excel_seer = ExcelSeer(project)
        self.details_sheet = details_sheet
        self.scenarios_sheets = [sh for sh in self.excel_seer.sheet_names \
                                 if (sh[:len(scenarios_sheet)] == scenarios_sheet) and (sh != scenarios_sheet)]

    # convert excel float to datetime
    def xldate(self, date_float):
        if type(date_float) is float:
            date = xlrd.xldate_as_datetime(date_float, 0).date()
        else:
            date = None
        return date

    # return floats where possible
    def floater(self, string_value):
        value = self.floatint(string_value, float)
        return value

    # return integers where possible
    def inter(self, string_value):
        value = self.floatint(string_value, int)
        return value

    # return float or interger
    def floatint(self, string_value, func):
        try:
            return func(string_value)
        except ValueError:
            return None
        return

    # get named range values on sheet
    def get_sheet_named_ranges(self, sheet_name, keys):
        values = [keys[key](self.excel_seer[(sheet_name, key)]) for key in keys]
        return values

    # get table values on sheet 
    def get_sheet_tables(self, sheet_name, tables):
        values = [self.excel_seer[(sheet_name, table)] for table in tables]
        return values

    # get common details of project
    def get_details(self, col=1):
        details_name = self.details_sheet

        keys = {'n_sites': int, 'n_years': int, 'n_runs': int}
        values = self.get_sheet_named_ranges(details_name, keys)

        n_sites, n_years, n_runs = values

        return n_sites, n_years, n_runs

    # return specific details of scenario
    def get_scenario(self, scenario=0, col=1):
        scenario_name = self.scenarios_sheets[scenario]
        
        keys = {'ctmo_limit': self.floater, 'wtmo_limit': self.floater, 'ptmo_limit': self.floater,
                'ceff_limit': self.floater, 'weff_limit': self.floater, 'peff_limit': self.floater, 'window': self.inter,
                'start_date': self.xldate, 'contract_length': int, 'contract_deal': str,
                'site_code': str, 'multiplier': self.floater,
                'allow_repairs': bool, 'allow_redeploy': bool, 'use_best_only': bool,
                'allow_early_deploy': bool, 'eoc_replacements': bool,
                }

        tables = ['NonReplace', 'NewServers', 'Roadmap']

        values_keys = self.get_sheet_named_ranges(scenario_name, keys)
        values_tables = self.get_sheet_tables(scenario_name, tables)

        [ctmo_limit, wtmo_limit, ptmo_limit, ceff_limit, weff_limit, peff_limit, window,
         start_date, contract_length, contract_deal,
         site_code, multiplier,
         repair, redeploy, best, early_deploy, eoc_deploy,
         ] = values_keys

        [non_replace, servers, roadmap] = values_tables

        # get rid of spaces in column names
        for vt in [non_replace, servers, roadmap]:
            vt.rename(columns={c: c.replace(' ', '_') for c in vt.columns}, inplace=True)

        limits = {'CTMO': ctmo_limit, 'WTMO': wtmo_limit, 'PTMO': ptmo_limit,
                  'Ceff': ceff_limit, 'Weff': weff_limit, 'Peff': peff_limit,
                  'window': window}
        
        # set non_replace to empty dataframe if blank
        non_replace.dropna(inplace=True)
        
        # drop totals row and empty rows from servers
        servers = servers.iloc[:-1].dropna(subset=['model', 'model_number'])

        # set tech roadmap to default if blank
        roadmap.rename
        if roadmap['model'].dropna().empty:
            roadmap = None
        
        # set repair to default if blank
        if repair is None:
            repair = False

        return scenario_name, limits, start_date, contract_length, contract_deal, non_replace, \
            site_code, servers, roadmap, multiplier, \
            repair, redeploy, best, early_deploy, eoc_deploy

    # total number of scenarios to explore
    def count_scenarios(self):
        n_scenarios = len(self.scenarios_sheets)
        return n_scenarios
