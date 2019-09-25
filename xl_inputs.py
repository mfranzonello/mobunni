# project selection and functions to read and write database and Excel data

import string
import zipfile
import fnmatch
import io
from xml.etree import ElementTree

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
        string1 = 'Filename: {}\n'.format(self.file)
        string2 = 'Tables:\n' + ''.join(' {}: sheet {}, range {}\n'.format(self.tables[t]['name'],
                                                                           self.tables[t]['sheet_name'],
                                                                           self.tables[t]['ref']) for t in self.tables)
        string = string1 + string2
        return string

    def __getitem__(self, location):
        sheet_name, item_name = location

        if sheet_name is None:
            matches = [(sheet, name) for (sheet, name) in self.data if item_name.lower() in name]
        else:
            matches = [(sheet, name) for (sheet, name) in self.data if (sheet == sheet_name.lower()) and (item_name.lower() in name)]
        
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
            df = xl_file.parse(sheet_name=sheet_num, skiprows=skip_r, usecols=parse_c).iloc[0:height]
            dataframes[(self.sheet_names[sheet_num], self.tables[table]['name'].lower())] = df
        self.data = dataframes

    # translate Excel reference to pandas numbers
    def _split_range(self, string):
        left = string[0:string.index(':')]
        right = string[string.index(':')+1:]
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

            if (cell_obj.formula_text != '#REF!') and ('!' in cell_obj.formula_text):
                (sheet_name, ref) = cell_obj.formula_text.split('!')
                (discard, col_str, row_str) = ref.split('$')
                col = 0
                for i in range(len(col_str)):
                    col_add = string.ascii_uppercase.index(col_str)
                    col += col_add * 10**i
                row = int(row_str)-1

                self.data[(sheet_name, range_name.lower())] = workbook.sheet_by_name(sheet_name).cell(row, col).value
        return

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
    def __init__(self, project, details_sheet='details', scenarios_sheet='scenario'):
        self.excel_seer = ExcelSeer(project)
        self.details_sheet = details_sheet
        self.scenarios_sheets = [sh for sh in self.excel_seer.sheet_names if scenarios_sheet in sh]

    # convert excel float to datetime
    def xldate(self, date_float):
        date = xlrd.xldate_as_datetime(date_float, 0).date()
        return date

    # return floats where possible
    def floater(self, string):
        value = self.floatint(string, float)
        return value

    # return integers where possible
    def inter(self, string):
        value = self.floatint(string, int)
        return value

    # return float or interger
    def floatint(self, string, func):
        try:
            return func(string)
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
                'target_size': float, 'start_date': self.xldate, 'contract_length': int, 'contract_start': float, 'nonreplace': str,
                'allow_repairs': bool, 'redeploy_level': self.inter, 'use_best_only': bool, 'allow_early_deploy': bool,
                'new_server_base': str, 'new_server_model': str, 'site_code': str,
                }

        tables = ['AllowedModules']

        values_keys = self.get_sheet_named_ranges(scenario_name, keys)
        values_tables = self.get_sheet_tables(scenario_name, tables)

        ctmo_limit, wtmo_limit, ptmo_limit, ceff_limit, weff_limit, peff_limit, window, \
            target_size, start_date, contract_length, start_month, \
            non_replace_string, repair, junk_level, best, early_deploy, \
            new_server_base, new_server_model, existing_server_model = values_keys

        allowed_fru_models, existing_servers_df = values_tables

        limits = {'CTMO': ctmo_limit, 'WTMO': wtmo_limit, 'PTMO': ptmo_limit,
                  'Ceff': ceff_limit, 'Weff': weff_limit, 'Peff': peff_limit,
                  'window': window}
        non_replace = [int(float(x)) for x in non_replace_string.split(',')] if len(non_replace_string) else []

        new_servers = {'base': new_server_base, 'model': new_server_model}
        
        if allowed_fru_models['model'].dropna().empty:
            allowed_fru_models = None
        
        if repair is None:
            repair = False

        return scenario_name, limits, target_size, start_date, contract_length, start_month, \
            non_replace, repair, junk_level, best, early_deploy, \
            site_code, new_servers, allowed_fru_models

    # total number of scenarios to explore
    def count_scenarios(self):
        n_scenarios = len(self.scenarios_sheets)
        return n_scenarios
