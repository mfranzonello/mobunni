# project selection and functions to read and write database and Excel data

import pandas
import os
import getpass
import xlrd
import string
import zipfile
import fnmatch
import requests
import io
from xml.etree import ElementTree
from sqlalchemy import create_engine

# ask user for project selection
class Project:
    folder = r'projects'
    start = 'bpm_inputs_'
    end = '.xlsx'

    def __init__(self):
        self.projects = [file[len(Project.start):-len(Project.end)] \
            for file in os.listdir(Project.folder) if (file[:len(Project.start)] == Project.start) and (file[-len(Project.end):] == Project.end)]
        self.name = None
        self.path = None

    # prompt user for project selection
    def ask_project(self):
        if len(self.projects):
            print('Available projects:')
            count = 0

            while self.name is None:
                for project in self.projects:
                    count += 1
                    print('[{}]: {}'.format(count, project))
                
                project_num = input('Select project 1-{}: '.format(len(self.projects)))
                if project_num.isdigit() and (int(project_num)-1 in range(len(self.projects))):
                    self.name = self.projects[int(project_num)-1]
                    print('Selected {}'.format(self.name))
                else:
                    print('Not a valid option!')

            self.path = r'{}\{}{}{}'.format(Project.folder, Project.start, self.name, Project.end)

        else:
            print('No projects available!')
            quit()

# read in standard assumptions from SQL
class SQLDB:
    def __init__(self, structure_db):
        self.engine = create_engine('sqlite:///{}'.format(structure_db))
        self.connection = self.engine.connect()

    # select a table from the database
    def get_table(self, table):
        sql = 'SELECT * FROM {}'.format(table)
        table = pandas.read_sql(sql, self.connection)
        return table

    # select the thresholds for FRU repairs and redeploys
    def get_thresholds(self):
        sql = 'SELECT * from Threshold'
        thresholds = pandas.read_sql(sql, self.connection, index_col='item').squeeze()
        return thresholds

    # select power modules compatible with server model
    def get_compatible_modules(self, server_model, allowed=None):
        sql = 'SELECT module FROM Compatibility WHERE server IS "{}"'.format(server_model)
        allowed_modules = pandas.read_sql(sql, self.connection).squeeze()
        return allowed_modules

    # select cost for shop action
    def get_cost(self, action, date, model=None, mark=None, operating_time=None, power=None):
        where_list = ['action IS "{}"'.format(action),
                       'date <= "{}"'.format(date)]
        max_list = ['date']
        
        if model is not None:
            where_list.append('model IS "{}"'.format(model))
        if mark is not None:
            where_list.append('mark IS "{}"'.format(mark))
        if operating_time is not None:
            where_list.append('operating_time <= {}'.format(operating_time))
            max_list.append('operating_time')
        if power is not None:
            where_list.append('power <= {}'.format(power))
            max_list.append('power')
        
        wheres = ' AND '.join(where_list)
        selects = ','.join(['cost'] + max_list)
        sql = 'SELECT {} FROM Cost WHERE {}'.format(selects, wheres)
        costs = pandas.read_sql(sql, self.connection)

        if len(costs):
            for max_value in max_list:
                costs = costs[costs[max_value] == costs[max_value].max()]

            cost = costs['cost'].iloc[0].squeeze()
        else:
            cost = 0

        return cost

    # select rating of power module
    def get_module_rating(self, module_model, module_mark):
        sql = 'SELECT rating FROM Module WHERE module IS "{}" and mark IS "{}"'.format(module_model, module_mark)
        rating = pandas.read_sql(sql, self.connection).iloc[0].squeeze()
        return rating

    # select nameplate of energy server
    def get_server_nameplate(self, server_model):
        sql = 'SELECT nameplate FROM Server WHERE server IS "{}"'.format(server_model)
        nameplate = pandas.read_sql(sql, self.connection).iloc[0].squeeze()
        return nameplate

    # select power modules avaible to create at a date
    def get_buildable_modules(self, install_date, bespoke=False, filtered=None, allowed=None):
        wheres = ' AND NOT bespoke' if not bespoke else ''
        sql = 'SELECT module, mark FROM Module WHERE initial_date <= "{}"'.format(install_date, wheres)
        buildable_modules = pandas.read_sql(sql, self.connection)

        sql = 'SELECT DISTINCT model, mark FROM PowerCurve'
        power_modules = pandas.read_sql(sql, self.connection)
        sql = 'SELECT DISTINCT model, mark FROM EfficiencyCurve'
        efficiency_modules = pandas.read_sql(sql, self.connection)
        
        buildable_modules = buildable_modules[\
            buildable_modules['module'].isin(power_modules['model']) &\
            buildable_modules['mark'].isin(power_modules['mark']) &\
            buildable_modules['module'].isin(efficiency_modules['model']) &\
            buildable_modules['mark'].isin(efficiency_modules['mark'])]

        if filtered is not None:
            buildable_modules = buildable_modules[buildable_modules['module'].isin(filtered)]

        if allowed is not None:
            buildable_modules = buildable_modules[\
                buildable_modules['module'].isin(allowed['model']) &\
                buildable_modules['mark'].isin(allowed['mark'])]

        return buildable_modules

    # select power curves for a power module model
    def get_power_curves(self, module_model, module_mark):
        rating = self.get_module_rating(module_model, module_mark)
        sql = 'SELECT percentile, quarter, kw FROM PowerCurve WHERE model IS "{}" and mark IS "{}"'.format(module_model, module_mark)
        power_curves = pandas.read_sql(sql, self.connection).pivot(index='percentile', columns='quarter')['kw']
        power_curves = power_curves.rename(columns={c: 3*c for c in power_curves.columns})
        power_curves.insert(0, 0, rating)
        power_curves = power_curves.reindex(range(0, power_curves.columns[-1]+1), axis='columns').interpolate(axis=1).transpose()
        return power_curves

    # select efficiency curves for a power module model
    def get_efficiency_curve(self, module_model, module_mark):
        sql = 'SELECT month, kw FROM EfficiencyCurve WHERE model IS "{}" and mark IS "{}"'.format(module_model, module_mark)
        efficiency_curve = pandas.read_sql(sql, self.connection)
        efficiency_curve.index = efficiency_curve.loc[:, 'month']-1
        efficiency_curve = efficiency_curve['kw']
        return efficiency_curve

    # SQL code for matching on keys
    def where_matches(self, table_name, keys, pairs):
        wheres = ' OR '.join('({})'.format(' AND '.join('("{}"."{}" IS "{}")'\
            .format(table_name, k, pairs[k].iloc[i]) for k in keys)) for i in range(len(pairs)))
        return wheres

    # add new data to database
    def write_table(self, table_name, keys, data=None):
        # remove data from table to be overwritten
        # find key pairs
        pairs = data[keys].drop_duplicates()
        wheres = self.where_matches(table_name, keys, pairs)
        
        sql = 'DELETE FROM "{}" WHERE {}'.format(table_name, wheres)
        self.connection.execute(sql)

        # add new data
        if data is not None:
            data.to_sql(table_name, self.connection, if_exists='append', index=False)
        
        return

    # add new power curves
    def write_power_curves(self, power_curves):
        ## check if power curves are already in?
        self.write_table('PowerCurve', ['model', 'mark'], power_curves)
        return

    # add new efficiency curves
    def write_efficiency_curves(self, efficiency_curves):
        self.write_table('EfficiencyCurve', ['model', 'mark'], efficiency_curves)
        return

    # copy values from within table
    def duplicate_values(self, table_name, keys, changes):
        pairs = pandas.DataFrame([c[0] for c in changes], columns=keys)
        wheres = self.where_matches(table_name, keys, pairs)
        copy_data = pandas.read_sql('SELECT * FROM "{}" WHERE {}'.format(table_name, where))
        for value in changes:
            copy_data.loc[pandas.concat([copy_data[k] == v for (k, v) in zip(keys, value[0])], axis=1).all(1), keys] = value[1]

        self.write_table(table_name, keys, copy_data)

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
        xl_file = pandas.ExcelFile(self.file)
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

            if '!' in cell_obj.formula_text:
                (sheet_name, ref) = cell_obj.formula_text.split('!')
                (discard, col_str, row_str) = ref.split('$')
                col = 0
                for i in range(len(col_str)):
                    col_add = string.ascii_uppercase.index(col_str)
                    col += col_add * 10**i
                row = int(row_str)-1

                self.data[(sheet_name, range_name.lower())] = workbook.sheet_by_name(sheet_name).cell(row, col).value
        return

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

        keys = {'n_sites': int, 'n_runs': int, 'n_phases': int, 'wait_time': int}
        values = self.get_sheet_named_ranges(details_name, keys)

        n_sites, n_runs, n_phases, wait_time = values

        return n_sites, n_runs, n_phases, wait_time

    # return specific details of scenario
    def get_scenario(self, scenario=0, col=1):
        scenario_name = self.scenarios_sheets[scenario]
        
        keys = {'ctmo_limit': self.floater, 'wtmo_limit': self.floater, 'ptmo_limit': self.floater,
                'ceff_limit': self.floater, 'weff_limit': self.floater, 'peff_limit': self.floater, 'window': self.inter,
                'target_size': float, 'start_date': self.xldate, 'contract_length': int, 'contract_start': float, 'nonreplace': str,
                'allow_repairs': bool, 'redeploy_level': self.inter, 'use_best_only': bool,
                'new_server_model': str, 'enclosures': self.inter, 'plus_one_empty': bool, 'existing_server_model': str,
                }

        tables = ['AllowedModules', 'ExistingServers']

        values_keys = self.get_sheet_named_ranges(scenario_name, keys)
        values_tables = self.get_sheet_tables(scenario_name, tables)

        ctmo_limit, wtmo_limit, ptmo_limit, ceff_limit, weff_limit, peff_limit, window, \
            target_size, start_date, contract_length, start_month, \
            non_replace_string, repair, junk_level, best, \
            server_model, max_enclosures, plus_one_empty, existing_nameplate = values_keys

        allowed_fru_models, existing_servers_df = values_tables

        limits = {'CTMO': ctmo_limit, 'WTMO': wtmo_limit, 'PTMO': ptmo_limit,
                  'Ceff': ceff_limit, 'Weff': weff_limit, 'Peff': peff_limit,
                  'window': window}
        non_replace = [int(float(x)) for x in non_replace_string.split(',')] if len(non_replace_string) else []

        existing_servers = {'df': existing_servers_df, 'model': existing_nameplate}
        if allowed_fru_models['model'].dropna().empty:
            allowed_fru_models = None
        
        if repair is None:
            repair = False

        return scenario_name, limits, target_size, start_date, contract_length, start_month, \
            non_replace, repair, junk_level, best, \
            server_model, max_enclosures, plus_one_empty, allowed_fru_models, existing_servers

    # total number of scenarios to explore
    def count_scenarios(self):
        n_scenarios = len(self.scenarios_sheets)
        return n_scenarios

# stores dataframe results and prints to multiple tabs of Excel spreadhseet
class Excelerator:
    def __init__(self, path=None, filename='results', extension='xlsx'):
        self.path = self.get_desktop() if path is None else path
        self.filename = filename
        self.extension = extension
        
        self.writer = None

        self.dataframes = {}

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
       
    # add values to an output sheet
    def add_sheet(self, sheetname, dataframe, index=True):
        self.dataframes[sheetname] = {'df': dataframe, 'index': index}
        return

    # add values to output sheets
    def add_sheets(self, sheets, index=True):
        for sheet in sheets:
            self.add_sheet(sheet, sheets[sheet], index=index)
        return

    # print output to Excel file and open
    def to_excel(self, start=False):
        next_file = Excelerator.next_file(self.path, self.filename, self.extension)
        outpath = r'{}\{}.{}'.format(self.path, next_file, self.extension)
        writer = pandas.ExcelWriter(outpath, engine='xlsxwriter')
        for sheetname in self.dataframes:
            self.dataframes[sheetname]['df'].to_excel(writer, sheet_name=sheetname, index=self.dataframes[sheetname]['index'])
        writer.save()

        if start:
            os.startfile(outpath)
        return

# read Excel data into SQL database
class ExcelSQL:
    def __init__(self, path, sql_db):
        self.path = path
        self.sql_db = sql_db
        
        self.excel_seer = ExcelSeer(path)

    # import new curves to database
    def import_curves(self, curve_name):
        table_name = {'power': 'PowerCurve', 'efficiency': 'EfficiencyCurve'}[curve_name]
        period = {'power': 'quarter', 'efficiency': 'month'}[curve_name]

        curves = self.excel_seer[(None, table_name)]
        value_vars = [int(s) for s in curves.columns if (type(s) is str) and (s.isdigit())]
        renames = {str(i): i for i in value_vars}
        curves.rename(columns=renames, inplace=True)
        curves_melted = curves.melt(id_vars=['model', 'mark', 'percentile'],
                                    value_vars=value_vars,
                                    var_name=period, value_name='kw')
        return curves_melted

    # import new power curves to database
    def import_power_curves(self):
        power_curves = self.import_curves('power')
        self.sql_db.write_power_curves(power_curves)

    # import new efficiency curves to database
    def import_efficiency_curves(self):
        efficiency_curves = self.import_curves('efficiency')
        self.sql_db.write_efficiency_curves(efficiency_curves)