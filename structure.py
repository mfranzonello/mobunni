# project selection and functions to read and write database and Excel data

# built-in imports
from datetime import date
from dateutil.relativedelta import relativedelta
from random import random
from tkinter import Tk, filedialog

# add-in imports
from pandas import DataFrame, Series, Timestamp, read_sql, to_numeric, merge
from numpy import nan
from sqlalchemy import create_engine

# self-defined imports
from urls import URL

# ask user for project selection
class Project:
    '''
    This class emulates an input UI. It searches a repository
    for input files, prompts the user for which file to run,
    and extracts details.
    '''
    folder = r'projects' # input files repository
    start = 'bpm_' # start of input files
    file_types = {'inputs': 'simulation',
                  'data': 'database'}
    middle = {m: '{}'.format(m) for m in file_types} # differentiator of input files
    end = 'xls' # input files extension

    def __init__(self):
        self.name = None
        self.path = None
        self.file_type = None

    # prompt user for project selection
    def ask_project(self):
        '''
        This function uses a dialog box to ask the user for what file to run.
        '''
        print('Asking for project')
        root = Tk()
        root.withdraw()

        identifiers = {i: Project.start + Project.middle[i] for i in Project.middle}

        self.path = filedialog.askopenfilename(initialdir=Project.folder, title='Select scenarios file',
                                               filetypes=[('BPM inputs', '{identifier}*.{extension}*'.format(identifier=identifiers['inputs'],
                                                                                                             extension=Project.end)),
                                                          ('BPM curves', '{identifier}*.{extension}*'.format(identifier=identifiers['data'],
                                                                                                             extension=Project.end)),
                                                          ('all files', '*.*')])
        if not self.path:
            # no project selected, so quit
            print('Simulator canceled!')
            quit()

        else:
            for id in identifiers:
                if identifiers[id] in self.path:
                    index_start = self.path.index(identifiers[id])
                    index_end = index_start + len(identifiers[id])
                    if self.path[index_start:index_end] == identifiers[id]:
                        # valid project
                        self.file_type = Project.file_types[id]
                        break

            if self.file_type is None:
                # invalid project
                print('Invalid project file!')
                self.ask_project()

            else:
                # valid project
                self.name = self.path[self.path.index(identifiers[id]) + len(identifiers[id]):self.path.rindex('.{}'.format(Project.end))].strip()
                print('Selected {}'.format(self.path))    

# read in standard assumptions from SQL
class SQLDB:
    '''
    This class connects with a database containing structural
    values common across all simulation runs. It has several
    functions for specific use cases. It should only be used
    by a minimal number of other classes to prevent too much
    direct touching of the database.
    '''

    ''' READING FROM DATABASE '''

    def __init__(self, structure_db: str):
        print('Getting database from {}'.format(structure_db))
        self.engine = create_engine(URL.get_database(structure_db))
        self.connection = self.engine.connect()

    # earliest historical date there is an energy server and power module available 
    def get_earliest_date(self) -> date:
        sql = 'SELECT initial_date FROM Module ORDER BY initial_date LIMIT 1'
        earliest_date = read_sql(sql, self.connection, parse_dates=['initial_date']).squeeze()
        return earliest_date

    # select a table from the database
    def get_table(self, table: str) -> DataFrame:
        sql = 'SELECT * FROM {}'.format(table)
        table = read_sql(sql, self.connection)
        return table

    # select the thresholds for FRU repairs and redeploys
    def get_thresholds(self) -> Series:
        sql = 'SELECT item, threshold from Threshold'
        thresholds = read_sql(sql, self.connection, index_col='item').squeeze()
        return thresholds

    # select latest energy server model
    def get_latest_server_model(self, install_date: date, target_model: str = None) -> str:
        sql = 'SELECT model FROM Module WHERE initial_date < "{}" ORDER BY rating DESC'.format(install_date)
        server_models = read_sql(sql, self.connection)
        
        if (target_model is not None) and (target_model in server_models['model'].to_list()):
            server_model = target_model
        else:
            server_model = server_models['model'].iloc[0]

        return server_model

    # find the alternative name of an internal server name
    def get_alternative_server_model(self, server_model: str) -> str:
        sql = 'SELECT server FROM Compatibility WHERE server = module'
        names = read_sql(sql, self.connection)['server'].squeeze()

        if server_model in names.to_list():
            # server is correct name
            alternative = server_model
        else:
            # server is alternative name
            sql = 'SELECT module FROM Compatibility WHERE server = module LIMIT 1'
            alternative = read_sql(sql, self.connection).squeeze()
        return alternative

    # select power modules compatible with server model
    def get_compatible_modules(self, server_model: str) -> list:
        sql = 'SELECT module FROM Compatibility WHERE server = "{}"'.format(server_model)
        allowed_modules = read_sql(sql, self.connection).squeeze()
        return allowed_modules

    # select default modules for roadmap
    def get_default_modules(self) -> DataFrame:
        sql = 'SELECT model, mark, model_number FROM Module WHERE mark = model_number'
        default_modules = read_sql(sql, self.connection)
        return default_modules

    # select cost for shop action
    def get_cost(self, action: str, action_date: date, model: str = None, mark: str = None, operating_time: int = None, power: float = None) -> float:
        where_list = ['action = "{}"'.format(action),
                      'date <= "{}"'.format(action_date)]
        max_list = ['date']
        
        if model is not None:
            where_list.append('model = "{}"'.format(model))
        if mark is not None:
            where_list.append('mark = "{}"'.format(mark))
        if operating_time is not None:
            where_list.append('operating_time <= {}'.format(operating_time))
            max_list.append('operating_time')
        if power is not None:
            where_list.append('power <= {}'.format(power))
            max_list.append('power')
       
        wheres = ' AND '.join(where_list)
        selects = ','.join(['cost'] + max_list)
        sql = 'SELECT {} FROM Cost WHERE {}'.format(selects, wheres)

        costs = read_sql(sql, self.connection)

        if len(costs):
            for max_value in max_list:
                costs = costs[costs[max_value] == costs[max_value].max()]

            cost = costs['cost'].iloc[0].squeeze()
        else:
            cost = 0

        return cost

    # select rating of power module
    def get_module_rating(self, model: str, mark: str, model_number: str) -> float:
        sql = 'SELECT rating FROM Module WHERE (model = "{}") and (mark = "{}") and (model_number = "{}")'.format(model, mark, model_number)
        rating = read_sql(sql, self.connection).iloc[0].squeeze()
        return rating

    # select initial efficiency of power module
    def get_module_efficiency(self, model: str, mark: str, model_number: str) -> float:
        sql = 'SELECT pct FROM EfficiencyCurve WHERE (model = "{}") and (mark = "{}") and (model_number = "{}") and (month = 1)'.format(model, mark, model_number)
        efficiency = read_sql(sql, self.connection).iloc[0].squeeze()
        return efficiency

    # select stacks of power module
    def get_module_stacks(self, model: str, mark: str, model_number: str) -> [int]:
        sql = 'SELECT stacks FROM Module WHERE (model = "{}") and (mark = "{}") and (model_number = "{}")'.format(model, mark, model_number)
        stacks = read_sql(sql, self.connection).iloc[0].squeeze().split(',')
        stacks = [int(stack) for stack in stacks]
        return stacks

    # select enclosure compatible with energy server
    def get_enclosure_model_number(self, server_model: str) -> [str, float]:
        sql = 'SELECT model_number, nameplate FROM Enclosure WHERE model = "{}"'.format(server_model)
        model_number, nameplate = read_sql(sql, self.connection).iloc[0, :]
        return model_number, nameplate

    # select default server sizes
    def get_server_nameplates(self, server_model_class: str, target_size: float) -> DataFrame:
        sql = 'SELECT model_number, nameplate, standard FROM Server WHERE nameplate <= {}'.format(target_size)
        server_details = read_sql(sql, self.connection)

        if (server_details['standard'] == 1).any():
            # return standard servers only if target size is available
            server_details = server_details[server_details['standard'] == 1]
        else:
            server_details = server_details.groupby('nameplate').first().reset_index()

        server_nameplates = server_details.drop('standard', axis='columns').sort_values('nameplate', ascending=False)

        return server_nameplates

    # try to find a model that matches a partial number
    def get_guessed_server_model(self, server_model_guess: str, site_size: float) -> str:
        sql = 'SELECT model, model_number, nameplate, standard FROM Server WHERE model_number LIKE "{}%"'.format(server_model_guess)
        server_details = read_sql(sql, self.connection)

        guess = server_details.query('model_number.str.startswith(@server_model_guess)', engine='python')
        guess.loc[:, 'div'] = guess['nameplate'].rdiv(site_size)
        guess.loc[:, 'fit'] = guess['div'].sub(guess['div'].astype(int))
        guess.sort_values(['fit', 'standard'], ascending=[True, False], inplace=True)

        if not guess.empty:
            # value found
            server_model_number = guess['model_number'].iloc[0]
        else:
            # pick randomly
            server_model_number = self.get_table('Server').sample(1)['model_number'].iloc[0]

        return server_model_number

    # select server model based on model number or model class + nameplate
    def get_server_model(self, server_model_number: str = None, server_model_class: str = None, nameplate_needed: float = 0, n_enclosures: int = None) -> DataFrame:
        sql = 'SELECT * FROM Server '
        
        if server_model_number is not None:
            sql += 'WHERE model_number = "{}"'.format(server_model_number)

        else:
            sql += 'WHERE (model = "{}") AND (standard != -1)'.format(server_model_class)

        server_details = read_sql(sql, self.connection)

        if server_model_number is None:
            # need a specific nameplate and enclosure count
            if (nameplate_needed > 0) and (n_enclosures is not None) and \
                len(server_details.query('(nameplate == @nameplate_needed) & (enclosures + plus_one == @n_enclosures)')):
                server_details.query('(nameplate == @nameplate_needed) & (enclosures + plus_one == @n_enclosures)', inplace=True)
                
            # need a specific nameplate that is standard
            elif len(server_details.query('(nameplate == @nameplate_needed) & (standard == 1)')):
                server_details.query('(nameplate == @nameplate_needed) & (standard == 1)', inplace=True)
           
            # need a specific nameplate, no standard specified
            elif len(server_details.query('(nameplate == @nameplate_needed) & (plus_one == 1)')):
                server_details.query('(nameplate == @nameplate_needed) & (plus_one == 1)', inplace=True)

            # need best fit nameplate
            else:
                server_details.query('nameplate <= @nameplate_needed').sort_values(['nameplate', 'plus_one', 'enclosures'], ascending=False)
               
        server_model = server_details.iloc[0]

        return server_model

    # select power modules avaible to create at a date
    def get_buildable_modules(self, install_date: date, server_model: str = None, allowed: DataFrame = None, wait_period: bool = False) -> DataFrame:
        sql = 'SELECT model, mark, model_number, initial_date FROM Module WHERE initial_date <= "{}"'.format(install_date) #avaibility_date
        buildable_modules = read_sql(sql, self.connection, parse_dates=['initial_date'])
        
        if wait_period:
            sql = 'SELECT * FROM Timeline'
            timeline = read_sql(sql, self.connection)
            buildable_modules['availability_year'] = buildable_modules['initial_date'].apply(lambda x: min(timeline['year'].max(), relativedelta(install_date, x).years + 1))
            buildable_modules = buildable_modules.merge(timeline, how='left', left_on='availability_year', right_on='year').query('availability >= @random()').drop(columns=['availability', 'availability_year'])

        buildable_modules.drop(columns=['initial_date'], inplace=True)

        sql = 'SELECT DISTINCT model, mark, model_number FROM PowerCurve'
        power_modules = read_sql(sql, self.connection)
        sql = 'SELECT DISTINCT model, mark, model_number FROM EfficiencyCurve'
        efficiency_modules = read_sql(sql, self.connection)
        
        buildable_modules.query(' & '.join('({x} in @{y}.{x})'.format(x=x, y=y) \
                                for x in ['model', 'mark', 'model_number'] \
                                for y in ['power_modules', 'efficiency_modules']),
                                inplace=True)
            
        filtered = self.get_compatible_modules(server_model)

        if filtered is not None:
            buildable_modules.query('model in @filtered', inplace=True)

        if allowed is not None:
            allowed_modules = merge(buildable_modules, allowed, how='inner')

            # only limit when allowables are buildable
            if not allowed_modules.empty:
                buildable_modules = allowed_modules

        return buildable_modules

    # select power curves for a power module model
    def get_power_curves(self, model: str, mark: str, model_number: str) -> DataFrame:
        rating = self.get_module_rating(model, mark, model_number)
        sql = 'SELECT percentile, month, quarter, kw FROM PowerCurve WHERE \
            (model = "{}") and (mark = "{}") and (model_number = "{}")'.format(model, mark, model_number)

        power_curves_periodic = read_sql(sql, self.connection)
        
        # determine if monthly or quarterly
        quarterly = not power_curves_periodic['quarter'].apply(to_numeric, errors='coerce').dropna().empty
        
        # reshape and interpolate
        power_curves = power_curves_periodic.pivot(index='percentile', columns='quarter' if quarterly else 'month')['kw']
        power_curves.insert(0, 0, rating)

        if quarterly:
            power_curves = power_curves.rename(columns={c: 3*c for c in power_curves.columns})
            power_curves = power_curves.reindex(range(0, int(power_curves.columns[-1])+1), axis='columns').interpolate(axis=1, limit_direction='backward')

        power_curves = self.clean_curve(power_curves.transpose().dropna(how='all'))

        return power_curves

    # select efficiency curves for a power module model
    def get_efficiency_curve(self, model: str, mark: str, model_number: str) -> Series:
        sql = 'SELECT month, pct FROM EfficiencyCurve WHERE \
            (model = "{}") and (mark = "{}") and (model_number = "{}")'.format(model, mark, model_number)
        efficiency_curve = read_sql(sql, self.connection)
        efficiency_curve.index = efficiency_curve.loc[:, 'month']-1

        efficiency_curve = self.clean_curve(efficiency_curve['pct'].dropna(how='all'))

        return efficiency_curve

    # remove items without integer indexes for power and efficiency curves
    def clean_curve(self, curve: DataFrame) -> DataFrame:
        cleaned_curve = curve.reindex([int(i) for i in curve.index if (type(i) in [int, float] and int(i) == float(i))])
        return cleaned_curve

    # select system sizes and full power date of historical distribution
    def get_system_sizes(self) -> [Series, Series]:
        sql = 'SELECT system_size, full_power_date FROM Site'
        systems = read_sql(sql, self.connection, parse_dates=['full_power_date'])
        system_sizes = systems['system_size']
        system_dates = systems['full_power_date']
        return system_sizes, system_dates

    # get contract values
    def get_contract(self, contract_number: str = None) -> DataFrame:
        if contract_number is None:
            sql = 'SELECT DISTINCT number FROM Contract'
            contract_number = read_sql(sql, self.connection).sample(1)['number']

        sql = 'SELECT * FROM Contract WHERE number = "{}"'.format(contract_number)
        contract = read_sql(sql, self.connection).drop_duplicates(subset=['number', 'requirement']).set_index('requirement')['value']

        return contract

    # get line items for cash flow
    def get_line_item(self, item: str, item_date: date, escalator_basis: str = None) -> [float, float]:
        sql = 'SELECT value FROM CashFlow WHERE (lineitem = "{}") AND (date <= "{}") ORDER BY date DESC LIMIT 1'.format(item, item_date)
        start_values = read_sql(sql, self.connection, parse_dates=['date'])

        if escalator_basis is None:
            sql = 'SELECT value FROM CashFlow WHERE (lineitem = "{} escalator") AND (date <= "{}") ORDER BY date DESC LIMIT 1'.format(item, item_date)
            escalators = read_sql(sql, self.connection, parse_dates=['date'])

        else:
            sql = 'SELECT value, date FROM CashFlow WHERE lineitem = "{}" ORDER BY date ASC'.format(escalator_basis)
            escalators = read_sql(sql, self.connection, parse_dates=['date'])
            escalators.loc[:, 'escalator'] = escalators['value'].pct_change()
            escalators = escalators[(escalators['date'] <= Timestamp(item_date))]

        # get last value or return NaN
        start_value = start_values.iloc[0]['value'] if len(start_values) else nan
        escalator = escalators.iloc[-1]['value'] if len(escalators) else nan
        
        return start_value, escalator

    # get sites from APC in database
    def get_apc_sites(self) -> DataFrame:
        sql = 'SELECT customer, id FROM APC'
        sites = read_sql(sql, self.connection)
        return sites

    ''' WRITING TO DATABASE '''

    # take existing data and scale to a new peak and stretch by an additional time period
    def scale_and_stretch(self, base_data: DataFrame, scale_factor: int, stretch_extension: int) -> DataFrame:
        scale = scale_factor is not None
        stretch = stretch_extension is not None

        new_data = base_data.copy()
        periods_old = new_data.index.to_list()
        
        if stretch:
            multiplier = (periods_old[-1] + stretch_extension) / periods_old[-1]
            periods_new = [c * multiplier for c in periods_old]
            new_data = new_data.rename(dict(zip(periods_old, periods_new)))
            periods_final = list(range(periods_old[-1]+stretch_extension + 1))
            new_data = new_data.reindex(sorted(set(periods_final + periods_new))).interpolate(limit_direction='backward').reindex(periods_final)

        if scale:
            new_data = new_data.mul(scale_factor)
                   
        return new_data

    # SQL code for matching on keys
    def where_matches(self, table_name: str, keys: list, pairs: dict) -> str:
        wheres = ' OR '.join('({})'.format(' AND '.join('({}.{} IS "{}")'\
            .format(table_name, k, pairs[k].iloc[i]) for k in keys)) for i in range(len(pairs)))
        return wheres

    # add new data to database
    def write_table(self, table_name: str, keys: list, data: DataFrame = None):
        # remove data from table to be overwritten
        # find key pairs
        pairs = data[keys].drop_duplicates()
        wheres = self.where_matches(table_name, keys, pairs)
        
        if len(wheres):
            sql = 'DELETE FROM "{}" WHERE {}'.format(table_name, wheres)
            self.connection.execute(sql)

        # add new data
        if (data is not None) and (not data.empty):
            data.to_sql(table_name, self.connection, if_exists='append', index=False)
        
        return

    # add new power modules
    def write_modules(self, modules: DataFrame):
        self.write_table('Module', ['model', 'mark', 'model_number'], modules)
        return

    # add new power curves
    def write_power_curves(self, power_curves: DataFrame):
        self.write_table('PowerCurve', ['model', 'mark', 'model_number'], power_curves)
        return

    # add new efficiency curves
    def write_efficiency_curves(self, efficiency_curves: DataFrame):
        self.write_table('EfficiencyCurve', ['model', 'mark', 'model_number'], efficiency_curves)
        return

    # add sites from APC to database
    def write_apc_sites(self, sites: DataFrame):
        self.write_table('APC', ['id'], sites)
        return