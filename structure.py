# project selection and functions to read and write database and Excel data

import os

from pandas import DataFrame, Timestamp, read_sql, to_numeric, merge
from numpy import nan
from sqlalchemy import create_engine

from debugging import StopWatch

# ask user for project selection
class Project:
    '''
    This class emulates an input UI. It searches a repository
    for input files, prompts the user for which file to run,
    and extracts details.
    '''
    folder = r'projects' # input files repository
    start = 'bpm_inputs_' # starting name of input files
    end = '.xlsx' # input files extension

    def __init__(self):
        self.projects = [file[len(Project.start):-len(Project.end)] \
            for file in os.listdir(Project.folder) if (file[:len(Project.start)] == Project.start) and (file[-len(Project.end):] == Project.end)]
        self.name = None
        self.path = None

    # prompt user for project selection
    def ask_project(self):
        '''
        '''
        if len(self.projects):
            print('Available projects:')

            while self.name is None:
                count = 0
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
    '''
    This class connects with a database containing structural
    values common across all simulation runs. It has several
    functions for specific use cases. It should only be used
    by a minimal number of other classes to prevent too much
    direct touching of the database.
    '''
    def __init__(self, structure_db: str):
        self.engine = create_engine('sqlite:///{}'.format(structure_db))
        self.connection = self.engine.connect()

    # earliest historical date there is an energy server and power module available 
    def get_earliest_date(self):
        sql = 'SELECT initial_date FROM Module ORDER BY initial_date LIMIT 1'
        earliest_date = read_sql(sql, self.connection, parse_dates=['initial_date']).squeeze()
        return earliest_date

    # select a table from the database
    def get_table(self, table):
        sql = 'SELECT * FROM {}'.format(table)
        table = read_sql(sql, self.connection)
        return table

    # select the thresholds for FRU repairs and redeploys
    def get_thresholds(self):
        sql = 'SELECT * from Threshold'
        thresholds = read_sql(sql, self.connection, index_col='item').squeeze()
        return thresholds

    # select latest energy server model
    def get_latest_server_model(self, install_date, target_model=None):
        sql = 'SELECT model FROM Module WHERE initial_date < "{}" ORDER BY rating DESC'.format(install_date)
        server_models = read_sql(sql, self.connection)
        
        if (target_model is not None) and (target_model in server_models['model'].to_list()):
            server_model = target_model
        else:
            server_model = server_models['model'].iloc[0]

        return server_model

    # select power modules compatible with server model
    def get_compatible_modules(self, server_model):
        sql = 'SELECT module FROM Compatibility WHERE server IS "{}"'.format(server_model)
        allowed_modules = read_sql(sql, self.connection).squeeze()
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

        costs = read_sql(sql, self.connection)

        if len(costs):
            for max_value in max_list:
                costs = costs[costs[max_value] == costs[max_value].max()]

            cost = costs['cost'].iloc[0].squeeze()
        else:
            cost = 0

        return cost

    # select rating of power module
    def get_module_rating(self, model, mark):
        sql = 'SELECT rating FROM Module WHERE model IS "{}" and mark IS "{}"'.format(model, mark)
        rating = read_sql(sql, self.connection).iloc[0].squeeze()
        return rating

    # select module base from model and mark
    def get_module_base(self, model, mark):
        where_list = [('model', 'IS', model),
                      ('mark', 'IS', mark)]
        wheres = ' AND '.join('({} {} "{}")'.format(this, to, that) for (this, to, that) in where_list)

        sql = 'SELECT base FROM Module WHERE {}'.format(wheres)
        base = read_sql(sql, self.connection).iloc[0].squeeze()
        return base

    # select enclosure compatible with energy server
    def get_enclosure_model_number(self, server_model):
        sql = 'SELECT model_number, rating FROM Enclosure WHERE model IS "{}"'.format(server_model)
        model_number, rating = read_sql(sql, self.connection).iloc[0, :]
        return model_number, rating

    # select default server sizes
    def get_server_nameplates(self, server_model_class, target_size):
        sql = 'SELECT model_number, nameplate, standard FROM Server WHERE nameplate <= {}'.format(target_size)# WHERE standard IS 1'
        server_details = read_sql(sql, self.connection)

        if (server_details['standard'] == 1).any():
            # return standard servers only if target size is available
            server_details = server_details[server_details['standard'] == 1]
        else:
            server_details = server_details.groupby('nameplate').first().reset_index()

        server_nameplates = server_details.drop('standard', axis='columns').sort_values('nameplate', ascending=False)

        return server_nameplates

    # select server model based on model number or model class + nameplate
    def get_server_model(self, server_model_number=None, server_model_class=None, nameplate_needed=0, n_enclosures=None):
        sql = 'SELECT * FROM Server '
        
        if server_model_number is not None:
            sql += 'WHERE model_number IS "{}"'.format(server_model_number)

        else:
            sql += 'WHERE (model IS "{}") AND (standard IS NOT -1)'.format(server_model_class)

        server_details = read_sql(sql, self.connection)

        if server_model_number is None:
            # need a specific nameplate and enclosure count
            if (nameplate_needed > 0) and (n_enclosures is not None) and \
                len(server_details[\
                    (server_details['nameplate'] == nameplate_needed) & \
                    (server_details['enclosures'] + server_details['plus_one'] == n_enclosures)]): ## pick standard first?

                server_details = server_details[\
                    (server_details['nameplate'] == nameplate_needed) & \
                    (server_details['enclosures'] + server_details['plus_one'] == n_enclosures)]

            # need a specific nameplate that is standard
            elif len(server_details[\
                    (server_details['nameplate'] == nameplate_needed) & \
                    (server_details['standard'] == 1)]):

                server_details = server_details[\
                    (server_details['nameplate'] == nameplate_needed) & \
                    (server_details['standard'] == 1)]
            
            # need a specific nameplate, no standard specified
            elif len(server_details[\
                    (server_details['nameplate'] == nameplate_needed) & \
                    (server_details['plus_one'] == 1)]):

                server_details = server_details[\
                    (server_details['nameplate'] == nameplate_needed) & \
                    (server_details['plus_one'] == 1)].sort_values('enclosures')

            # need best fit nameplate
            else:
                server_details = server_details[server_details['nameplate'] <= nameplate_needed].sort_values(['nameplate', 'plus_one', 'enclosures'], ascending=False)
               
        server_model = server_details.iloc[0]

        return server_model

    # select power modules avaible to create at a date
    def get_buildable_modules(self, install_date, server_model=None, allowed=None, wait_period=None):
        availability_date = install_date if wait_period is None else install_date + wait_period
        sql = 'SELECT model, mark FROM Module WHERE initial_date <= "{}"'.format(availability_date) ## AND NOT bespoke
        buildable_modules = read_sql(sql, self.connection)

        sql = 'SELECT DISTINCT model, mark FROM PowerCurve'
        power_modules = read_sql(sql, self.connection)
        sql = 'SELECT DISTINCT model, mark FROM EfficiencyCurve'
        efficiency_modules = read_sql(sql, self.connection)
        
        buildable_modules = buildable_modules[\
            buildable_modules['model'].isin(power_modules['model']) &\
            buildable_modules['mark'].isin(power_modules['mark']) &\
            buildable_modules['model'].isin(efficiency_modules['model']) &\
            buildable_modules['mark'].isin(efficiency_modules['mark'])]

        filtered = self.get_compatible_modules(server_model)

        if filtered is not None:
            buildable_modules = buildable_modules[buildable_modules['model'].isin(filtered)]

        if allowed is not None:
            allowed_modules = merge(buildable_modules, allowed, how='inner')

            # only limit when allowables are buildable
            if not allowed_modules.empty:
                buildable_modules = allowed_modules

        return buildable_modules

    # select power curves for a power module model
    def get_power_curves(self, model, mark):
        rating = self.get_module_rating(model, mark)
        sql = 'SELECT percentile, month, quarter, kw FROM PowerCurve WHERE model IS "{}" and mark IS "{}"'.format(model, mark)

        power_curves_periodic = read_sql(sql, self.connection)
        
        # determine if monthly or quarterly
        quarterly = not power_curves_periodic['quarter'].apply(to_numeric, errors='coerce').dropna().empty
        
        # reshape and interpolate
        power_curves = power_curves_periodic.pivot(index='percentile', columns='quarter' if quarterly else 'month')['kw']
        power_curves.insert(0, 0, rating)

        if quarterly:
            power_curves = power_curves.rename(columns={c: 3*c for c in power_curves.columns})
            power_curves = power_curves.reindex(range(0, power_curves.columns[-1]+1), axis='columns').interpolate(axis=1, limit_direction='backward')
        power_curves = power_curves.transpose().dropna(how='all')

        return power_curves

    # select efficiency curves for a power module model
    def get_efficiency_curve(self, model, mark):
        sql = 'SELECT month, pct FROM EfficiencyCurve WHERE (model IS "{}") and (mark IS "{}")'.format(model, mark)
        efficiency_curve = read_sql(sql, self.connection)
        efficiency_curve.index = efficiency_curve.loc[:, 'month']-1
        efficiency_curve = efficiency_curve['pct'].dropna(how='all')

        return efficiency_curve

    # select system sizes and full power date of historical distribution
    def get_system_sizes(self):
        sql = 'SELECT system_size, full_power_date FROM Site'
        systems = read_sql(sql, self.connection, parse_dates=['full_power_date'])
        system_sizes = systems['system_size']
        system_dates = systems['full_power_date']
        return system_sizes, system_dates

    # get line items for cash flow
    def get_line_item(self, item, date, escalator_basis=None):
        sql = 'SELECT value FROM CashFlow WHERE (lineitem IS "{}") AND (date <= "{}") ORDER BY date DESC LIMIT 1'.format(item, date)
        start_values = read_sql(sql, self.connection, parse_dates=['date'])

        if escalator_basis is None:
            sql = 'SELECT value FROM CashFlow WHERE (lineitem IS "{} escalator") AND (date <= "{}") ORDER BY date DESC LIMIT 1'.format(item, date)
            escalators = read_sql(sql, self.connection, parse_dates=['date'])

        else:
            sql = 'SELECT value, date FROM CashFlow WHERE lineitem IS "{}" ORDER BY date ASC'.format(escalator_basis)
            escalators = read_sql(sql, self.connection, parse_dates=['date'])
            escalators.loc[:, 'escalator'] = escalators['value'].pct_change()
            escalators = escalators[(escalators['date'] <= Timestamp(date))]

        # get last value or return NaN
        start_value = start_values.iloc[0]['value'] if len(start_values) else nan
        escalator = escalators.iloc[-1]['value'] if len(escalators) else nan
        
        return start_value, escalator

    # take existing data and scale to a new peak and stretch by an additional time period
    def scale_and_stretch(self, base_data, scale_factor, stretch_extension):
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
    def where_matches(self, table_name, keys, pairs):
        wheres = ' OR '.join('({})'.format(' AND '.join('({}.{} IS "{}")'\
            .format(table_name, k, pairs[k].iloc[i]) for k in keys)) for i in range(len(pairs)))
        return wheres

    # add new data to database
    def write_table(self, table_name, keys, data=None):
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

    # add new power curves
    def write_power_curves(self, power_curves):
        self.write_table('PowerCurve', ['model', 'mark'], power_curves)
        return

    # add new efficiency curves
    def write_efficiency_curves(self, efficiency_curves):
        self.write_table('EfficiencyCurve', ['model', 'mark'], efficiency_curves)
        return

    # copy values from within table
    def duplicate_curve(self, curve_name, changes):  
        for change in changes:
            # get change values
            keys = change['change'].keys()
            scale_factor = change.get('scale')
            stretch_extension = change.get('stretch')

            # pull data from database
            if curve_name == 'power':
                table_name = 'PowerCurve'
                curve = self.get_power_curves(change['change']['model'][0], change['change']['mark'][0])
            elif curve_name == 'efficiency':
                table_name = 'EfficiencyCurve'
                curve = self.get_efficiency_curve(change['change']['model'][0], change['change']['mark'][0])

            # scale and stretch data if required
            if (scale_factor is not None) or (stretch_extension is not None):
                curve = self.scale_and_stretch(curve, scale_factor, stretch_extension)

            # convert series to dataframe
            if curve_name == 'efficiency':
                curve = DataFrame(curve)

            # add period
            period = curve.index.name
            curve.insert(0, period, curve.index + 1)    

            # pivot and add alternate period
            if curve_name == 'power':
                curve = curve.melt(id_vars = period, value_name = 'kw')
                if 'quarter' not in curve.columns:
                    curve.insert(curve.columns.to_list().index('month') + 1, 'quarter', nan)
                elif 'month' not in curve.columns:
                    curve.insert(curve.columns.to_list().index('quarter'), 'month', nan)
                
            # add module descriptors
            curve.insert(0, 'model', change['change']['model'][-1])
            curve.insert(1, 'mark', change['change']['mark'][-1])
            
            # add to database
            self.write_table(table_name, keys, curve)

        return curve

