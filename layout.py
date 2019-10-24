# API connections

# built-in imports
from datetime import date
from dateutil.relativedelta import relativedelta
from urllib.request import urlopen
from urllib.error import URLError

# add-on imports
from pandas import read_json, isna, concat, DataFrame, Series, to_datetime

# self-defined imports
from urls import URL
from properties import Site
from structure import SQLDB
# generic layout for servers

class ServerLayout:
    '''
    Sites start with variable servers and enclosures.
    If there are servers, then the layout exists.
    '''
    def __init__(self, server_layout:DataFrame):
        self.server_layout = server_layout if ((server_layout is not None) and len(server_layout)) else None

    def __repr__(self) -> str:
        repr_string = '\n'.join(' |'.join(' {}.{}'.format(server_number, fru_number) \
            for fru_number in self.get_enclosure_numbers(server_number)) for server_number in self.get_server_numbers())
        return repr_string

    # layout existence
    def exist(self) -> bool:
        exists = self.server_layout is not None
        return exists

# actual deployed server layout from APC
class ExistingServers(ServerLayout):
    '''
    Servers at existing sites have been running for
    some time and have actual performance.
    '''
    def __init__(self, server_layout):
        ServerLayout.__init__(self, server_layout)

    def __getitem__(self, number) -> DataFrame:
        if type(number) in [str, int]:
            server_number = number
            item = self.server_layout[server_number]
        elif type(number) is tuple:
            server_number, fru_number = number
            item = self.server_layout[server_number]['frus'][fru_number]
        else:
            item = None
        return item

    def get_size(self) -> float:
        if self.exist():
            size = sum([self.server_layout[server]['nameplate'] for server in self.get_server_numbers()])
        else:
           size = 0

        return size

    def get_dates(self) -> [date, int]:
        if self.exist():
            install_date = min([self.server_layout[server]['frus'][fru]['install date'] for server in self.get_server_numbers() \
                                for fru in self.get_enclosure_numbers(server)]).date()
            current_date = max([self.server_layout[server]['frus'][fru]['current date'] for server in self.get_server_numbers() \
                                for fru in self.get_enclosure_numbers(server)]).date()
            
            operating_time = relativedelta(current_date, install_date)
            start_month = operating_time.years * 12 + operating_time.months

        else:
            install_date, start_month = [None]*2

        return install_date, start_month

    def get_models(self) -> [str]:
        if self.exist():
            models = [self.server_layout[server]['model'] for server in self.get_server_numbers()]
            return models

    def get_server_numbers(self) -> [str]:
        if self.exist():
            server_numbers = self.server_layout.keys()
            return server_numbers

    def get_enclosure_numbers(self, server_number:str) -> [str]:
        if self.exist():
            enclosure_numbers = self.server_layout[server_number]['frus'].keys()
            return enclosure_numbers

# user defined server layout
class NewServers(ServerLayout):
    '''
    Servers at theoretical new sites have 
    sequential numbering and no performance yet.
    '''
    def __init__(self, server_layout:DataFrame):
        ServerLayout.__init__(self, server_layout)

    def __getitem__(self, number:int) -> Series:
        item = self.server_layout.query('server_number == @number').iloc[0]
        return item

    def get_size(self) -> float:
        if self.exist():
            size = self.server_layout['nameplate'].sum()
            return size

    def get_server_numbers(self) -> [int]:
        if self.exist():
            server_numbers = self.server_layout['server_number'].to_list()
            return server_numbers

    def get_enclosure_numbers(self, server_number:int, filled_only:bool=True) -> [int]:
        if self.exist():
            to_count = ['filled'] if filled_only else ['filled', 'empty']
            enclosure_numbers = list(range(int(self[server_number][to_count].sum())))
            return enclosure_numbers

    def get_models(self) -> [str]:
        if self.exist():
            models = self.server_layout['model'].to_list()
            return models

class LayoutGenerator:
    def __init__(self, sql_db:SQLDB):
        self.sql_db = sql_db

# connect to internal Bloom API for site, server and power module performance of fleet
class APC(LayoutGenerator):
    url, endpoint = URL.get_apc()

    # check if there is an internet connection
    def check_internet() -> bool:
        try:
            urlopen(APC.url, timeout=5)
            internet = True
        except URLError:
            internet = False

        return internet

    # get Bloom sites, customer names, servers and power modules
    def get_data(keyword:str) -> DataFrame:
        keywords = {'sites': 'sites',
                    'servers': 'energyServers'}
        print('Connecting to APC for {} data'.format(keyword))
        url = '{endpoint}/{key}'.format(endpoint=APC.endpoint, key=keywords[keyword])
        data = read_json(url)
        return data

    def __init__(self, sql_db:SQLDB):
        LayoutGenerator.__init__(self, sql_db)
        self.sites = None
        self.servers = None

    # check what sites are in the database and add new site codes
    def add_to_db(self):
        if APC.check_internet():
            if self.sites is None: self.sites = APC.get_data('sites')

            apc_sites = self.sites.copy()
            db_sites = self.sql_db.get_apc_sites()

            missing_sites = apc_sites[~apc_sites['id'].isin(db_sites['id'])][['id', 'customer', 'acceptance_date']].drop_duplicates(subset='id')
            
            self.sql_db.write_apc_sites(missing_sites)

    # get performance of each power module at a site
    def get_site_performance(self, site_code:str, start_date:date=None, end_date:date=None, tmo_threshold:float=10) -> dict:
        site_performance = {}
        if APC.check_internet() and ((site_code is not None) and len(site_code)):
            if self.sites is None: self.sites = APC.get_data('sites')
            if self.servers is None: self.servers = APC.get_data('servers')

            print('Downloading {} performance from APC'.format(site_code))
            for server_code in self.servers.query('site == @site_code')['id']:
                server_number = server_code.replace(site_code, '')
                server_nameplate = self.servers.query('id == @server_code')['nameplateKw'].squeeze()
                server_model = self.servers.query('id == @server_code')['type'].squeeze().title()

                site_performance[server_number] = {'nameplate': server_nameplate,
                                                   'model': server_model,
                                                   'frus': {}}
            
                for fru_code in self.servers.query('id == @server_code')['powerModules'].iloc[0]:
                    fru_number = fru_code.replace(server_code, '')
                    
                    print(' | {}{}'.format(server_number, fru_number), end='', flush=True)

                    fru_performance, fru_install_date, fru_current_date, fru_operating_time = self.get_fru_performance(fru_code, start_date, end_date, tmo_threshold)

                    site_performance[server_number]['frus'][fru_number] = {'performance': fru_performance,
                                                                           'install date': fru_install_date,
                                                                           'current date': fru_current_date,
                                                                           'operating time': fru_operating_time}
                print()
        
        return site_performance

    # get performance of individual power module and determine start date
    def get_fru_performance(self, fru_code:str, start_date:date=None, end_date:date=None, tmo_threshold:float=10) -> [DataFrame, date, date, relativedelta]:
        start_param = '?start={}'.format(start_date.strftime('%Y-%m-%d')) if start_date is not None else ''
        end_param = '&end={}'.format(end_date.strftime('%Y-%m-%d')) if (start_date is not None) and (end_date is not None) else ''
        params = '{}{}'.format(start_param, end_param)

        fru_values = {}

        for value in ['power', 'eff']:
            url = '{endpoint}/pwm/{value}/{fru_code}.json{params}'.format(endpoint=APC.endpoint,
                                                                            value=value,
                                                                            fru_code=fru_code,
                                                                            params=params)
            fru_value = read_json(url)
            data_col = {'power': 'dc_kw_ave',
                        'eff': 'pwm_eff'}[value]
            div_value = {'power': 1, 'eff': 100}[value]
            renames = {'dc_kw_ave': 'kw',
                        'pwm_eff': 'pct'}
            values = DataFrame(data=fru_value[data_col]).div(div_value).rename(columns=renames)

            values.index = to_datetime(fru_value['ts'])
            values.index.rename(None, inplace=True)

            fru_values[value] = values
                    
        fru_performance = concat(fru_values.values(), axis=1).resample('M').mean()

        # get start date based on increase in TMO from a FRU replacement
        fru_reset = fru_performance.dropna(subset=['kw']).diff().query('kw > @tmo_threshold')
        fru_install_date = fru_performance.index.min() if fru_reset.empty else fru_reset.index.max()
        fru_current_date = fru_performance.index.max()
        fru_operating_time = relativedelta(fru_performance.index[-1], fru_install_date)

        return fru_performance, fru_install_date, fru_current_date, fru_operating_time

class RandomLayout(LayoutGenerator):
    def __init__(self, sql_db:SQLDB):
        LayoutGenerator.__init__(self, sql_db)
        self.columns = ['model', 'model_number', 'nameplate', 'filled', 'empty']
        self.sql_db = sql_db
        self.sites = self.get_sites_from_db()

    def get_sites_from_db(self) -> DataFrame:
        sites = self.sql_db.get_table('Site')
        return sites

    def get_site_layout(self, max_servers:int=10) -> NewServers:
        # check that there are sites left
        if self.sites.empty:
            self.sites = self.get_sites_from_db

        # randomly pick a site
        site = self.sites.sample(1)
        # remove from set
        self.sites.drop(site.index, inplace=True)
       
        # try for model
        site_size = site['system_size'].squeeze()
        model_guess = site['energy_server_model'].squeeze().split(',')[0]    
        model_number = self.sql_db.get_guessed_server_model(model_guess, site_size)
        model = self.sql_db.get_server_model(server_model_number=model_number)

        # fill in servers
        server_count = min(int(site_size / model['nameplate']), max_servers)

        server_layout = DataFrame([[model['model'], model['model_number'], model['nameplate'], model['enclosures'], model['plus_one']]] * server_count,
                                  columns=self.columns)
        server_layout.insert(0, 'server_number', range(1, server_count + 1))

        new_servers = NewServers(server_layout)

        return new_servers