# API connections

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas import read_json, isna, concat, DataFrame, to_datetime

# connect to internal Bloom API for site, server and power module performance of fleet
class APC:
    endpoint = 'https://tmo-portal.ionamerica.priv:4433' # API location

    # get Bloom sites, customer names, servers and power modules
    def get_data(keyword):
        keywords = {'sites': 'sites',
                    'servers': 'energyServers'}
        print('Connecting to APC for {} data'.format(keyword))
        url = '{endpoint}/{key}'.format(endpoint=APC.endpoint, key=keywords[keyword])
        data = read_json(url)
        return data

    def __init__(self):
        self.sites = APC.get_data('sites')
        self.servers = APC.get_data('servers')

    # get performance of each power module at a site
    def get_site_performance(self, site_code, start_date=None, end_date=None, tmo_threshold=10):
        print('Downloading {} performance from APC'.format(site_code))
        site_performance = {}
        if site_code is not None:

            for server_code in self.servers[self.servers['site']==site_code]['id']:
                server_number = server_code.replace(site_code, '')
                site_performance[server_number] = {'nameplate': self.servers[self.servers['id']==server_code]['nameplateKw'].squeeze(),
                                                   'model': self.servers[self.servers['id']==server_code]['type'].squeeze().title(),
                                                   'frus': {}}
            
                for fru_code in self.servers[self.servers['id']==server_code]['powerModules'].iloc[0]:
                    fru_number = fru_code.replace(server_code, '')
                    
                    print(' | {}{}'.format(server_number, fru_number), end='', flush=True)

                    fru_performance, fru_install_date, fru_current_date, fru_operating_time = self.get_fru_performance(fru_code, start_date, end_date, tmo_threshold)

                    site_performance[server_number]['frus'][fru_number] = {'performance': fru_performance,
                                                                           'install date': fru_install_date,
                                                                           'current date': fru_current_date,
                                                                           'operating time': fru_operating_time}
                print()
        
        return site_performance

    def get_existing_servers(self, site_code, start_date=None, end_date=None, tmo_threshold=10):
        site_performance = self.get_site_performance(site_code, start_date=start_date, end_date=end_date, tmo_threshold=tmo_threshold)
        existing_servers = ExistingServers(site_performance)
        return existing_servers

    # get performance of individual power module and determine start date
    def get_fru_performance(self, fru_code, start_date=None, end_date=None, tmo_threshold=10):
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
        fru_reset_date = fru_performance[fru_performance[~fru_performance['kw'].isna()].diff() > tmo_threshold].idxmax()['kw']
        fru_install_date = fru_reset_date if not isna(fru_reset_date) else fru_performance.index.min()
        fru_current_date = fru_performance.index.max()
        fru_operating_time = relativedelta(fru_performance.index[-1], fru_install_date)

        return fru_performance, fru_install_date, fru_current_date, fru_operating_time

class ExistingServers:
    def __init__(self, site_performance):
        self.performance = site_performance if len(site_performance) else None

    def exist(self):
        exists = self.performance is not None
        return exists

    def get_size(self):
        if self.exist():
            size = sum([self.performance[server]['nameplate'] for server in self.performance])
        else:
           size = 0

        return size

    def get_dates(self):
        if self.exist():
            install_date = min([self.performance[server]['frus'][fru]['install date'] for server in self.performance for fru in self.performance[server]['frus']])
            current_date = max([self.performance[server]['frus'][fru]['current date'] for server in self.performance for fru in self.performance[server]['frus']])
            
            operating_time = relativedelta(current_date, install_date)
            start_month = operating_time.years * 12 + operating_time.months

        else:
            install_date, start_month = [None]*2

        return install_date, start_month

    def get_models(self):
        if self.exist():
            models = ' / '.join([self.performance[server]['model'] for server in self.performance])
            return models

    def get_server_numbers(self):
        if self.exist():
            server_numbers = self.performance.keys()
            return server_numbers

    def get_fru_numbers(self, server_number):
        if self.exist():
            fru_numbers = self.performance[server_number]['frus'].keys()
            return fru_numbers

    def __getitem__(self, number):
        if type(number) in [str, int]:
            server_number = number
            item = self.performance[server_number]
        elif type(number) is tuple:
            server_number, fru_number = number
            item = self.performance[server_number]['frus'][fru_number]
        else:
            item = None
        return item