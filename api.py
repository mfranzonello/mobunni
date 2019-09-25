# API connections

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pandas import read_json, isna, concat, DataFrame, to_datetime

# connect to internal Bloom API for site, server and power module performance of fleet
class APC:
    endpoint = 'https://tmo-portal.ionamerica.priv:4433' # API location

    # get list of Bloom sites and customer names
    def get_sites():
        url = '{endpoint}/sites'.format(endpoint=APC.endpoint)
        sites = read_json(url)
        return sites

    # get list of Bloom servers and power modules
    def get_servers():
        url = '{endpoint}/energyServers'.format(endpoint=APC.endpoint)
        servers = read_json(url)
        return servers

    def __init__(self):
        self.sites = APC.get_sites()
        self.servers = APC.get_servers()

    # get performance of each power module at a site
    def get_site_performance(self, site_code, start_date=None, end_date=None, tmo_threshold=10):
        site_performance = {}
        if site_code is not None:

            for server_code in self.servers[self.servers['site']==site_code]['id']:
                server_number = server_code.replace(site_code, '')
                site_performance[server_number] = {'nameplate': self.servers[self.servers['id']==server_code]['nameplateKw'].squeeze(),
                                                   'model': self.servers[self.servers['id']==server_code]['type'].squeeze().title(),
                                                   'frus': {}}
            
                for fru_code in self.servers[self.servers['id']==server_code]['powerModules'].iloc[0]:
                    fru_number = fru_code.replace(server_code, '')

                    fru_performance, fru_install_date, fru_operating_time = self.get_fru_performance(fru_code, start_date, end_date, tmo_threshold)

                    site_performance[server_number]['frus'][fru_number] = {'performance': fru_performance,
                                                                           'install date': fru_install_date,
                                                                           'operating time': fru_operating_time}
        
        return site_performance

    # get performance of individual power module and determine start date
    def get_fru_performance(self, fru_code, start_date, end_date, tmo_threshold=10):
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
        fru_operating_time = relativedelta(fru_performance.index[-1], fru_install_date)

        return fru_performance, fru_install_date, fru_operating_time