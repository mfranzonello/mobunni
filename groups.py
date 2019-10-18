# collections of inputs to simplify values being passed

from datetime import date

from pandas import DataFrame

# generic collection
class Group:
    '''
    A group is a generic collection of inputs,
    either from a user interface or a backing
    database. These control the specifications
    of each scenario and the simulator.
    It also contains a function to list all
    inputs in a dataframe for auditing outputs.
    '''
    def __init__(self):
        self.data = None

    def get_inputs(self):
        inputs = DataFrame(columns=['input', 'value'], data=self.data)

        return inputs

# common details across project
class Details(Group):
    '''
    These details specify how many sites to
    simulate (one site or a whole fleet), how many
    years to simulated a fleet and how many
    monte carlo simulations to run.
    '''
    def __init__(self, n_sites, n_years, n_runs, n_scenarios):
        Group.__init__(self)
        self.n_sites = n_sites
        self.n_years = n_years
        self.n_runs = n_runs
        self.n_scenarios = n_scenarios

        self.data=([['# of sites', self.n_sites],
                    ['# of phase years', self.n_years],
                    ['# of MC runs', self.n_runs]])
        
# collection of customer commitments
class Commitments(Group):
    '''
    These are the monthly power and efficiency
    outputs that must be maintained through
    the life of the contract, as well as the
    contract start date and "downside" years
    (a time when FRUs cannot be replaced because
    of manufacturing limitations).
    '''
    limits_values = ['PTMO', 'WTMO', 'CTMO', 'Peff', 'Weff', 'Ceff', 'window']
    def __init__(self, **kwargs):
        Group.__init__(self)
        self.length = kwargs.get('length', 1)
        self.target_size = kwargs.get('target_size', 1000)
        self.start_date = kwargs.get('start_date', date(date.today().year, 1, 1))
        self.start_month = kwargs.get('start_month', 0)
        self.non_replace = kwargs.get('non_replace')
        self.limits = {value: kwargs['limits'].get(value) for value in Commitments.limits_values} if 'limits' in kwargs else None

        self.number = None
        self.deal = None

        # return years of non-replacement
        non_replace_years = ' / '.join(' to '.join('Y'+str(self.non_replace[j][i]) for j in ['start', 'end']) for i in self.non_replace.index)

        self.data = [['contract length', self.length],
                     ['contract target size', self.target_size],
                     ['contract start date', self.start_date],
                     ['contract months passed', self.start_month],
                     ['cumulative TMO limit', self.limits['CTMO']],
                     ['windowed TMO limit', self.limits['WTMO']],
                     ['periodic TMO limit', self.limits['PTMO']],
                     ['cumulative efficiency limit', self.limits['Ceff']],
                     ['windowed efficiency limit', self.limits['Weff']],
                     ['periodic efficiency limit', self.limits['Peff']],
                     ['window', self.limits['window'] if self.limits['WTMO'] or self.limits['Weff'] else None],
                     ['downside years', non_replace_years if len(non_replace_years) else None]]

# collection of exisiting and future technology
class Technology(Group):
    '''
    These define which servers exists or should
    exist at a target site, as well as what 
    power modules can be used in the future.
    '''
    def __init__(self, **kwargs):
        Group.__init__(self)
        self.new_servers = kwargs['new_servers']
        self.existing_servers = kwargs['existing_servers']
        self.roadmap = kwargs.get('roadmap')
        self.site_code = kwargs['site_code']
        self.site_name = self.site_code if len(self.site_code) else '< NEW SITE >'

        if self.has_existing_servers():
            model_string = 'existing'
            models = self.existing_servers.get_models()

        elif self.has_new_servers():
            model_string = 'new'
            models = self.new_servers.get_models()

        else:
            model_string = []

        self.data = [['site code', self.site_name],
                     ['{} server models'.format(model_string), ' / '.join(models)]]

    # check if there are existing servers
    def has_existing_servers(self):
        existing = self.existing_servers.exist()
        return existing

    # check if new server model is given
    def has_new_servers(self):
        existing = self.new_servers.exist()
        return existing

    # check if there are any servers
    def has_servers(self):
        existing = self.has_existing_servers() or self.has_new_servers()
        return existing

    # check for default roadmap if none given
    def get_roadmap(self, sql_db=None):
        if self.roadmap is not None:
            roadmap = self.roadmap
        elif sql_db is not None:
            roadmap = sql_db.get_default_modules()
        else:
            roadmap = None

        return roadmap

# collection of modeling tweaks
class Tweaks(Group):
    '''
    These are tweaks around whether FRUs
    can be repaired or redeployed, if older
    FRU models or bespoke models can be used,
    and whether to install a FRU ahead of
    schedule to minimize FRU residuals at the
    end of a contract.
    These are specified per scenario.
    '''
    def __init__(self, **kwargs):
        Group.__init__(self)
        self.repair = kwargs.get('repair', False)
        self.junk_level = kwargs.get('junk_level')
        self.best = kwargs.get('best', True)
        self.early_deploy = kwargs.get('early_deploy')

        self.data = [['repair threshold', self.repair],
                     ['redeploy level', self.junk_level],
                     ['use best FRU available', self.best],
                     ['allow early deploy', self.early_deploy]]

# collection of database modeling thresholds
class Thresholds(Group):
    '''
    These are system wide limits dictating when FRUs
    can be pulled, repaired or refurbished.
    They are set in the database and should not
    change per scenario.
    '''
    def __init__(self, thresholds):
        self.thresholds = thresholds

        self.data = [['min power degradation when FRUs can be pulled', self.thresholds.get('degraded')],
                     ['min efficiency degradation when FRUs can be repaired', self.thresholds.get('inefficient')],
                     ['min deviation when FRUs can be repaired', self.thresholds.get('deviated')],
                     ['years before end of contract cannot deploy', self.thresholds.get('no deploy')],
                     ['early deploy target TMO padding', self.thresholds.get('tmo pad')],
                     ['early deploy target efficiency padding', self.thresholds.get('eff pad')],
                     ['process time for FRU redeployment', self.thresholds.get('deploy months')],
                     ['power module as FRU availability wait period', self.thresholds.get('fru availability')],
                     ['refurbished FRU availability wait period', self.thresholds.get('refurb availability')],
                     ]

    def get_values(self):
        values = self.thresholds
        return values
