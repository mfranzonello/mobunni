# collections of inputs to simplify values being passed

from datetime import date

from pandas import DataFrame

# generic collection
class Group:
    def __init__(self):
        self.data = None

    def get_inputs(self):
        inputs = DataFrame(columns=['Input', 'Value'], data=self.data)

        return inputs

# common details across project
class Details(Group):
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
    limits_values = ['PTMO', 'WTMO', 'CTMO', 'Peff', 'Weff', 'Ceff', 'window']
    def __init__(self, **kwargs):
        Group.__init__(self)
        self.length = kwargs.get('length', 1)
        self.target_size = kwargs.get('target_size', 1000)
        self.start_date = kwargs.get('start_date', date(date.today().year, 1, 1))
        self.start_month = kwargs.get('start_month', 0)
        self.non_replace = kwargs.get('non_replace')
        self.limits = {value: kwargs['limits'].get(value) for value in Commitments.limits_values} if 'limits' in kwargs else None
        self.start_ctmo = kwargs.get('start_ctmo', 1.0) ##

        self.number = None
        self.deal = None

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
                     ['downside years', self.non_replace]]

# collection of exisiting and future technology
class Technology(Group):
    def __init__(self, **kwargs):
        Group.__init__(self)
        self.new_servers = kwargs['new_servers']
        self.existing_servers = kwargs['existing_servers']
        self.allowed_fru_models = kwargs.get('allowed_fru_models')

        self.data = [['new server model', self.new_servers['model'] if self.has_new_server_model() else self.new_servers['base']],
                     ['existing server model', self.existing_servers['model'] if self.has_existing_servers() else None]]

    # check if there are existing servers
    def has_existing_servers(self):
        existing = len(self.existing_servers['df']) > 0
        return existing

    # check if new server model is given
    def has_new_server_model(self):
        model_number = len(self.new_servers['model']) > 0
        return model_number

# collection of modeling tweaks
class Tweaks(Group):
    def __init__(self, **kwargs):
        Group.__init__(self)
        self.repair = kwargs.get('repair', False)
        self.junk_level = kwargs.get('junk_level')
        self.best = kwargs.get('best', True)
        self.deploy_months = kwargs.get('deploy_months', 3) ##

        self.data = [['repair threshold', self.repair],
                     ['redeploy level', self.junk_level],
                     ['use best FRU available', self.best]]

# collection of database modeling thresholds
class Thresholds(Group):
    def __init__(self, thresholds):
        self.thresholds = thresholds

        self.data = [['min power degradation when FRUs can be pulled', self.thresholds.get('degraded')],
                     ['min efficiency degradation when FRUs can be repaired', self.thresholds.get('inefficient')],
                     ['min deviation when FRUs can be repaired', self.thresholds.get('deviated')],
                     ['years before end of contract to early deploy', self.thresholds.get('early deploy')],
                     ['years before end of contract cannot deploy', self.thresholds.get('no deploy')],
                     ['early deploy target TMO padding', self.thresholds.get('tmo pad')],
                     ['early deploy target efficiency padding', self.thresholds.get('eff pad')],
                     ['process time for FRU redeployment', self.thresholds.get('deploy months')],
                     ]

    def get_values(self):
        values = self.thresholds
        return values
