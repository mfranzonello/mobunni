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
class Contract(Group):
    limits_values = ['PTMO', 'WTMO', 'CTMO', 'Peff', 'Weff', 'Ceff', 'window']
    def __init__(self, **kwargs):
        Group.__init__(self)
        self.length = kwargs.get('length', 1)
        self.target_size = kwargs.get('target_size', 1000)
        self.start_date = kwargs.get('start_date', date(date.today().year, 1, 1))
        self.start_month = kwargs.get('start_month', 0)
        self.non_replace = kwargs.get('non_replace')
        self.limits = {value: kwargs.get(value) for value in Contract.limits_values}
        self.start_ctmo = kwargs.get('start_ctmo', 1.0) ##

        self.data = self.set_data()

    # change the terms of the contract
    def change_terms(self, **kwargs):
        contract = Contract(length=kwargs.get('length', self.length),
                            target_size=kwargs.get('length', self.target_size),
                            start_date=kwargs.get('length', self.start_date),
                            start_month=kwargs.get('start_month', self.start_month),
                            non_replace=kwargs.get('non_replace', self.non_replace),
                            limits={value: kwargs.get(value, self.limits[value]) for value in Contract.limits_values},
                            start_ctmo=self.start_ctmo)

        return contract

    # FRUs can be installed during given year of contract
    def is_replaceable_year(self, year):
        downside = (self.non_replace is None) or (len(self.non_replace) == 0) or \
            not (self.non_replace[0] <= year <= self.non_replace[-1])

        return downside

    def set_data(self):
        self.data = [['contract length', self.length],
                     ['contract target size', self.target_size],
                     ['contract start date', self.start_date],
                     ['contract months passed', self.start_month],
                     ['CTMO limit', self.limits['CTMO']],
                     ['WTMO limit', self.limits['WTMO']],
                     ['WTMO window', self.limits['window']],
                     ['PTMO limit', self.limits['PTMO']],
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