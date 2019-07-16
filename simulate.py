# creating and managing a fleet over time to optimize servicing costs 

import pandas
from datetime import date
from dateutil.relativedelta import relativedelta
from properties import Site
from operations import Shop, Fleet

# common details across project
class Details:
    def __init__(self, n_sites, n_years, n_runs, n_scenarios):
        self.n_sites = n_sites
        self.n_years = n_years
        self.n_runs = n_runs
        self.n_scenarios = n_scenarios
        
    # save common inputs from all scenarios
    def get_inputs(self):
        inputs = pandas.DataFrame(columns=['Input', 'Value'],
                                  data=[['# of sites', self.n_sites],
                                        ['# of phase years', self.n_years],
                                        ['# of MC runs', self.n_runs],
                                        ])
        return inputs

# details specific to scenario
class Scenario:
    def __init__(self, number, name, **kwargs):
        self.number = number
        self.name = name
        self.contract_length = kwargs.get('contract_length', 5)
        self.target_size = kwargs.get('target_size', 1000)
        self.start_date = kwargs.get('start_date', date(date.today().year, 1, 1))
        self.limits = kwargs['limits']
        self.server_model = kwargs['server_model']
        self.max_enclosures = kwargs.get('max_enclosures', 6)
        self.plus_one_empty = kwargs.get('plus_one_empty', False)
        self.allowed_fru_models = kwargs.get('allowed_fru_models')
        self.existing_servers = kwargs['existing_servers']
        self.non_replace = kwargs.get('non_replace')
        self.start_month = kwargs.get('start_month', 0)
        self.repair = kwargs.get('repair', False)
        self.junk_level = kwargs.get('junk_level')
        self.best = kwargs.get('best', True)

    # save specifice inputs from a scenario
    def get_inputs(self):
        inputs = pandas.DataFrame(columns=['Input', 'Value'],
                                  data=[['contract length', self.contract_length],
                                        ['contract target size', self.target_size],
                                        ['contract start date', self.start_date],
                                        ['contract months passed', self.start_month],
                                        ['CTMO limit', self.limits['CTMO']],
                                        ['WTMO limit', self.limits['WTMO']],
                                        ['WTMO window', self.limits['window']],
                                        ['PTMO limit', self.limits['PTMO']],
                                        ['server model', self.server_model],
                                        ['server enclosures', self.max_enclosures],
                                        ['plus-one', self.plus_one_empty],
                                        ['existing servers', len(self.existing_servers)>0],
                                        ['downside years', self.non_replace],
                                        ['repair threshold', self.repair],
                                        ['redeploy level', self.junk_level],
                                        ['use best FRU available', self.best],
                                        ])
        return inputs

# sites installed over phases and run for full contracts
class Simulation:
    def __init__(self, details, scenario, sql_db):
        self.fru_performance = []
        self.site_performance = []
        self.residuals = []
        self.costs = []
        self.transactions = []
        
        self.n_runs = details.n_runs
        self.n_scenarios = details.n_scenarios
        self.n_sites = details.n_sites
        self.n_years = details.n_years

        self.sql_db = sql_db
        
        self.scenario_number = scenario.number

        self.contract_length = scenario.contract_length
        self.target_size = scenario.target_size

        self.start_date = scenario.start_date
        self.limits = scenario.limits

        self.server_model = scenario.server_model

        self.max_enclosures = scenario.max_enclosures
        self.plus_one_empty = scenario.plus_one_empty
        self.allowed_fru_models = scenario.allowed_fru_models

        self.existing_servers = scenario.existing_servers

        self.start_month = scenario.start_month
        self.non_replace = scenario.non_replace
        self.repair = scenario.repair
        self.junk_level = scenario.junk_level
        self.best = scenario.best

        self.details_inputs = details.get_inputs()
        self.scenario_inputs = scenario.get_inputs()
        
    # create operations and cost objects
    def set_up_fleet(self):
        system_sizes, system_dates = self.sql_db.get_system_sizes()
        min_date = self.sql_db.get_earliest_date()
        fleet = Fleet(self.target_size, self.n_sites, self.n_years, system_sizes, system_dates, self.start_date, min_date)

        shop = Shop(self.sql_db, self.start_date, junk_level=self.junk_level, best=self.best, allowed_fru_models=self.allowed_fru_models)
        fleet.add_shop(shop)

        # adjust start date to account for sites being installed before the target site
        self.start_date -= relativedelta(months=fleet.target_month)

        return fleet, shop
       
    # create a site at the beginning of a phase
    def set_up_site(self, fleet, month):
        site_number = len(fleet.sites)
        print('Constructing site {}'.format(site_number+1), end='')

        # pick site size according to distribution for all but one specific site
        site_size = fleet.install_sizes[site_number]
        print(' | {}kW'.format(site_size))

        site = Site(site_number, fleet.shop, site_size, self.start_date + relativedelta(months=month),
                    self.contract_length, self.limits, self.repair, self.server_model,
                    max_enclosures=self.max_enclosures, start_month=self.start_month, start_ctmo=1.0, non_replace=self.non_replace,
                    thresholds=self.sql_db.get_thresholds()) ## START_CTMO

        if (site_number == fleet.target_site) and len(self.existing_servers['df']):
            # target site has exisitng servers
            site.populate(self.existing_servers)
        else:
            # build site from scratch
            site.populate({'df': []}, plus_one_empty=self.plus_one_empty)

        return site

    # look at site to see if FRUs need to be repaired, replaced or redeployed or if contract is finished
    def inspect_site(self, fleet, site):
        decommissioned = False

        # check TMO, efficiency, repairs and other site statuses
        transaction_date = site.get_date()

        # return FRUs at end of contract
        if site.is_expired():
            site.decommission()
            decommissioned = True
        else:
            # check TMO, efficiency, repairs and other site statuses
            site.check_site()
                                               
            # degrade FRUs and continue contract
            site.degrade()

        # display what happened
        last_transaction = fleet.get_transactions(site_number=site.number, last_date=transaction_date)
        if len(last_transaction):
            print('MONTHLY SUMMARY')
            print(last_transaction)

        return decommissioned

    # save results of a simulation
    def append_summaries(self, fleet):
        # store fleet power, efficiency and costs
        self.site_performance.append(fleet.summarize_site_performance())
        self.residuals.append(fleet.summarize_residuals())
        self.costs.append(fleet.summarize_transactions())

        # keep record of transactions and FRU performance
        self.fru_performance.append(fleet.get_fru_performance())
        self.transactions.append(fleet.get_transactions())
        
    # run simulations for a scenario
    def run_scenario(self):
        print('SCENARIO {}'.format(self.scenario_number+1))
        for run_n in range(self.n_runs):
            print('Simulation {}'.format(run_n+1))

            # create fleet related objects
            fleet, shop = self.set_up_fleet()

            # run through all contracts
            for month in range(self.contract_length*12 + fleet.target_month + 1): #+ self.n_years ## variable length contracts?

                # install site at sampled months
                for site_n in range(fleet.install_months.get(month, 0)):
                    site = self.set_up_site(fleet, month)
                    fleet.add_site(site)
                        
                for site in fleet.sites:
                    # check site status and move FRUs as required
                    decommissioned = self.inspect_site(fleet, site)
                    if decommissioned:
                        fleet.remove_site(site)
                   
                # make units in shop deployable.phases
                fleet.shop.advance()

            # get value of remaining FRUs
            fleet.shop.salvage_frus()

            # store results
            self.append_summaries(fleet)

    # summarize results of all simulations
    def get_results(self):
        # return results of simulation runs
        print('Consolidating results')

        # collect inputs
        inputs = pandas.concat([self.details_inputs, self.scenario_inputs])

        # average the run performance
        performance = pandas.concat(self.site_performance)
        performance_gb = performance.drop(['site', 'year'], axis='columns').groupby(['date'])
        performance_summary_mean = performance_gb.mean().reset_index()
        performance_summary_max = performance_gb.max().reset_index()
        performance_summary_min = performance_gb.min().reset_index()
        performance_summary = performance_summary_mean

        # average the residual value
        residuals = pandas.concat(self.residuals)
        residual_summary = residuals.mean()
        
        # average the run costs
        costs = pandas.concat(self.costs)
        cost_summary = costs[costs['target']].drop('target', axis='columns').groupby(['year', 'action']).mean().reset_index()
      
        # pull the last FRU performance
        fru_power_sample = self.fru_performance[-1]['power'].drop('site', axis='columns')
        fru_efficiency_sample = self.fru_performance[-1]['efficiency'].drop('site', axis='columns')

        # pull last transaction log 
        transaction_sample = self.transactions[-1]

        return inputs, performance_summary, residual_summary, cost_summary, fru_power_sample, fru_efficiency_sample, transaction_sample