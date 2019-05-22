# creating and managing a fleet over time to optimize servicing costs

import pandas
from datetime import date
from dateutil.relativedelta import relativedelta
from properties import Site
from operations import Shop, Fleet

# common details across project
class Details:
    def __init__(self, n_sites, n_runs, n_scenarios, n_phases, wait_time):
        self.n_sites = n_sites
        self.n_runs = n_runs
        self.n_scenarios = n_scenarios
        self.n_phases = n_phases
        self.wait_time = wait_time

    # save common inputs from all scenarios
    def get_inputs(self):
        inputs = pandas.DataFrame(columns=['Input', 'Value'],
                                  data=[['# of sites', self.n_sites],
                                        ['# of MC runs', self.n_runs],
                                        ['# of phases', self.n_phases],
                                        ['wait time between phases', self.wait_time],
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
        self.powers = []
        self.residuals = []
        self.costs = []
        self.transactions = []
                
        self.n_runs = details.n_runs
        self.n_scenarios = details.n_scenarios
        self.n_sites = details.n_sites

        self.n_phases = details.n_phases
        self.wait_time = details.wait_time

        phase_sites = zip([y for x in range(1, details.n_phases+1) for y in [x]*details.n_sites], range(1, details.n_sites*details.n_phases+1))
        self.phases = pandas.DataFrame(phase_sites, columns=['phase','site'])

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
        fleet = Fleet()
        shop = Shop(self.sql_db, self.start_date, junk_level=self.junk_level, best=self.best, allowed_fru_models=self.allowed_fru_models)
        fleet.add_shop(shop)
        return fleet, shop

    # create a site at the beginning of a phase
    def set_up_site(self, fleet, phase, month):
        site_number = len(fleet.sites)
        print('Constructing site {}'.format(site_number+1))

        site = Site(site_number, fleet.shop, self.target_size, self.start_date + relativedelta(months=month),
                    self.contract_length, self.limits, self.repair, self.server_model,
                    max_enclosures=self.max_enclosures, start_month=self.start_month, start_ctmo=1.0, non_replace=self.non_replace,
                    thresholds=self.sql_db.get_thresholds()) ## START_CTMO

        if (phase == 1) and len(self.existing_servers['df']):
            # first phase has exisitng servers
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
            print(last_transaction)

        return decommissioned

    # save results of a simulation
    def append_summaries(self, fleet):
        # store fleet power, efficiency and costs
        self.powers.extend(fleet.summarize_performance())
        self.residuals.append(fleet.summarize_residuals())
        self.costs.append(fleet.summarize_transactions())

        # keep record of transactions
        self.transactions.append(fleet.get_transactions())

    # run simulations for a scenario
    def run_scenario(self):
        print('SCENARIO {}'.format(self.scenario_number+1))
        for run_n in range(self.n_runs):
            print('Simulation {}'.format(run_n+1))

            # create fleet related objects
            fleet, shop = self.set_up_fleet()

            phase = 0

            # run through all contracts
            for month in range(self.contract_length*12 + (self.n_phases-1)*self.wait_time + 1):

                # enter new phase after a specified period
                if (month < self.n_phases*self.wait_time - 1) and (month % self.wait_time == 0):
                    phase += 1
                    print('PHASE {}'.format(phase))

                    # add sites
                    for site_n in range(self.n_sites):
                        site = self.set_up_site(fleet, phase, month)
                        fleet.add_site(site)
                        
                for site in fleet.sites:
                    # check site status and move FRUs as required
                    decommissioned = self.inspect_site(fleet, site)
                    if decommissioned:
                        fleet.remove_site(site)
                   
                # make units in shop deployable
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

        # average the run power
        powers = pandas.concat(self.powers)
        power_gb = powers.merge(self.phases, how='left', on='site').groupby(['phase', 'date'])
        power_summary_mean = power_gb.mean().reset_index()
        power_summary_max = power_gb.max().reset_index()
        power_summary_min = power_gb.min().reset_index()
        power_summary = power_summary_mean.drop('site', axis='columns')

        # average the residual value
        residuals = pandas.concat(self.residuals)
        residual_summary = residuals.merge(self.phases, how='left', on='site').groupby(['phase']).mean().reset_index()
        residual_summary = residual_summary.drop('site', axis='columns')
        
        # average the run costs
        costs = pandas.concat(self.costs)
        cost_summary = costs.merge(self.phases, how='left', on='site').groupby(['year', 'phase', 'action']).mean().reset_index()
        cost_summary = cost_summary.drop('site', axis='columns')
      
        # pull last transaction log
        transaction_sample = self.transactions[-1]

        return inputs, power_summary, residual_summary, cost_summary, transaction_sample