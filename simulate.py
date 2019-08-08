# creating and managing a fleet over time to optimize servicing costs

from datetime import date
from dateutil.relativedelta import relativedelta

from pandas import concat

from properties import Site
from operations import Shop, Fleet
from legal import Portfolio
from debugging import StopWatch

# details specific to scenario
class Scenario:
    def __init__(self, number, name, commitments, technology, tweaks):
        self.number = number
        self.name = name

        self.commitments = commitments
        self.technology = technology
        self.tweaks = tweaks

        self.windowed = (self.commitments.limits['WTMO'] or self.commitments.limits['Weff']) and self.commitments.limits['window']

    # save specifice inputs from a scenario
    def get_inputs(self, *args):
        inputs = concat([item.get_inputs() for item in [*args, self.commitments, self.technology, self.tweaks]],
                         ignore_index=True)
        
        return inputs

# sites installed over phases and run for full contracts
class Simulation:
    def __init__(self, details, scenario, sql_db, thresholds):
        self.fru_performance = []
        self.site_performance = []
        self.costs = []
        self.transactions = []
        
        self.details = details
        self.scenario = scenario

        self.sql_db = sql_db
        self.thresholds = thresholds.get_values()
        
        self.tweaks = scenario.tweaks

        self.inputs = scenario.get_inputs(details, thresholds)

        self.portfolio = Portfolio()
       
    # create operations and cost objects
    def set_up_fleet(self):
        system_sizes, system_dates = self.sql_db.get_system_sizes()
        min_date = self.sql_db.get_earliest_date()
        fleet = Fleet(self.scenario.commitments.target_size, self.details.n_sites, self.details.n_years,
                      system_sizes, system_dates, self.scenario.commitments.start_date, min_date)

        shop = Shop(self.sql_db, self.thresholds, self.scenario.commitments.start_date, tweaks=self.tweaks,
                    allowed_fru_models=self.scenario.technology.allowed_fru_models)
        fleet.add_shop(shop)

        # adjust start date to account for sites being installed before the target site
        ##print('TARGET MONTH: {}'.format(fleet.target_month))
        self.scenario.commitments.start_date -= relativedelta(months=fleet.target_month)

        return fleet, shop
       
    # create a site at the beginning of a phase
    def set_up_site(self, fleet, month):
        site_number = len(fleet.sites)
        print('Constructing site {}'.format(site_number+1), end='')

        # pick site size according to distribution for all but one specific site
        site_size = fleet.install_sizes[site_number]
        print(' | {}kW'.format(site_size))

        site_start_date = self.scenario.commitments.start_date + relativedelta(months=month)

        ## update
        site_limits = self.scenario.commitments.limits
        site_start_month = self.scenario.commitments.start_month ##
        site_deal = 'CapEx' # 'PPA'
        site_length = self.scenario.commitments.length
        site_non_replace = self.scenario.commitments.non_replace
        site_start_ctmo = self.scenario.commitments.start_ctmo

        # update contract
        contract = self.portfolio.generate_contract(site_deal, site_length, site_size, site_start_date, site_start_month,
                                                    site_non_replace, site_limits, start_ctmo=site_start_ctmo)
        site = Site(site_number, fleet.shop, contract)

        if (site_number == fleet.target_site) and self.scenario.technology.has_existing_servers():
            # target site has exisitng servers
            site.populate(existing_servers=self.scenario.technology.existing_servers)
        else:
            # build site from scratch
            site.populate(new_servers=self.scenario.technology.new_servers)

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
            StopWatch.timer('check site [simulate]')
            site.check_site()
            StopWatch.timer('check site [simulate]')
                                               
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
        self.costs.append(fleet.summarize_transactions())

        # keep record of transactions and FRU performance
        self.fru_performance.append(fleet.get_fru_performance())
        self.transactions.append(fleet.get_transactions())
        
    # run simulations for a scenario
    def run_scenario(self):
        print('SCENARIO {}'.format(self.scenario.number+1))
        for run_n in range(self.details.n_runs):
            print('Simulation {}'.format(run_n+1))

            # create fleet related objects
            fleet, shop = self.set_up_fleet()

            # run through all contracts
            for month in range(self.scenario.commitments.length*12 + fleet.target_month + 1):

                # install site at sampled months
                for site_n in range(fleet.get_install_count(month)):
                    StopWatch.timer('set up site [simulate]')
                    site = self.set_up_site(fleet, month)
                    StopWatch.timer('set up site [simulate]')

                    fleet.add_site(site)

                        
                for site in fleet.sites:
                    # check site status and move FRUs as required
                    ##print('Inspecting site {}'.format(site.number + 1))
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

        # average the run performance
        performance = concat(self.site_performance)
        drops = ['site', 'year']
        if self.scenario.windowed:
            drops.extend(['WTMO', 'Weff'])
        performance_gb = performance.drop(drops, axis='columns').groupby(['date'])
        performance_mean = performance_gb.mean().reset_index()
        performance_max = performance_gb.max().reset_index()
        performance_min = performance_gb.min().reset_index()

        site_performance = performance_mean\
            .merge(performance_max, on='date', suffixes=['', '_max'])\
            .merge(performance_min, on='date', suffixes=['', '_min'])
       
        # average the run costs
        costs = concat(self.costs)
        cost_summary = costs[costs['target']].drop('target', axis='columns').groupby(['year', 'action']).mean().reset_index()
        cost_summary_dollars = cost_summary.pivot(index='year', columns='action', values='service cost').reset_index()
        cost_summary_quants = cost_summary.pivot(index='year', columns='action', values='count').reset_index()
        cost_tables = [cost_summary_dollars, cost_summary_quants]
      
        # pull the last FRU performance
        fru_power_sample = self.fru_performance[-1]['power'].drop('site', axis='columns')
        fru_efficiency_sample = self.fru_performance[-1]['efficiency'].drop('site', axis='columns')

        # pull last transaction log 
        transaction_sample = self.transactions[-1]

        return self.inputs, site_performance, cost_tables, fru_power_sample, fru_efficiency_sample, transaction_sample