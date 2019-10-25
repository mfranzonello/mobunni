# creating and managing a fleet over time to optimize servicing costs

# built-in imports
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Tuple

# add-on imports
from pandas import DataFrame, concat

# self-defined imports
from groups import Details, Commitments, Technology, Tweaks, Thresholds
from structure import SQLDB
from properties import Site
from operations import Shop, Fleet
from legal import Portfolio
from finances import Cash
from layout import RandomLayout

# details specific to scenario
class Scenario:
    '''
    This class contains the specific details of each scenario,
    made up of contract commitments, server layout, technology roadmaps,
    and tweaks (e.g., allowing deploys or repairs or early deploys).
    '''
    def __init__(self, number: int, name: str, commitments: Commitments = None, technology: Technology = None, tweaks: Tweaks = None):
        self.number = number
        self.name = name

        self.commitments = commitments
        self.technology = technology
        self.tweaks = tweaks

        self.windowed = ((self.commitments.limits['WTMO'] or self.commitments.limits['Weff']) and self.commitments.limits['window']) \
            if self.commitments is not None else None

    def is_runnable(self) -> bool:
        runnable = all([not any(attribute is None for attribute in [self.commitments, self.technology, self.tweaks]),
                        self.technology.has_servers()])
        return runnable

    # save specifice inputs from a scenario
    def get_inputs(self, *args) -> DataFrame:
        inputs = concat([item.get_inputs() for item in [*args, self.commitments, self.technology, self.tweaks]],
                         ignore_index=True)
        
        return inputs

    def get_years(self, cash_flow: bool = False) -> list:
        if cash_flow:
            start, end = self.commitments.get_cash_flow_dates()

        else:
            start = self.commitments.start_date.year
            end = start + self.commitments.length

        years = list(range(start, end + 1))

        return years

# sites installed over phases and run for full contracts
class Simulation:
    '''
    This class runs the details found in the scenario object
    and connects with the SQL database. It runs a specified
    number of monte carlo simulations, and can either run
    a single site or simulate fleet operations. The results
    of the individual runs are store and averaged, with more
    details of the last run saved for auditing.
    '''
    def __init__(self, details: Details, scenario: Scenario, sql_db: SQLDB, thresholds: Thresholds):
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

        self.portfolio = Portfolio(self.sql_db)
        self.cash = Cash(self.sql_db)

        self.size = 0
       
    # create operations and cost objects
    def set_up_fleet(self) -> Tuple[Fleet, Shop]:
        system_sizes, system_dates = self.sql_db.get_system_sizes()
        min_date = self.sql_db.get_earliest_date()
        fleet = Fleet(self.scenario.commitments.target_size, self.details.n_sites, self.details.n_years,
                      system_sizes, system_dates, self.scenario.commitments.start_date, min_date)

        shop = Shop(self.sql_db, self.thresholds, self.scenario.commitments.start_date, self.scenario.commitments.get_downside_years(),
                    self.tweaks, self.scenario.technology)
        fleet.add_shop(shop)

        # adjust start date to account for sites being installed before the target site
        self.scenario.commitments.start_date -= relativedelta(months=fleet.target_month)

        return fleet, shop
       
    # create a site at the beginning of a phase
    def set_up_site(self, fleet: Fleet, month: int, random_layout: RandomLayout) -> Site:
        site_number = len(fleet.sites)
        if site_number == fleet.target_site:
            site_name = '{} (TARGET)'.format(self.scenario.technology.site_name)
        else:
            site_name = 'site {}'.format(site_number+1)
        print('Constructing {}'.format(site_name), end='')

        # pick site size according to distribution for all but one specific site
        site_size = fleet.install_sizes[site_number]

        multiplier = self.scenario.technology.multiplier if site_number == fleet.target_site else 1
        print(' | {:0.1f}kW'.format(site_size * multiplier))

        # update
        site_start_date = self.scenario.commitments.start_date + relativedelta(months=month)

        # set up contract
        if (site_number == fleet.target_site):
            site_deal = self.scenario.commitments.deal
            site_length = self.scenario.commitments.length
            site_limits = self.scenario.commitments.limits
            site_start_month = self.scenario.commitments.start_month
        
        else:
            site_deal, site_length, site_limits = [None]*3
            site_start_month = 0

        contract = self.portfolio.generate_contract(site_size, site_start_date, site_start_month, site_deal, site_length, site_limits)
        site = Site(site_number, fleet.shop, contract)

        if (site_number == fleet.target_site):
            if self.scenario.technology.has_existing_servers():
                # target site has exisitng servers
                site.populate(existing_servers=self.scenario.technology.existing_servers)
            else:
                site.populate(new_servers=self.scenario.technology.new_servers)

            self.size = site.get_system_size()

        else:
            # build site from scratch
            random_servers = random_layout.get_site_layout()
            site.populate(new_servers=random_servers)

        return site

    # look at site to see if FRUs need to be repaired, replaced or redeployed or if contract is finished
    def inspect_site(self, fleet: Fleet, site: Site) -> bool:
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
    def append_summaries(self, fleet: Fleet):
        # store fleet power, efficiency and costs
        self.site_performance.append(fleet.summarize_site_performance())
        self.costs.append(fleet.summarize_transactions())

        # keep record of transactions and FRU performance
        self.fru_performance.append(fleet.get_fru_performance())
        self.transactions.append(fleet.get_transactions())
        
    # run simulations for a scenario
    def run_scenario(self):
        print('SCENARIO {}: {}'.format(self.scenario.number+1, self.scenario.name))

        if not self.scenario.is_runnable():
            print('Site is empty, cannot run scenario!')

        else:
            for run_n in range(self.details.n_runs):
                print('Simulation {}'.format(run_n+1))
                self.run_iteration()

    # run just one
    def run_iteration(self):
        # create fleet related objects
        fleet, shop = self.set_up_fleet()
        random_layout = RandomLayout(self.sql_db)

        # run through all contracts
        for month in range(self.scenario.commitments.length*12 + fleet.target_month + 1):

            # install site at sampled months
            for site_n in range(fleet.get_install_count(month)):
                site = self.set_up_site(fleet, month, random_layout)

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
            
        # print simulation update
        cost_tables = self.get_costs(last=True)
        print('Cost $')
        print(cost_tables['dollars'])
        print('Cost #')
        print(cost_tables['quants'])

    # average the run performance
    def get_site_performance(self) -> DataFrame:
        performance = concat(self.site_performance)
        performance_gb = performance.drop(['site', 'year'], axis='columns').groupby(['date'])

        performances = {'min': performance_gb.min(),
                        '25': performance_gb.quantile(.25),
                        '75': performance_gb.quantile(.75),
                        'max': performance_gb.max()}

        site_performance = performance_gb.mean().reset_index()
        for perf in performances:
            site_performance = site_performance.merge(performances[perf], on='date', suffixes=['', '_{}'.format(perf)])

        return site_performance

    # average the run costs
    def get_costs(self, last: bool = False) -> Dict[str, DataFrame]:
        if last:
            costs = self.costs[-1].copy()
        else:
            costs = concat(self.costs)

        cost_div = len(self.costs) if not last else 1
        cost_summary = costs.query('target == 1').drop('target', axis='columns').groupby(['year', 'action']).sum().div(cost_div).reset_index()

        cost_years = self.scenario.get_years()
        cost_summary_dollars = self.pivot_and_total(cost_summary, 'year', 'action', 'service cost', years=cost_years, yearly=True)
        cost_summary_quants = self.pivot_and_total(cost_summary, 'year', 'action', 'count', years=cost_years, yearly=False)
        cost_summary_power = self.pivot_and_total(cost_summary, 'year', 'action', 'power', years=cost_years, yearly=True)
        cost_summary_power.loc[:, 'stored FRU'] *= -1 # stored kW should be negative
       
        cost_tables = {'dollars': cost_summary_dollars,
                       'quants': cost_summary_quants,
                       'power': cost_summary_power}

        return cost_tables

    # pivot and get cost totals
    def pivot_and_total(self, costs: DataFrame, index: str, columns: str, values: str,
                        years: list = None, year_col: str = 'year', yearly: bool = False) -> DataFrame:
        cost_table = costs.pivot(index=index, columns=columns, values=values)

        non_year_columns = [c for c in cost_table.columns if c != year_col]
        cost_table.loc[:, non_year_columns] = cost_table[non_year_columns].mul(self.scenario.technology.multiplier)

        if yearly:
            cost_table.loc[:, 'total'] = cost_table.sum('columns')
        cost_table.loc['total', :] = cost_table.sum('rows')
        cost_table = cost_table.fillna(0).reset_index()

        if years:
            cost_table = cost_table.set_index(year_col).reindex(years + ['total']).fillna(0).reset_index()

        return cost_table

    # pull the last FRU performance
    def get_fru_performance(self) -> Tuple[DataFrame, DataFrame]:
        fru_power_sample = self.fru_performance[-1]['power'].drop('site', axis='columns')
        fru_efficiency_sample = self.fru_performance[-1]['efficiency'].drop('site', axis='columns')

        return fru_power_sample, fru_efficiency_sample

    # pull last transaction log 
    def get_transactions(self) -> DataFrame:
        transaction_sample = self.transactions[-1]

        return transaction_sample

    def get_cash_flow(self, cost_tables: Dict[str, DataFrame]) -> DataFrame:
        cash_flow = self.cash.generate_cash_flow(cost_tables, self.size, self.scenario.get_years(cash_flow=True))
        return cash_flow

    # summarize results of all simulations
    def get_results(self) -> Tuple[DataFrame]:
        # return results of simulation runs
        print('Consolidating results')

        site_performance = self.get_site_performance()
        cost_tables = self.get_costs()
        fru_power_sample, fru_efficiency_sample = self.get_fru_performance()
        transaction_sample = self.get_transactions()
        cash_flow = self.get_cash_flow(cost_tables)

        print(cash_flow)

        return self.inputs, site_performance, cost_tables, fru_power_sample, fru_efficiency_sample, transaction_sample, cash_flow