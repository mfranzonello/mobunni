# central warehouse for creating, storing and deploying components and fleet to manage all sites

import time

import pandas
from dateutil.relativedelta import relativedelta
from datetime import date
from random import randrange
from powerful import PowerCurves, PowerModules
from components import FRU, Enclosure, Server

# warehouse to store, repair and deploy old FRUs and create new FRUs
class Shop:
    def __init__(self, sql_db, install_date, junk_level=20, best=False,
                 allowed_fru_models=None):
        self.sql_db = sql_db
        self.power_modules = PowerModules(sql_db)

        self.junk_level = junk_level
        self.deploy_months = 3
        self.best = best

        self.storage = []
        self.deployable = []
        self.junk = []
        self.salvage = []
        self.residuals = pandas.Series()

        self.ghosts = {} # templates of FRUs to copy

        self.date = install_date

        self.allowed_fru_models = allowed_fru_models

        self.next_serial = {'ES': 0, 'PWM': 0, 'ENC': 0}

        self.transactions = pandas.DataFrame(columns=['date', 'serial', 'model', 'mark', 'power', 'efficiency', 'action',
                                                      'direction', 'site', 'server', 'enclosure', 'service cost'])

    # record log of transactions
    def transact(self, serial, model, mark, power, efficiency,
                 action, direction, site_number, server_number, enclosure_number, cost):
        self.transactions.loc[len(self.transactions), :] = [self.date, serial, model, mark, power, efficiency,
                                                            action, direction, site_number+1, server_number+1, enclosure_number+1, cost]

    # return serial number for component tracking
    def get_serial(self, component):
        self.next_serial[component] += 1
        serial = '{}{}'.format(component, str(self.next_serial[component]).zfill(6))
        return serial

    # get cost for action
    def get_cost(self, action, model=None, mark=None, operating_time=None, power=None):
        cost = self.sql_db.get_cost(action, self.date, model, mark, operating_time, power)
        return cost

    # create a new FRU or replicate an existing FRU
    def ghost_fru(self, model, mark, fit=None):
        serial = '_' # blank serial
        install_date = date(1985, 4, 25) ## install date should be initially available date

        power_curves = PowerCurves(self.sql_db.get_power_curves(model, mark))
        efficiency_curve = self.sql_db.get_efficiency_curve(model, mark)

        base = mark ##
        fru = FRU(serial, model, base, mark, power_curves, efficiency_curve, install_date, current_date=install_date, fit=fit)

        # add FRU template
        self.ghosts[(model, mark)] = fru

    # copy a FRU from a template
    def create_fru(self, model, mark, install_date, site_number, server_number, enclosure_number, initial=False, fit=None):
        serial = self.get_serial('PWM')
        
        # check if template already created to reduce calls to DB
        if (model, mark) not in self.ghosts:
            self.ghost_fru(model, mark, fit=None)
            
        fru = self.ghosts[(model, mark)].copy(serial, install_date, current_date=install_date, fit=fit)
        
        # get costs
        cost_action = 'initialize fru' if initial else 'create fru'
        cost = self.get_cost(cost_action, model=model, mark=mark)

        # record costs
        transact_action = 'intialized PWM' if initial else 'created FRU'
        self.transact(serial, model, mark, fru.get_power(), fru.get_efficiency(),
                      transact_action, 'to', site_number, server_number, enclosure_number, cost)

        return fru

    # take a FRU from a site and add to storage queue
    def store_fru(self, fru, site_number, server_number, enclosure_number, repair=False, final=False):
        if (self.junk_level is None) or (fru.get_power() < self.junk_level) or (fru.get_expected_life() <= self.deploy_months):
            # if FRU has no value, junk it
            self.junk.append(fru)

            cost = self.get_cost('junk fru')

            self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                          'junked FRU', 'from', site_number, server_number, enclosure_number, cost)

        else:
            # FRU could be deployed in future
            self.storage.append(fru)

            cost = self.get_cost('store fru') if not final else 0 ## decommissioning doesn't count as service

            self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                          'stored FRU', 'from', site_number, server_number, enclosure_number, cost)

            # repairing FRU moves power curve up
            if repair:
                self.storage[-1].repair()

                cost = self.get_cost('repair fru', model=fru.model, mark=fru.mark, operating_time=fru.month, power=fru.get_power())

                self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                              'repaired FRU', 'from', site_number, server_number, enclosure_number, cost)

        if final:
            # FRU is finished with site and has residual value
            if site_number not in self.residuals:
                self.residuals.loc[site_number] = 0
            self.residuals.loc[site_number] += fru.get_power()

        return

    # take a FRU out of storage to send to site    
    def deploy_fru(self, queue, site_number, server_number, enclosure_number):
        fru = self.deployable.pop(queue)
        
        cost = self.get_cost('deploy fru')

        self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                      'deployed FRU', 'to', site_number, server_number, enclosure_number, cost)

        return fru

    # get value for FRUs leftover after a contract expires and use for redeploys
    def salvage_frus(self):
        for fru in self.storage:
            cost = self.get_cost('salvage fru', fru.model, fru.mark, operating_time=fru.month, power=fru.get_power())

            self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(), 'salvaged FRU',
                          '', 0, 0, 0, cost)
            self.salvage.append(fru)
        self.storage = []

    # move FRUs between energy servers at a site
    def balance_frus(self, fru, site_number, server1, enclosure1, server2, enclosure2):
        cost = self.get_cost('balance fru')

        self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                      'pulled FRU', 'from', site_number, server1, enclosure1, cost/2)

        self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                     'moved FRU', 'to', site_number, server2, enclosure2, cost/2)

    # overhaul a FRU to make it refurbished and bespoke
    ## NOT IMPLEMENTED
    def overhaul_fru(self, queue, mark, site_number, server_number, enclosure_number):
        fru = self.junk.pop(queue)
        power_curves = PowerCurves(self.sql_db.get_power_curves(model, mark))
        efficiency_curve = self.sql_db.get_efficiency_curve(model, mark)

        fru.overhaul(mark, power_curves, efficiency_curve)
        cost = self.get_cost('overhaul fru')
        self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                        'overhauled FRU', 'to', site_number, server_number, enclosure_number, cost)
        return fru

    # find FRU in deployable or junk that best fits requirements
    def find_fru(self, allowed_models, power_needed=0, energy_needed=0, time_needed=0, max_power=None, junked=False):
        powers = self.list_powers(allowed_models, junked=junked) - power_needed
        energies = self.list_energies(allowed_models, time_needed, junked=junked) - energy_needed
        found = \
            (powers.where(powers > 0) if power_needed > 0 else 1) * \
            (powers.where(powers < (max_power - power_needed)) if max_power is not None else 1) * \
            (energies.where(energies > 0)/time_needed if energy_needed > 0 else 1)
        queue = found.idxmin() if len(found) else pandas.np.nan

        return queue

    # flatten a list of lists
    def flatten_list(self, list_of_lists):
        flattened_list = [x for y in list_of_lists for x in y]
        return flattened_list

    # get power value of each fru in storage
    def list_powers(self, allowed_models, junked=False):
        if junked:
            powers_list = self.flatten_list([self.power_modules.get_ratings(fru.model, self.date) \
                if fru.model in allowed_models.to_list() else [0] for fru in self.junk])
        else:
            powers_list = [fru.get_power() if fru.model in allowed_models.to_list() else 0 \
                for fru in self.deployable]

        powers = pandas.Series(powers_list)
        return powers

    # get energy value of each fru in storage
    def list_energies(self, allowed_models, time_needed, junked=False):
        if junked:
            energies_list = self.flatten_list([self.power_modules.get_energiesf(fru.model, self.date, time_needed) \
                if fru.model in allowed_models.to_list() else [0] for fru in self.junk])
        else:
            energies_list = [fru.get_energy(months=time_needed) if fru.model in allowed_models.to_list() else 0 \
                for fru in self.deployable]

        energies = pandas.Series(energies_list)
        return energies

    # return queue of queues
    def get_queues(self, junked=False):
        if junked:
            queues = []
            for i in range(len(self.junk)):
                queues.extend([i] * len(self.power_modules.get_marks(self.junk[i].base)))
        else:
            queues = list(range(len(self.deployable)))
        return queues

    # use a stored FRU or create a new one for power and energy requirements
    def best_fit_fru(self, server_model, install_date, site_number, server_number, enclosure_number,
                     power_needed=0, energy_needed=0, time_needed=0, max_power=0, initial=False):
        allowed_modules = self.sql_db.get_compatible_modules(server_model)
        
        junked = {'deployable': False} ##, 'junked': True}
        queues = {}
        for location in junked:
            powers = self.list_powers(allowed_modules, junked[location])
            energies = self.list_energies(allowed_modules, time_needed, junked[location])
            queues[location] = self.find_fru(allowed_modules, junked=junked[location],
                                             power_needed=power_needed, energy_needed=energy_needed, time_needed=time_needed,
                                             max_power=max_power if not False else None) #self.allow_ceiling_loss
        
        if (not initial) and len(self.deployable) and (not pandas.isna(queues['deployable'])):
            # there is a FRU available to deploy
            queue = queues['deployable']
            fru = self.deploy_fru(queue, site_number, server_number, enclosure_number)

        ##elif (not initial) and len(self.junk) and (not pandas.isna(queues['deployable'])):
        ##    # there is a FRU available to overhaul
        ##    queue = self.list_queues(junked=True)[queues['junked']]
        ##    fru = self.overhaul_fru(queue, mark, site_number, server_number, enclosure_nunber)

        else:
            # there is not a FRU available, so create a new one
            if self.best:
                #tick = time.clock()
                model, mark = self.power_modules.get_model(install_date,
                                                           power_needed=power_needed, energy_needed=energy_needed, time_needed=time_needed,
                                                           best=self.best, server_model=server_model, allowed_fru_models=self.allowed_fru_models)
                #tock = time.clock()
                #print('Time to get_model [best fit FRU]: {:0.3f}s'.format(tock-tick))
            else:
                model, mark = self.power_modules.get_model(install_date,
                                                           power_needed=power_needed, energy_needed=energy_needed, time_needed=time_needed,
                                                           bespoke=not initial, server_model=server_model, allowed_fru_models=self.allowed_fru_models)
            fru = self.create_fru(model, mark, install_date, site_number, server_number, enclosure_number, initial)

        return fru

    # get base model of a server model number
    def get_server_model(self, server_model_number):
        server_model = self.sql_db.get_server_model(server_model_number)['model']
        return server_model

    # get model types to most closely match target size
    def prepare_servers(self, new_servers, target_size):
        # check if server model number is given
        if len(new_servers['model']):
            # get values from database
            server_model_number = new_servers['model']
            server_model = self.sql_db.get_server_model(new_servers['model'])
            server_count = int(target_size / server_model['nameplate'])

        else:
            # check if server model class is available in given year, else use next best
            latest_server_model_class = self.sql_db.get_latest_server_model(self.date, target_model=new_servers['model'])
        
            # get default nameplate sizes   
            server_nameplates = self.sql_db.get_server_nameplates(latest_server_model_class)

            # determine max number of various size servers could fit
            server_nameplates.loc[:, 'fit'] = (target_size / server_nameplates['nameplate']).astype(int)
            # find lost potential because of server sizes
            server_nameplates.loc[:, 'loss'] = target_size - (server_nameplates['nameplate'] * server_nameplates['fit'])
            # return best fit model to minimize potential loss
            server_model_number, server_count = server_nameplates.sort_values('loss')[['model_number', 'fit']].iloc[0]

        return server_model_number, server_count


    # create a new energy server
    def create_server(self, site_number, server_number, server_model_number=None, server_model_class=None, nameplate_needed=0):
        # get cost to create a server
        cost = self.get_cost('install server')

        serial = self.get_serial('ES')

        server_model = self.sql_db.get_server_model(server_model_number, server_model_class, nameplate_needed)
        
        server = Server(serial, server_number, server_model['model'], server_model['model_number'], server_model['nameplate'])

        self.transact(server.serial, server.model, 'SERVER', server.nameplate, 0,
                      'installed ES', 'at', site_number, server.number, -1, cost)

        enclosures = self.create_enclosures(site_number, server, enclosure_count=server_model['enclosures'], plus_one_count=server_model['plus_one'])     

        for enclosure in enclosures:
            server.add_enclosure(enclosure)

        return server

    # upgrade server inverter to a higher capactiy ## NOT USED YET
    def upgrade_server(self, server, site_number, nameplate):
        server.upgrade_nameplate(nameplate)
        cost = self.get_cost('upgrade server', model=server.model, power=nameplate - server.nameplate)
        self.transact(server.serial, server.model, 'SERVER', nameplate, 0,
                     'upgraded ES', 'at', site_number, server.number, -1, cost)
        pass
    
    # return nameplate rating of server model
    def get_server_nameplate(self, server_model):
        nameplate = self.sql_db.get_server_nameplate(server_model)
        return nameplate

    # create an enclosure cabinent to add to a server to house a FRU
    def create_enclosures(self, site_number, server, start_number=0, enclosure_count=0, plus_one_count=0):
        # get cost per enclosure
        costs = enclosure_count * [self.get_cost('intialize enclosure')] + plus_one_count * [self.get_cost('add enclosure')] 
       
        # create enclosures
        enclosures = []
        for c in range(enclosure_count + plus_one_count):
            serial = self.get_serial('ENC')
            enclosure = Enclosure(serial, start_number + c)
            enclosures.append(enclosure)
            self.transact(serial, server.model, 'ENCLOSURE', 0, 0,
                          'add enclosure', 'at', site_number, server.number, enclosure.number, costs[c])

        return enclosures

    # all FRUs in storage become deployable after one period
    def advance(self):
        for fru in self.storage:
            # FRU storage moves power curve forward
            fru.store(self.deploy_months)

            # check if storage killed FRU
            if fru.is_dead():
                self.junk.append(fru)
               
                self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                              'junked FRU', '', -1, -1, -1, 0)

            else:
                self.deployable.append(fru)

        self.storage = []
        self.date += relativedelta(months=1)

# collection of sites and a shop to move FRUs between
class Fleet:
    def __init__(self, target_size, total_sites, install_years, system_sizes, system_dates, start_date, min_date):
        self.total_sites = total_sites
        self.install_years = install_years
        
        self.target_size = target_size
        self.target_site = 0
        self.target_month = 0
        self.install_sizes = []
        self.install_months = None
        self.install_months_count = None

        self.sites = []
        self.shop = None
        self.site_performance = {}
        self.fru_performance = {}

        self.system_sizes = system_sizes
        self.system_dates = system_dates

        self.set_up_install_sizes()
        self.set_up_install_months()
        self.set_up_target_site(start_date, min_date)

    # pick sizes based on distribution for sites other than target
    def set_up_install_sizes(self):
        self.install_sizes = self.system_sizes.sample(self.total_sites - 1, replace=True).to_list()
        
    # pick months based on distribution for site installation
    def set_up_install_months(self):
        # max time to install
        population_dates = self.system_dates[self.system_dates >= self.system_dates.max() - relativedelta(years=self.install_years)]
        sampled_dates = population_dates.sample(self.total_sites, replace=self.total_sites >= len(population_dates))
        self.install_months = (sampled_dates - sampled_dates.min()).dt.days.div(30).round().astype(int).sort_values()
        self.install_months_count = self.install_months.value_counts(sort=False)

    # pick target install sequence order
    def set_up_target_site(self, start_date, min_date):
        delta = relativedelta(start_date, min_date)
        max_month = delta.years*12 + delta.months

        # pick a spot for the target site where the initial install date works
        self.target_site = randrange(len(self.install_months[self.install_months <= max_month]))
        self.target_month = self.install_months.iloc[self.target_site]
        self.install_sizes.insert(self.target_site, self.target_size)

    # return number of sites to install in a given month
    def get_install_count(self, month):
        count = self.install_months_count.get(month, 0)
        return count
        
    # add shop to fleet
    def add_shop(self, shop):
        self.shop = shop

    # add site to fleet    
    def add_site(self, site):
        self.sites.append(site)

    # remove site from fleet
    def remove_site(self, site):
        self.sites = [s for s in self.sites if s is not site]
        # record power and efficiency
        self.store_site_performance(site)
        self.store_fru_performance(site)

    # store site power and efficiency
    def store_site_performance(self, site):
        self.site_performance[site.number] = site.performance

    # store FRU power and efficiency
    def store_fru_performance(self, site):
        power = site.power.copy()
        power.insert(0, 'site', site.number)
        efficiency = site.efficiency.copy()
        efficiency.insert(0, 'site', site.number)
        self.fru_performance[site.number] = {'power': power, 'efficiency': efficiency}

    # get transaction log
    def get_transactions(self, site_number=None, last_date=None):
        if self.shop is not None:
            transactions = self.shop.transactions.copy()

            if site_number is not None:
                transactions = transactions[\
                    (transactions['site'] == site_number+1) & \
                    (transactions['date'] == last_date)]

            return transactions
        return

    # return series of value if site is target site
    def identify_target(self, sites, site_col='site', site_adder=True, site_number='target', target_col='target'):
        if site_number == 'target':
            site_number = self.target_site
        

        target_series = sites[site_col] == (site_number + site_adder)
        target_identified = pandas.concat([sites, target_series.rename(target_col)], axis='columns')

        return target_identified

    # combine transactions by year, site and action
    def summarize_transactions(self, site_number='target'):
        if site_number == 'target':
            site_number = self.target_site

        transactions_yearly = self.get_transactions()
        transactions_yearly.insert(0, 'year', pandas.to_datetime(transactions_yearly['date']).dt.year)
        transactions_gb = transactions_yearly[['year', 'site', 'action', 'service cost']].groupby(['year', 'site', 'action'])
        
        transactions_sum = transactions_gb.sum()[['service cost']]
        transactions_count = transactions_gb.count()[['service cost']].rename(columns={'service cost': 'count'})
        
        transactions_summarized = pandas.concat([transactions_count, transactions_sum], axis='columns').reset_index()

        transactions = self.identify_target(transactions_summarized)

        return transactions

    # return power and efficiency of all sites
    def summarize_site_performance(self, site_number='target'):
        if site_number == 'target':
            site_number = self.target_site

        for site in self.sites:
            # add TMO and eff of any site that hasn't expired
            self.store_site_performance(site)

        site_performance = self.site_performance[site_number]

        return site_performance

    def get_fru_performance(self, site_number='target'):
        if site_number == 'target':
            site_number = self.target_site

        for site in self.sites:
            # add FRU perfromance of any site that hasn't expired
            self.store_fru_performance(site)
       
        fru_performance = self.fru_performance[site_number]

        return fru_performance

    # return residual value
    def summarize_residuals(self, site_number='target'):
        if site_number == 'target':
            site_number = self.target_site

        if self.shop is not None:
            residuals = pandas.DataFrame(data=self.shop.residuals[self.shop.residuals.index==site_number], columns=['residual'])
            return residuals