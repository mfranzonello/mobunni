# central warehouse for creating, storing and deploying components and fleet to manage all sites

from dateutil.relativedelta import relativedelta
from datetime import date
from random import randrange

from pandas import DataFrame, Series, concat, to_datetime, isna
from numpy import nan

from powerful import PowerModules, HotBoxes, EnergyServers
from components import FRU, Enclosure, Server
from finances import Bank
from debugging import StopWatch

# record of transactions and results across shop and fleet
class LogBook:
    def __init__(self):
        self.transactions = DataFrame(columns=['date', 'serial', 'model', 'mark', 'power', 'efficiency', 'action',
                                               'direction', 'site', 'server', 'enclosure', 'service cost', 'reason'])
        self.performance = {'site': {}, 'fru': {}}

    def number(self, value):
        try:
            if int(value) == value:
                num = int(value) + 1
            else:
                num = value
        except TypeError:
            num = value
        
        return num

    # record log of transactions
    def record_transaction(self, date, serial, model, mark, power, efficiency,
                           action, direction, site_number, server_number, enclosure_number, cost, reason=None):
        self.transactions.loc[len(self.transactions), :] = [date, serial, model, mark, power, efficiency,
                                                            action, direction,
                                                            self.number(site_number), self.number(server_number), self.number(enclosure_number),
                                                            cost, reason]

    # store power and efficiency
    def record_performance(self, table, site_number, *args):
        if table == 'site':
            [performance] = args
            self.performance['site'][site_number] = performance

        elif table == 'fru':
            power, efficiency = args

            power.insert(0, 'site', site_number)
            efficiency.insert(0, 'site', site_number)
            self.performance['fru'][site_number] = {'power': power, 'efficiency': efficiency}

    # combine transactions by year, site and action
    def get_transactions(self, site_number=None, last_date=None):
        transactions = self.transactions.copy()

        if site_number is not None:
            filter = (transactions['site'] == site_number+1)

            if last_date is not None:
                filter &= (transactions['date'] == last_date)
            transactions = transactions[filter]

        return transactions

    def get_performance(self, table, site_number):
        performance = self.performance[table][site_number]
        return performance

    # return series of value if site is target site
    def identify_target(self, sites, site_number, site_col='site', site_adder=True, target_col='target'):
        target_series = sites[site_col] == (site_number + site_adder)
        target_identified = concat([sites, target_series.rename(target_col)], axis='columns')

        return target_identified

    # combine transactions by year, site and action
    def summarize_transactions(self, site_number):
        transactions_yearly = self.get_transactions()
        transactions_yearly.insert(0, 'year', to_datetime(transactions_yearly['date']).dt.year)
        transactions_gb = transactions_yearly[['year', 'site', 'action', 'service cost']].groupby(['year', 'site', 'action'])
        
        transactions_sum = transactions_gb.sum()[['service cost']]
        transactions_count = transactions_gb.count()[['service cost']].rename(columns={'service cost': 'count'})
        
        transactions_summarized = concat([transactions_count, transactions_sum], axis='columns').reset_index()

        transactions = self.identify_target(transactions_summarized, site_number)

        return transactions

# template for new modules and servers
class Templates:
    def __init__(self, power_modules):
        self.power_modules = power_modules
        self.servers = {}
        self.modules = {}

    def find_server(self, model):
        server = None
        return server

    def ghost_server(self, model):
        pass

    def find_module(self, model, mark):
        if (model, mark) in self.modules:
            module = self.modules[(model, mark)]
        else:
            module = self.ghost_module(model, mark)
        return module

    def ghost_module(self, model, mark):
        serial = None # blank serial
        install_date = None # blank date

        power_curves, efficiency_curves = self.power_modules.get_curves(model, mark)

        base = self.power_modules.get_module_base(model, mark)
        fru = FRU(serial, model, base, mark, power_curves, efficiency_curves, install_date, current_date=install_date)

        # add FRU template
        self.modules[(model, mark)] = fru

        return fru

# warehouse to store, repair and deploy old FRUs and create new FRUs
class Shop:
    def __init__(self, sql_db, thresholds, install_date, tweaks,
                 allowed_fru_models=None):
        self.power_modules = PowerModules(sql_db)
        self.hot_boxes = HotBoxes(sql_db)
        self.energy_servers = EnergyServers(sql_db)
        self.bank = Bank(sql_db)

        self.templates = Templates(self.power_modules)
        self.log_book = LogBook()

        self.thresholds = thresholds

        self.junk_level = tweaks.junk_level
        self.best = tweaks.best
        self.repair = tweaks.repair
        self.early_deploy = tweaks.early_deploy

        self.storage = []
        self.deployable = []
        self.junk = []
        self.salvage = []

        self.date = install_date

        self.allowed_fru_models = allowed_fru_models

        self.next_serial = {'ES': 0, 'PWM': 0, 'ENC': 0}

    # record log of transactions
    def transact(self, serial, model, mark, power, efficiency,
                 action, direction, site_number, server_number, enclosure_number, cost, reason=None):
        self.log_book.record_transaction(self.date, serial, model, mark, power, efficiency,
                                         action, direction, site_number, server_number, enclosure_number, cost, reason)

    # return serial number for component tracking
    def get_serial(self, component):
        self.next_serial[component] += 1
        serial = '{}{}'.format(component, str(self.next_serial[component]).zfill(6))
        return serial

    # get cost for action
    def get_cost(self, action, component=None, **kwargs):
        cost = self.bank.get_cost(self.date, action, component, **kwargs)
        return cost

    # copy a FRU from a template
    def create_fru(self, model, mark, install_date, site_number, server_number, enclosure_number, initial=False, current_date=None, fit=None, reason=None):
        serial = self.get_serial('PWM')
        
        # check if template already created to reduce calls to DB
        fru = self.templates.find_module(model, mark).copy(serial, install_date, current_date=current_date if current_date is not None else install_date, fit=fit)

        # get costs
        if initial:
            cost = self.get_cost('initialize fru', fru)
        else:
            cost = self.get_cost('create fru', fru)

        # record costs
        transact_action = 'intialized PWM' if initial else 'created FRU'
        self.transact(serial, model, mark, fru.get_power(), fru.get_efficiency(),
                      transact_action, 'to', site_number, server_number, enclosure_number, cost, reason=reason)

        return fru

    # take a FRU from a site and add to storage queue
    def store_fru(self, fru, site_number, server_number, enclosure_number, repair=False, final=False, reason=None):
        self.storage.append(fru)

        cost = self.get_cost('store fru') if not final else 0 # decommissioning doesn't count as service

        self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                      'stored FRU', 'from', site_number, server_number, enclosure_number, cost, reason=reason)

        # repairing FRU moves power curve up
        if repair:
            self.storage[-1].repair()

            cost = self.get_cost('repair fru', fru, operating_time=fru.get_month(), power=fru.get_power())

            self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                          'repaired FRU', 'from', site_number, server_number, enclosure_number, cost, reason='deviated FRU')

        return

    # take a FRU out of storage to send to site    
    def deploy_fru(self, queue, site_number, server_number, enclosure_number, reason=None):
        fru = self.deployable.pop(queue)
        
        cost = self.get_cost('deploy fru')

        self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                      'deployed FRU', 'to', site_number, server_number, enclosure_number, cost, reason=reason)

        return fru

    # get value for FRUs leftover after a contract expires and use for redeploys
    def salvage_frus(self):
        for fru in self.storage:
            cost = self.get_cost('salvage fru', fru, operating_time=fru.get_month(), power=fru.get_power())

            self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(), 'salvaged FRU',
                          'in storage', None, None, None, cost, reason='end of contract')
            self.salvage.append(fru)
        self.storage = []

    # move FRUs between energy servers at a site
    def balance_frus(self, fru, site_number, server1, enclosure1, server2, enclosure2, reason=None):
        cost = self.get_cost('balance fru')

        self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                      'pulled FRU', 'from', site_number, server1, enclosure1, cost/2, reason=reason)

        self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                     'moved FRU', 'to', site_number, server2, enclosure2, cost/2, reason=reason)

    # overhaul a FRU to make it refurbished and bespoke
    ## NOT IMPLEMENTED
    def overhaul_fru(self, queue, mark, site_number, server_number, enclosure_number, reason=None):
        fru = self.junk.pop(queue)
        power_curves, efficiency_curve = self.power_modules.get_curves(fru.model, mark)

        fru.overhaul(mark, power_curves, efficiency_curve)
        cost = self.get_cost('overhaul fru')
        self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                        'overhauled FRU', 'to', site_number, server_number, enclosure_number, cost, reason=reason)
        return fru

    # find FRU in deployable or junk that best fits requirements
    def find_fru(self, allowed_models, power_needed=0, energy_needed=0, time_needed=0, max_power=None, junked=False):
        powers = self.list_powers(allowed_models, junked=junked) - power_needed
        energies = self.list_energies(allowed_models, time_needed, junked=junked) - energy_needed
        found = \
            (powers.where(powers > 0) if power_needed > 0 else 1) * \
            (powers.where(powers < (max_power - power_needed)) if max_power is not None else 1) * \
            (energies.where(energies > 0)/time_needed if energy_needed > 0 else 1)

        queue = found.idxmin() if (type(found) is Series) and len(found) else nan

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

        powers = Series(powers_list)
        return powers

    # get energy value of each fru in storage
    def list_energies(self, allowed_models, time_needed, junked=False):
        if junked:
            energies_list = self.flatten_list([self.power_modules.get_energiesf(fru.model, self.date, time_needed) \
                if fru.model in allowed_models.to_list() else [0] for fru in self.junk])
        else:
            energies_list = [fru.get_energy(months=time_needed) if fru.model in allowed_models.to_list() else 0 \
                for fru in self.deployable]

        energies = Series(energies_list)
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

    # get lastest version of energy server, power module or hotbox
    def get_latest_model(self, category, base_model, install_date=None, **kwargs):
        if category == 'server':
            model = self.energy_servers.get_latest_server_model(install_date, base_model, **kwargs)
            return model

        elif category == 'module':
            module = self.power_modules.get_model(install_date, best=True, server_model=base_model, **kwargs)
            if module is not None:
                model, mark = module
            else:
                model, mark = [None]*2

            return model, mark

        elif category == 'enclosure':
            model = self.hot_boxes.get_model(base_model, **kwargs)
            return model

    # use a stored FRU or create a new one for power and energy requirements
    def get_best_fit_fru(self, server_model, install_date, site_number, server_number, enclosure_number,
                         power_needed=0, energy_needed=0, time_needed=0, max_power=None, initial=False, reason=None):
        allowed_modules = self.energy_servers.get_compatible_modules(server_model)
       
        junked = {'deployable': False} ##, 'junked': True}
        queues = {}

        for location in junked:
            powers = self.list_powers(allowed_modules, junked[location])
            energies = self.list_energies(allowed_modules, time_needed, junked[location])
            queues[location] = self.find_fru(allowed_modules, junked=junked[location],
                                             power_needed=power_needed, energy_needed=energy_needed, time_needed=time_needed,
                                             max_power=max_power)
        
        if (not initial) and len(self.deployable) and (not isna(queues['deployable'])):
            # there is a FRU available to deploy
            queue = queues['deployable']
            fru = self.deploy_fru(queue, site_number, server_number, enclosure_number, reason=reason)

        ##elif (not initial) and len(self.junk) and (not isna(queues['deployable'])):
        ##    # there is a FRU available to overhaul
        ##    queue = self.list_queues(junked=True)[queues['junked']]
        ##    fru = self.overhaul_fru(queue, mark, site_number, server_number, enclosure_nunber, reason=reason)

        else:
            # there is not a FRU available, so create a new one
            if self.best:
                module = self.power_modules.get_model(install_date,
                                                      power_needed=power_needed, max_power=max_power,
                                                      energy_needed=energy_needed, time_needed=time_needed,
                                                      best=self.best, server_model=server_model, allowed_fru_models=self.allowed_fru_models)

            else:
                module = self.power_modules.get_model(install_date,
                                                      power_needed=power_needed, max_power=max_power,
                                                      energy_needed=energy_needed, time_needed=time_needed,
                                                      bespoke=not initial, server_model=server_model, allowed_fru_models=self.allowed_fru_models)

            if module is not None:
                # can create a FRU accoring to requirements
                model, mark = module
                fru = self.create_fru(model, mark, install_date, site_number, server_number, enclosure_number, initial, reason=reason)

            else:
                # cannot create a FRU according to requirements
                fru = None

        return fru

    # get base model of a server model number
    def get_server_model(self, server_model_number):
        server_model = self.energy_servers.get_server_model(server_model_number=server_model_number)['model']
        return server_model

    # get model types to most closely match target size
    def prepare_servers(self, new_servers, target_size):
        # check if server model number is given
        if (new_servers is not None) and (len(new_servers['model'])):
            # get values from database
            server_model_number = new_servers['model']
            server_model = self.energy_servers.get_server_model(server_model_number=new_servers['model'])
            server_count = int(target_size / server_model['nameplate'])

        else:
            # check if server model class is available in given year, else use next best
            target_model = new_servers['model'] if new_servers is not None else None
            latest_server_model_class = self.energy_servers.get_latest_server_model(self.date, target_model)
        
            # get default nameplate sizes   
            server_nameplates = self.energy_servers.get_server_nameplates(latest_server_model_class, target_size)

            # determine max number of various size servers could fit
            server_nameplates.loc[:, 'fit'] = (target_size / server_nameplates['nameplate']).astype(int)
            # find lost potential because of server sizes
            server_nameplates.loc[:, 'loss'] = target_size - (server_nameplates['nameplate'] * server_nameplates['fit'])
            # return best fit model to minimize potential loss
            server_model_number, server_count = server_nameplates.sort_values('loss')[['model_number', 'fit']].iloc[0]

        return server_model_number, server_count

    # create a new energy server
    def create_server(self, site_number, server_number, server_model_number=None, server_model_class=None,
                      nameplate_needed=0, n_enclosures=None,
                      reason='populating site'):
        serial = self.get_serial('ES')

        server_model = self.energy_servers.get_server_model(server_model_number=server_model_number,
                                                            server_model_class=server_model_class,
                                                            nameplate_needed=nameplate_needed,
                                                            n_enclosures=n_enclosures)
        
        server = Server(serial, server_number, server_model['model'], server_model['model_number'], server_model['nameplate'])

        # get cost to create a server
        cost = self.get_cost('install server', server)

        self.transact(server.serial, server.model, server.model_number, server.nameplate, None,
                      'installed ES', 'at', site_number, server.number, None, cost, reason=reason)

        enclosure_model, enclosure_rating = self.hot_boxes.get_model(server_model['model'])

        enclosures = self.create_enclosures(site_number, server, enclosure_model, enclosure_rating,
                                            enclosure_count=server_model['enclosures'], plus_one_count=server_model['plus_one'])     

        for enclosure in enclosures:
            server.add_enclosure(enclosure)

        return server

    # upgrade server inverter to a higher capactiy ## NOT USED YET
    def upgrade_server(self, server, site_number, nameplate, reason=None):
        server.upgrade_nameplate(nameplate)
        cost = self.get_cost('upgrade server', server, power=nameplate - server.nameplate)
        self.transact(server.serial, server.model, server.model_number, nameplate, None,
                     'upgraded ES', 'at', site_number, server.number, None, cost, reason=reason)
        pass

    # upgrade components on enclosure to allow more power throughput for new module tech
    def upgrade_enclosures(self, site_number, server, new_fru, reason=None):
        cost = self.get_cost('upgrade enclosure')

        new_enclosure_model, new_enclosure_rating = self.hot_boxes.get_model(new_fru.model)

        for enclosure in server.enclosures:
            enclosure.model = new_enclosure_model
            enclosure.rating = new_enclosure_rating
            self.transact(enclosure.serial, enclosure.model, None, None, None, 'increase enclosure rating',
                          'at', site_number, server.number, enclosure.number, cost, reason)
        return
    
    # return nameplate rating of server model
    def get_server_nameplate(self, server_model):
        nameplate = self.energy_servers.get_server_nameplates(server_model)
        return nameplate

    # create an enclosure cabinent to add to a server to house a FRU
    def create_enclosures(self, site_number, server, enclosure_model, rating,
                          start_number=0, enclosure_count=0, plus_one_count=0,
                          reason='populating server'):
        # get cost per enclosure
        costs = enclosure_count * [self.get_cost('intialize enclosure')] + plus_one_count * [self.get_cost('add enclosure')] 
       
        # create enclosures
        enclosures = []
        for c in range(enclosure_count + plus_one_count):
            serial = self.get_serial('ENC')
            enclosure = Enclosure(serial, start_number + c, enclosure_model, rating)
            enclosures.append(enclosure)
            self.transact(serial, server.model, 'ENCLOSURE', None, None,
                          'add enclosure', 'at', site_number, server.number, enclosure.number, costs[c], reason=reason)

        return enclosures

    # all FRUs in storage become deployable after one period
    def advance(self):
        for fru in self.storage:
            # FRU storage moves power curve forward
            fru.store(self.thresholds['deploy months'])

            # check if storage killed FRU
            if fru.get_power() < self.junk_level:
                self.junk.append(fru)
               
                self.transact(fru.serial, fru.model, fru.mark, fru.get_power(), fru.get_efficiency(),
                              'junked FRU', 'in storage', None, None, None, None, reason='power below {}kw'.format(self.junk_level))

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

        self.system_sizes = system_sizes
        self.system_dates = system_dates

        self.set_up_install_sizes()
        self.set_up_install_months()
        self.set_up_target_site(start_date, min_date)

    # pick sizes based on distribution for sites other than target
    def set_up_install_sizes(self):
        self.install_sizes = self.system_sizes.sample(self.total_sites - 1, replace=True).to_list()
        return
        
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
        return

    # add site to fleet    
    def add_site(self, site):
        self.sites.append(site)
        return

    # remove site from fleet
    def remove_site(self, site):
        self.sites = [s for s in self.sites if s is not site]
        # record power and efficiency
        self.store_site_performance(site)
        self.store_fru_performance(site)

    # store site power and efficiency
    def store_site_performance(self, site):
        self.shop.log_book.record_performance('site', site.number, site.monitor.get_results('performance'))
        return

    # store FRU power and efficiency
    def store_fru_performance(self, site):
        self.shop.log_book.record_performance('fru', site.number, site.monitor.get_results('power'), site.monitor.get_results('efficiency'))
        return

    # get transaction log
    def get_transactions(self, site_number=None, last_date=None):
        if self.shop is not None:
            transactions = self.shop.log_book.get_transactions(site_number, last_date)
            return transactions

    # combine transactions by year, site and action
    def summarize_transactions(self, site_number='target'):
        if site_number == 'target':
            site_number = self.target_site

        transactions = self.shop.log_book.summarize_transactions(site_number)

        return transactions

    # return power and efficiency of all a site
    def summarize_site_performance(self, site_number='target'):
        if site_number == 'target':
            site_number = self.target_site

        for site in self.sites:
            # add TMO and eff of any site that hasn't expired
            self.store_site_performance(site)

        site_performance = self.shop.log_book.get_performance('site', site_number)

        return site_performance

    # return power and efficiency of all FRUs at a site
    def get_fru_performance(self, site_number='target'):
        if site_number == 'target':
            site_number = self.target_site

        for site in self.sites:
            # add FRU perfromance of any site that hasn't expired
            self.store_fru_performance(site)
       
        fru_performance = self.shop.log_book.get_performance('fru', site_number)

        return fru_performance