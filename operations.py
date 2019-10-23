# central warehouse for creating, storing and deploying components and fleet to manage all sites

# built-in imports
from dateutil.relativedelta import relativedelta
from datetime import date
from random import randrange
from math import floor

# add-on imports
from pandas import DataFrame, Series, concat, to_datetime, to_numeric, isna
from numpy import nan

# self-defined imports
from structure import SQLDB
from powerful import PowerModules, HotBoxes, EnergyServers
from components import Component, FRU, Enclosure, Server
from finances import Bank
from groups import Thresholds, Technology, Tweaks

# record of transactions and results across shop and fleet
class LogBook:
    def __init__(self):
        self.transactions = DataFrame(columns=['date', 'serial', 'model', 'model number', 'power', 'efficiency', 'action',
                                               'direction', 'site', 'server', 'enclosure', 'service cost', 'reason'])
        self.performance = {'site': {}, 'fru': {}}

    def number(self, value:float) -> int:
        try:
            if int(value) == value:
                num = int(value) + 1
            else:
                num = value
        except TypeError:
            num = value
        
        return num

    # record log of transactions
    def record_transaction(self, action_date:date, serial:str, model:str, model_number:str, power:float, efficiency:float,
                           action:str, direction:str, site_number:str, server_number:str, enclosure_number:str, cost:float, reason:str=None):
        self.transactions.loc[len(self.transactions), :] = [action_date, serial, model, model_number, power, efficiency,
                                                            action, direction,
                                                            self.number(site_number), self.number(server_number), self.number(enclosure_number),
                                                            cost, reason]

    # store power and efficiency
    def record_performance(self, table:str, site_number:str, *args):
        if table == 'site':
            [performance] = args
            self.performance['site'][site_number] = performance

        elif table == 'fru':
            power, efficiency = args

            power.insert(0, 'site', site_number)
            efficiency.insert(0, 'site', site_number)
            self.performance['fru'][site_number] = {'power': power, 'efficiency': efficiency}

    # combine transactions by year, site and action
    def get_transactions(self, site_number:str=None, last_date:bool=None) -> DataFrame:
        transactions = self.transactions.copy()

        if site_number is not None:
            filter = (transactions['site'] == site_number+1)

            if last_date is not None:
                filter &= (transactions['date'] == last_date)
            transactions = transactions[filter]

        return transactions

    def get_performance(self, table:str, site_number:str) -> DataFrame:
        performance = self.performance[table][site_number]
        return performance

    # return series of value if site is target site
    def identify_target(self, sites:DataFrame, site_number:str, site_col:str='site', site_adder:bool=True, target_col:str='target') -> DataFrame:
        target_series = sites[site_col] == (site_number + site_adder)
        target_identified = concat([sites, target_series.rename(target_col)], axis='columns')

        return target_identified

    # combine transactions by year, site and action
    def summarize_transactions(self, site_number:str) -> DataFrame:
        transactions_yearly = self.get_transactions()
        transactions_yearly.insert(0, 'year', to_datetime(transactions_yearly['date']).dt.year)
        transactions_gb = transactions_yearly[['year', 'site', 'action', 'service cost', 'power']].groupby(['year', 'site', 'action'])
        
        transactions_sum = transactions_gb.sum()[['service cost', 'power']]
        transactions_count = transactions_gb.count()[['service cost']].rename(columns={'service cost': 'count'})
        
        transactions_summarized = concat([transactions_sum, transactions_count], axis='columns').reset_index()

        transactions = self.identify_target(transactions_summarized, site_number)

        return transactions

# template for new modules and servers
class Templates:
    def __init__(self, power_modules:PowerModules, hot_boxes:HotBoxes, energy_servers:EnergyServers):
        self.power_modules = power_modules
        self.hot_boxes = hot_boxes
        self.energy_servers = energy_servers

        self.components = {comp: {} for comp in ['module', 'enclosure', 'server']}

    def find_component(self, component_type:str, model:str=None, mark:str=None, model_number:str=None, serial=None, **kwargs) -> Component:
        key = tuple(m for m in [model, mark, model_number] if m is not None)
        
        if key not in self.components[component_type]:
            self.ghost_component(component_type, key, model, mark, model_number)

        component = self.components[component_type][key].copy(serial, **kwargs)

        return component

    def ghost_component(self, component_type:str, key:tuple, model:str=None, mark:str=None, model_number:str=None):
        if component_type == 'module':
            component = self.ghost_module(model, mark, model_number)

        elif component_type == 'enclosure':
            component = self.ghost_enclosure(model, model_number)

        elif component_type == 'server':
            component = self.ghost_server(model, model_number)

        self.components[component_type][key] = component

    def ghost_module(self, model:str, mark:str, model_number:str) -> FRU:
        serial, install_date = [None]*2 # blank serial and date

        power_curves, efficiency_curves = self.power_modules.get_curves(model, mark, model_number)
        rating = self.power_modules.get_rating(model, mark, model_number)
        stacks = self.power_modules.get_stacks(model, mark, model_number)[0]

        fru = FRU(serial, model, mark, model_number,
                  rating, power_curves, efficiency_curves, stacks,
                  install_date, current_date=install_date)

        return fru

    def ghost_enclosure(self, model:str, model_number:str) -> Enclosure:
        serial, number = [None]*2
        _, nameplate = self.hot_boxes.get_model_details(model)
        enclosure = Enclosure(serial, number, model, model_number, nameplate)

        return enclosure

    def ghost_server(self, model:str, model_number:str) -> Server:
        serial, number = [None]*2

        nameplate = self.energy_servers.get_server_model(server_model_class=model, server_model_number=model_number)['nameplate']
        server = Server(serial, number, model, model_number, nameplate)

        return server


# warehouse to store, repair and deploy old FRUs and create new FRUs
class Shop:
    def __init__(self, sql_db:SQLDB, thresholds:Thresholds, install_date:date, tweaks:Tweaks, technology:Technology):
        self.power_modules = PowerModules(sql_db)
        self.hot_boxes = HotBoxes(sql_db)
        self.energy_servers = EnergyServers(sql_db)
        self.bank = Bank(sql_db)

        self.templates = Templates(self.power_modules, self.hot_boxes, self.energy_servers)
        self.log_book = LogBook()

        self.thresholds = thresholds
        self.tweaks = tweaks

        self.storage = []
        self.deployable = []
        self.junked = []
        self.salvage = []

        self.date = install_date

        self.roadmap = technology.get_roadmap()

        self.next_serial = {'ES': 0, 'PWM': 0, 'ENC': 0}

    # record log of transactions
    def transact(self, serial:str, model:str, model_number:str, power:float, efficiency:float,
                 action:str, direction:str, site_number:str, server_number:str, enclosure_number:str, cost:float, reason:str=None):
        self.log_book.record_transaction(self.date, serial, model, model_number, power, efficiency,
                                         action, direction, site_number, server_number, enclosure_number, cost, reason)

    # return serial number for component tracking
    def get_serial(self, component:str) -> str:
        self.next_serial[component] += 1
        serial = '{}{}'.format(component, str(self.next_serial[component]).zfill(6))
        return serial

    # get cost for action
    def get_cost(self, action:str, component:str=None, **kwargs) -> float:
        cost = self.bank.get_cost(self.date, action, component, **kwargs)
        return cost

    # copy a FRU from a template
    def create_fru(self, model:str, mark:str, model_number:str, install_date:date, site_number:str, server_number:str, enclosure_number:str,
                   initial:bool=False, current_date:date=None, fit:dict=None, reason:str=None) -> FRU:

        serial = self.get_serial('PWM')
        
        # check if template already created to reduce calls to DB
        fru = self.templates.find_component('module', model=model, mark=mark, model_number=model_number,
                                            serial=serial, install_date=install_date,
                                            current_date=current_date if current_date is not None else install_date,
                                            fit=fit)

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

        self.transact(fru.serial, fru.model, fru.get_model_number(), fru.get_power(), fru.get_efficiency(),
                      'stored FRU', 'from', site_number, server_number, enclosure_number, cost, reason=reason)

        # repairing FRU moves power curve up
        if self.tweaks['repair'] and repair:
            self.storage[-1].repair()

            cost = self.get_cost('repair fru', fru, operating_time=fru.get_month(), power=fru.get_power())

            self.transact(fru.serial, fru.model, fru.get_model_number(), fru.get_power(), fru.get_efficiency(),
                          'repaired FRU', 'from', site_number, server_number, enclosure_number, cost, reason=reason)

        return

    # take a FRU out of storage to send to site    
    def deploy_fru(self, queue, site_number, server_number, enclosure_number, reason=None):
        fru = self.deployable.pop(queue)
        
        cost = self.get_cost('deploy fru')

        self.transact(fru.serial, fru.model, fru.get_model_number(), fru.get_power(), fru.get_efficiency(),
                      'deployed FRU', 'to', site_number, server_number, enclosure_number, cost, reason=reason)

        return fru

    # get value for FRUs leftover after a contract expires and use for redeploys
    def salvage_frus(self):
        for fru in self.storage:
            cost = self.get_cost('salvage fru', fru, operating_time=fru.get_month(), power=fru.get_power())

            self.transact(fru.serial, fru.model, fru.get_model_number(), fru.get_power(), fru.get_efficiency(), 'salvaged FRU',
                          'in storage', None, None, None, cost, reason='end of contract')
            self.salvage.append(fru)
        self.storage = []

    # move FRUs between energy servers at a site
    def balance_frus(self, fru, site_number, server1, enclosure1, server2, enclosure2, reason=None):
        cost = self.get_cost('balance fru')

        self.transact(fru.serial, fru.model, fru.get_model_number(), fru.get_power(), fru.get_efficiency(),
                      'pulled FRU', 'from', site_number, server1, enclosure1, cost/2, reason=reason)

        self.transact(fru.serial, fru.model, fru.get_model_number(), fru.get_power(), fru.get_efficiency(),
                     'moved FRU', 'to', site_number, server2, enclosure2, cost/2, reason=reason)

    # overhaul a FRU to make it refurbished and bespoke
    def overhaul_fru(self, queue, stacks, site_number, server_number, enclosure_number, reason=None):
        fru = self.junked.pop(queue)

        fru.overhaul(stacks)
        cost = self.get_cost('overhaul fru', )
        self.transact(fru.serial, fru.model, fru.get_model_number(), fru.get_power(), fru.get_efficiency(),
                        'overhauled FRU', 'to', site_number, server_number, enclosure_number, cost, reason=reason)
        return fru

    # get lastest version of energy server, power module or hotbox
    def get_latest_model(self, category, base_model, install_date=None, **kwargs):
        if category == 'server':
            model = self.energy_servers.get_latest_server_model(install_date, base_model, **kwargs)
            return model

        elif category == 'module':
            module = self.power_modules.get_model(install_date, best=True, server_model=base_model, roadmap=self.roadmap, **kwargs)
            if module is not None:
                model, mark, model_number = module
            else:
                model, mark, model_number = [None]*3

            return model, mark, model_number

        elif category == 'enclosure':
            model, _ = self.hot_boxes.get_model_details(base_model, **kwargs)
            return model

    # find FRU in deployable or junk that best fits requirements
    def find_fru(self, allowed_models, power_needed=0, energy_needed=0, efficiency_needed=0, 
                 time_needed=0, max_power=None, junked=False):
        powers = self.list_powers(allowed_models, junked=junked) - power_needed
        energies = self.list_energies(allowed_models, time_needed, junked=junked) - energy_needed
        efficiencies = self.list_efficiencies(allowed_models, junked=junked) - efficiency_needed

        found = \
            (powers.where(powers > 0) if power_needed > 0 else 1) * \
            (powers.where(powers < (max_power - power_needed)) if max_power is not None else 1) * \
            (energies.where(energies > 0)/time_needed if energy_needed > 0 else 1) * \
            (efficiencies.where(efficiencies > 0) if efficiency_needed > 0 else 1)

        queue, stacks = [None]*2

        if (found is not 1):
            for c in found.columns:
                found.loc[:, c] = to_numeric(found[c], errors='coerce')
            
            if not found.empty and not found.stack().empty:
                idx = found.stack().idxmin()
                if not isna(idx):
                    queue, stacks = idx

        return queue, stacks
    
    # get power value of each fru in storage
    def list_powers(self, allowed_models, junked=False):
        powers = DataFrame(columns=self.list_stacks(allowed_models, junked))
        location = self.junked if junked else self.deployable
        for fru in location:
            if fru.model in allowed_models.to_list():
                if junked:
                    stacks = self.power_modules.get_stacks(fru.model, fru.mark, fru.model_number)
                    power = [self.power_modules.get_rating(fru.model, fru.mark, fru.model_number) * (stack/fru.stacks) for stack in stacks]
                    powers.loc[location.index(fru), stacks] = power

                else:
                    powers.loc[location.index(fru), fru.stacks] = fru.get_power()
                    
        return powers

    # get energy value of each fru in storage
    def list_energies(self, allowed_models, time_needed, junked=False):
        energies = DataFrame(columns=self.list_stacks(allowed_models, junked))
        location = self.junked if junked else self.deployable
        for fru in location:
            if fru.model in allowed_models.to_list():
                if junked:
                    stacks = self.power_modules.get_stacks(fru.model, fru.mark, fru.model_number)
                    energy = [self.power_modules.get_energy(fru.model, fru.mark, fru.model_number,
                                                            time_needed=time_needed) * (stack/fru.stacks) for stack in stacks]
                    energies.loc[location.index(fru), stacks] = energy
                        
                else:
                    energies.loc[location.index(fru), fru.stacks] = fru.get_energy(months=time_needed)

        return energies

    # get efficiency value of each fru in storage
    def list_efficiencies(self, allowed_models, junked=False):
        efficiencies = DataFrame(columns=self.list_stacks(allowed_models, junked))
        location = self.junked if junked else self.deployable
        for fru in location:
            if fru.model in allowed_models.to_list():
                if junked:
                    stacks = self.power_modules.get_stacks(fru.model, fru.mark, fru.model_number)
                    efficiency = [self.power_modules.get_efficiency(fru.model, fru.mark, fru.model_number) * \
                                    (stack/fru.stacks) for stack in stacks]
                    efficiencies.loc[location.index(fru), stacks] = efficiency

                else:
                    efficiencies.loc[location.index(fru), fru.stacks] = fru.get_power() * fru.get_efficiency()

        return efficiencies

    # get stack value of each fru in storage
    def list_stacks(self, allowed_models, junked=False):
        if junked:
            stacks_list = [x for y in [self.power_modules.get_stacks(fru.model, fru.mark, fru.model_number) \
                for fru in self.junked if fru.model in allowed_models.to_list()] for x in y]
        else:
            stacks_list = [fru.stacks for fru in self.deployable if fru.model in allowed_models.to_list()]

        stacks_list = list(set(stacks_list))

        return stacks_list

    # use a stored FRU or create a new one for power and energy requirements
    def get_best_fit_fru(self, server_model, install_date, site_number, server_number, enclosure_number,
                         power_needed=0, energy_needed=0, efficiency_needed=0, time_needed=0, max_power=None, initial=False, reason=None):
        allowed_modules = self.energy_servers.get_compatible_modules(server_model)
       
        storage_location = {'deployable': False, 'junked': True}
        queues = {}

        for location in storage_location:
            powers = self.list_powers(allowed_modules, storage_location[location])
            energies = self.list_energies(allowed_modules, time_needed, storage_location[location])
            efficiencies = self.list_efficiencies(allowed_modules, storage_location[location])

            queues[location] = self.find_fru(allowed_modules, junked=storage_location[location],
                                             power_needed=power_needed, energy_needed=energy_needed, efficiency_needed=efficiency_needed,
                                             time_needed=time_needed, max_power=max_power)
        
        if self.tweaks['redeploy'] and (not initial) and len(self.deployable) and (queues['deployable'][0] is not None):
            # there is a FRU available to deploy
            queue, _ = queues['deployable']
            fru = self.deploy_fru(queue, site_number, server_number, enclosure_number, reason=reason)

        elif self.tweaks['redeploy'] and (not initial) and len(self.junked) and (queues['junked'][0] is not None):
            # there is a FRU available to overhaul
            queue, stacks = queues['junked']
            fru = self.overhaul_fru(queue, stacks, site_number, server_number, enclosure_number, reason=reason)

        else:
            # there is not a FRU available, so create a new one
            if self.tweaks['best']:
                module = self.power_modules.get_model(install_date, wait_period=not initial,
                                                      power_needed=power_needed, max_power=max_power,
                                                      energy_needed=energy_needed, time_needed=time_needed,
                                                      best=self.tweaks['best'], server_model=server_model, roadmap=self.roadmap)

            else:
                module = self.power_modules.get_model(install_date, wait_period=not initial,
                                                      power_needed=power_needed, max_power=max_power,
                                                      energy_needed=energy_needed, time_needed=time_needed,
                                                      server_model=server_model, roadmap=self.roadmap) ##bespoke=not initial

            if module is not None:
                # can create a FRU accoring to requirements
                model, mark, model_number = module
                fru = self.create_fru(model, mark, model_number, install_date, site_number, server_number, enclosure_number,
                                      initial=initial, reason=reason)

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
        
        server = self.templates.find_component('server', model=server_model['model'], model_number=server_model['model_number'],
                                               serial=serial, number=server_number)

        # get cost to create a server
        cost = self.get_cost('install server', server)

        self.transact(server.serial, server.model, server.get_model_number(), server.nameplate, None,
                      'installed ES', 'at', site_number, server.number, None, cost, reason=reason)

        enclosure_model_number, _ = self.hot_boxes.get_model_details(server_model['model'])

        enclosures = self.create_enclosures(site_number, server, server.model, enclosure_model_number,
                                            enclosure_count=server_model['enclosures'], plus_one_count=server_model['plus_one'])     

        for enclosure in enclosures:
            server.add_enclosure(enclosure)

        return server

    # upgrade server inverter to a higher capactiy ## NOT USED YET
    def upgrade_server(self, server, site_number, nameplate, reason=None):
        server.upgrade_nameplate(nameplate)
        cost = self.get_cost('upgrade server', server, power=nameplate - server.nameplate)
        self.transact(server.serial, server.model, server.get_model_number(), nameplate, None,
                     'upgraded ES', 'at', site_number, server.number, None, cost, reason=reason)
        pass

    # upgrade components on enclosure to allow more power throughput for new module tech
    def upgrade_enclosures(self, site_number, server, new_fru, reason=None):
        cost = self.get_cost('upgrade enclosure')

        enclosure_model_number, enclosure_nameplate = self.hot_boxes.get_model_details(new_fru.model)

        for enclosure in server.enclosures:
            enclosure.upgrade_enclosure(new_fru.model, enclosure_model_number, enclosure_nameplate)

            self.transact(enclosure.serial, enclosure.model, enclosure.get_model_number(), enclosure.nameplate, None,
                          'increased ENC', 'at', site_number, server.number, enclosure.number, cost, reason)
        return
    
    # return nameplate rating of server model
    def get_server_nameplate(self, server_model):
        nameplate = self.energy_servers.get_server_nameplates(server_model)
        return nameplate

    # create an enclosure cabinent to add to a server to house a FRU
    def create_enclosures(self, site_number, server, enclosure_model, enclosure_model_number,
                          start_number=0, enclosure_count=0, plus_one_count=0,
                          reason='populating server'):
        # get cost per enclosure
        costs = enclosure_count * [self.get_cost('intialize enclosure')] + plus_one_count * [self.get_cost('add enclosure')] 
       
        # create enclosures
        enclosures = []
        for c in range(enclosure_count + plus_one_count):
            serial = self.get_serial('ENC')
            enclosure = self.templates.find_component('enclosure', model=enclosure_model, model_number=enclosure_model_number,
                                                      serial=serial, number=start_number + c)
            enclosures.append(enclosure)
            self.transact(serial, enclosure.model, enclosure.get_model_number(), enclosure.nameplate, None,
                          'add enclosure', 'at', site_number, server.number, enclosure.number, costs[c], reason=reason)

        return enclosures

    # all FRUs in storage become deployable after one period
    def advance(self):
        for fru in self.storage:
            # FRU storage moves power curve forward
            fru.store(self.thresholds['deploy months'])

            # check if storage killed FRU
            if fru.get_power() < self.thresholds['junk level']:
                self.junked.append(fru)
               
                self.transact(fru.serial, fru.model, fru.get_model_number(), fru.get_power(), fru.get_efficiency(),
                              'junked FRU', 'in storage', None, None, None, None, reason='power below {:0.1f}kw'.format(self.thresholds['junk level']))

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