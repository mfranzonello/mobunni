# central warehouse for creating, storing and deploying components and fleet to manage all sites

import pandas
from dateutil.relativedelta import relativedelta
from powerful import PowerCurves, PowerModules
from components import FRU, Enclosure, Server

# warehouse to store, repair and deploy old FRUs and create new FRUs
class Shop:
    def __init__(self, sql_db, install_date, junk_level=20, best=False, allowed_fru_models=None):
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

        self.date = install_date

        self.allowed_fru_models = allowed_fru_models

        self.next_serial = {'ES': 0, 'PWM': 0, 'ENC': 0}

        self.transactions = pandas.DataFrame(columns=['date', 'serial', 'model', 'mark', 'power', 'efficiency', 'action',
                                                      'direction', 'site', 'server', 'enclosure', 'service cost'])

    # record log of transactions
    def transact(self, serial, module_model, module_mark, power, efficiency,
                 action, direction, site_number, server_number, enclosure_number, cost):
        self.transactions.loc[len(self.transactions), :] = [self.date, serial, module_model, module_mark, power, efficiency,
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
    def create_fru(self, module_model, module_mark, install_date, site_number, server_number, enclosure_number, initial=False, fit=None):
        serial = self.get_serial('PWM')
        power_curves = PowerCurves(self.sql_db.get_power_curves(module_model, module_mark))
        efficiency_curve = self.sql_db.get_efficiency_curve(module_model, module_mark)

        fru = FRU(serial, module_model, module_mark, power_curves, efficiency_curve, install_date, current_date=install_date, fit=fit)

        cost_action = 'initialize fru' if initial else 'create fru'
        cost = self.get_cost(cost_action, model=module_model, mark=module_mark)

        transact_action = 'intialized PWM' if initial else 'created FRU'
        self.transact(serial, module_model, module_mark, fru.get_power(), fru.get_efficiency(),
                      transact_action, 'to', site_number, server_number, enclosure_number, cost)

        return fru

    # take a FRU from a site and add to storage queue
    def store_fru(self, fru, site_number, server_number, enclosure_number, repair=False, final=False):
        if (self.junk_level is None) or (fru.get_power() < self.junk_level) or (fru.get_expected_life() <= self.deploy_months):
            # if FRU has no value, junk it
            self.junk.append(fru)

            cost = self.get_cost('junk fru')

            self.transact(fru.serial, fru.module_model, fru.module_mark, fru.get_power(), fru.get_efficiency(),
                          'junked FRU', 'from', site_number, server_number, enclosure_number, cost)

        else:
            # FRU could be deployed in future
            self.storage.append(fru)

            cost = self.get_cost('store fru') if not final else 0 ## decommissioning doesn't count as service

            self.transact(fru.serial, fru.module_model, fru.module_mark, fru.get_power(), fru.get_efficiency(),
                          'stored FRU', 'from', site_number, server_number, enclosure_number, cost)

            # repairing FRU moves power curve up
            if repair:
                self.storage[-1].repair()

                cost = self.get_cost('repair fru', model=fru.module_model, mark=fru.module_mark, operating_time=fru.month, power=fru.get_power())

                self.transact(fru.serial, fru.module_model, fru.module_mark, fru.get_power(), fru.get_efficiency(),
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

        self.transact(fru.serial, fru.module_model, fru.module_mark, fru.get_power(), fru.get_efficiency(),
                      'deployed FRU', 'to', site_number, server_number, enclosure_number, cost)

        return fru

    # get value for FRUs leftover after a contract expires and use for redeploys
    def salvage_frus(self):
        for fru in self.storage:
            cost = self.get_cost('salvage fru', fru.module_model, fru.module_mark, operating_time=fru.month, power=fru.get_power())

            self.transact(fru.serial, fru.module_model, fru.module_mark, fru.get_power(), fru.get_efficiency(), 'salvaged FRU',
                          '', 0, 0, 0, cost)
            self.salvage.append(fru)
        self.storage = []

    # move FRUs between energy servers at a site
    def balance_frus(self, fru, site_number, server1, enclosure1, server2, enclosure2):
        cost = self.get_cost('balance fru')

        self.transact(fru.serial, fru.module_model, fru.module_mark, fru.get_power(), fru.get_efficiency(),
                      'pulled FRU', 'from', site_number, server1, enclosure1, cost/2)

        self.transact(fru.serial, fru.module_model, fru.module_mark, fru.get_power(), fru.get_efficiency(),
                     'moved FRU', 'to', site_number, server2, enclosure2, cost/2)

    # find fru that best fits requirements
    def find_fru(self, allowed_models, power_needed=0, energy_needed=0, time_needed=0):
        powers = pandas.Series(self.list_powers(allowed_models)) - power_needed
        energies = pandas.Series(self.list_energies(allowed_models, time_needed)) - energy_needed
        found = (powers.where(powers>0) if power_needed > 0 else 1) * \
            (energies.where(energies>0)/time_needed if energy_needed > 0 else 1)
        queue = found.idxmin()

        return queue

    # get power value of each fru in storage
    def list_powers(self, allowed_models):
        powers = [fru.get_power() if fru.module_model in allowed_models else 0 for fru in self.deployable]
        return powers

    # get energy value of each fru in storage
    def list_energies(self, allowed_models, time_needed):
        energies = [fru.get_energy(months=time_needed) if fru.module_model in allowed_models else 0 for fru in self.deployable]
        return energies

    # use a stored FRU or create a new one for power and energy requirements
    def best_fit_fru(self, server_model, install_date, site_number, server_number, enclosure_number,
                     power_needed=0, energy_needed=0, time_needed=0, initial=False):
        allowed_modules = self.sql_db.get_compatible_modules(server_model)
        
        powers = self.list_powers(allowed_modules)
        energies = self.list_energies(allowed_modules, time_needed)
        matching = [power >= power_needed and energy >= energy_needed for power, energy in zip(powers, energies)]

        if (not initial) and len(self.deployable) and any(matching):
            # there is a FRU available to deploy
            queue = self.find_fru(allowed_modules, power_needed=power_needed, energy_needed=energy_needed, time_needed=time_needed)
            fru = self.deploy_fru(queue, site_number, server_number, enclosure_number)
        else:
            # there is not a FRU available, so create a new one
            if self.best:
                module_model, module_mark = self.power_modules.get_model(allowed_modules, install_date,
                                                                         power_needed=power_needed, energy_needed=energy_needed, time_needed=time_needed,
                                                                         best=self.best, allowed_fru_models=self.allowed_fru_models)
            else:
                module_model, module_mark = self.power_modules.get_model(allowed_modules, install_date,
                                                                         power_needed=power_needed, energy_needed=energy_needed, time_needed=time_needed,
                                                                         bespoke=not initial, allowed_fru_models=self.allowed_fru_models)
            fru = self.create_fru(module_model, module_mark, install_date, site_number, server_number, enclosure_number, initial)

        return fru

    # create a new energy server
    def create_server(self, site_number, server_number, server_model):
        serial = self.get_serial('ES')

        nameplate = self.get_server_nameplate(server_model)
        server = Server(serial, server_number, server_model, nameplate)

        cost = self.get_cost('install server')

        self.transact(server.serial, server_model, 'SERVER', nameplate, 0,
                      'installed ES', 'at', site_number, server_number, -1, cost)

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
    def create_enclosure(self, site_number, server, enclosure_number, plus_one=False):
        serial = self.get_serial('ENC')
        enclosure = Enclosure(serial, enclosure_number)
        cost_action = 'initialize enclosure' if not plus_one else 'add enclosure'
        cost = self.get_cost(cost_action)
        self.transact(serial, server.model, 'ENCLOSURE', 0, 0,
                      'add enclosure', 'at', site_number, server.number, enclosure.number, cost)

        return enclosure

    # all FRUs in storage become deployable after one period
    def advance(self):
        for fru in self.storage:
            # FRU storage moves power curve forward
            fru.store(self.deploy_months)

            # check if storage killed FRU
            if fru.is_dead():
                self.junk.append(fru)
               
                self.transact(fru.serial, fru.module_model, fru.module_mark, fru.get_power(), 'junked FRU',
                          '', -1, -1, -1, 0)

            else:
                self.deployable.append(fru)

        self.storage = []
        self.date += relativedelta(months=1)

# collection of sites and a shop to move FRUs between
class Fleet:
    def __init__(self):
        self.sites = []
        self.shop = None
        self.performance = []

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
        self.performance.append(site.performance)

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

    # combine transactions by year, site and action
    def summarize_transactions(self):
        transactions_yearly = self.get_transactions()
        transactions_yearly.insert(0, 'year', pandas.to_datetime(transactions_yearly['date']).dt.year)
        transactions_gb = transactions_yearly[['year', 'site', 'action', 'service cost']].groupby(['year', 'site', 'action'])
        
        transactions_sum = transactions_gb.sum()[['service cost']]
        transactions_count = transactions_gb.count()[['service cost']].rename(columns={'service cost': 'count'})
        
        transactions_summarized = pandas.concat([transactions_count, transactions_sum], axis='columns').reset_index()

        return transactions_summarized

    # return power and efficiency of all sites
    def summarize_performance(self):
        for site in self.sites:
            # add TMO and eff of any site that hasn't expired
            self.performance.append(site.performance)

        return self.performance

    # return residual value
    def summarize_residuals(self):
        if self.shop is not None:
            residuals = pandas.DataFrame(data=zip(self.shop.residuals.index+1, self.shop.residuals), columns=['site', 'residual'])
            return residuals