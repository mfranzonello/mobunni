# physical sites were energy servers are installed

from dateutil.relativedelta import relativedelta
from math import ceil, floor

from pandas import DataFrame, Series, isnull
from numpy import nan

from inspection import Monitor, Inspector
from debugging import StopWatch

# group of energy servers
class Site:
    '''
    A site holds a collection of energy servers with
    enclosures filled with power modules.
    Each month it is inspected to see if it is meeting
    contractural requirements. It calls the shop
    for new components or redeploys from the rest
    of the fleet.
    At the end of the contract, the site is
    decommissioned.
    '''
    def __init__(self, number, shop, contract): 
        self.number = number
        self.shop = shop

        self.contract = contract
        self.system_size = 0

        self.limits = contract.limits
        self.windowed = contract.windowed

        self.monitor = Monitor(self.number, self.contract.start_date, self.contract.length, self.windowed)

        self.servers = {}

        self.month = 0 ##contract.start_month

    def __str__(self):
        m_string = max(0, self.get_month()-1)
        month_string = 'Contract month {}'.format(self.get_month()+1)
        ceiling_loss = self.get_site_ceiling_loss()
        ceiling_string = ' | {:0.3f}MW ceiling loss'.format(ceiling_loss/1000) if ceiling_loss > 0 else ''
        site_string = 'SITE: {:0.3f}MW @ {:0.3f}MW | CTMO {:0.1%}, PTMO {:0.1%}, eff {:0.1%}{}'\
            .format(self.system_size/1000, self.performance.loc[m_string, 'power']/1000,
                    self.performance.loc[m_string, 'CTMO'],
                    self.performance.loc[m_string, 'PTMO'],
                    self.performance.loc[m_string, 'cumu eff'],
                    ceiling_string)

        line_string = '='*20
        server_string = '\n'.join([line_string] + [str(server) for server in self.get_servers()] + [line_string])
        string = '{}\n{}'.format(site_string, server_string)
        return string

    # get current operating month
    def get_month(self):
        month = int(self.month)
        return month

    # return current date for FRU installation
    def get_date(self):
        date = self.contract.start_date + relativedelta(months=self.get_month())
        return date

    # return years into the contract
    def get_years_passed(self):
        passed = self.get_month()/12
        return passed

    # return monst left in the contract
    def get_months_remaining(self):
        remaining = self.contract.length * 12 - self.get_month()
        return remaining

    # return years left in the contract
    def get_years_remaining(self):
        remaining = self.get_months_remaining() / 12
        return remaining

    # return number of years into contract
    def get_year(self):
        year = floor(self.get_years_passed()) + 1
        return year

    # contract has expired
    def is_expired(self):
        expired = self.get_years_passed() >= self.contract.length
        return expired

    # sum up to nameplate rating of each installed energy server
    def get_system_size(self):
        size = sum(server.nameplate if not server.is_empty() else 0 for server in self.get_servers())
        return size

    # return array of servers
    def get_servers(self):
        servers = [self.servers[server_number] for server_number in self.servers]
        return servers

    # return array of server numbers
    def get_server_numbers(self):
        server_numbers = [server_number for server_number in self.servers]
        return server_numbers

    # add a server with empty enclosures to site
    def add_server(self, server):
        self.servers[server.number] = server

    # current power output of all frus on site
    def get_fru_power(self, lookahead=None):
        fru_power = DataFrame(data=[[enclosure.get_power(lookahead=lookahead) for enclosure in server.enclosures] for server in self.get_servers()],
                              index=self.get_server_numbers())

        return fru_power

    # current overall power output of site
    def get_site_power(self, lookahead=None):
        # find potential power output of frus at each server
        site_power = sum([server.get_power(lookahead=lookahead) for server in self.get_servers()])

        return site_power

    # estimate the remaining energy in all FRUs
    def get_fru_energy(self):
        fru_energy = DataFrame(data=[[enclosure.get_energy() for enclosure in server.enclosures] for server in self.get_servers()],
                               index=self.get_server_numbers())
        
        return fru_energy

    # caculate energy already produced at all servers
    def get_energy_produced(self):
        if self.get_month() > 0:
            ctmo = self.monitor.get_result('performance', 'CTMO', self.get_month() - 1)
        else:
            ctmo = 0
        site_energy = (ctmo * (self.get_month() - 1)) * self.system_size + self.get_site_power()
        return site_energy

    # estimate the remaining energy in all servers
    def get_energy_remaining(self):
        site_energy = sum(server.get_energy(months=self.get_months_remaining()) for server in self.get_servers())
        return site_energy

    # series of server nameplate ratings
    def get_server_nameplates(self):
        server_nameplates = Series([server.nameplate for server in self.get_servers()], index=self.get_server_numbers())
        return server_nameplates

    # current efficiency of all FRUs on site
    def get_fru_efficiency(self):
        fru_efficiency = DataFrame(data=[[enclosure.get_efficiency() for enclosure in server.enclosures] for server in self.get_servers()],
                                   index=self.get_server_numbers())

        return fru_efficiency

    # current overall efficiency output of site
    def get_site_efficiency(self):
        # find potential power output of FRUs at each server
        fru_power = self.get_fru_power()
        site_power = self.get_fru_power().sum().sum()
        
        # find weighted average efficiency
        if (site_power == 0):
            # all FRUs are dead or removed so there is no efficiency
            site_efficiency = 0

        else:
            fru_efficiency = self.get_fru_efficiency()
            site_efficiency = (fru_power * fru_efficiency).sum().sum() / site_power

        return site_efficiency

    # power that is lost due to nameplate capacity per server
    def get_server_ceiling_loss(self):
        server_ceiling_loss = [server.get_ceiling_loss() for server in self.get_servers()]
        return server_ceiling_loss

    # power available due to nameplate capacity per server
    def get_server_headroom(self):    
        server_headroom = [server.get_headroom() for server in self.get_servers()]
        return server_headroom

    # anticipate where modules could go without going over
    def get_max_headroom(self):
        return

    # servers with at least one empty enclosure
    def get_server_has_empty(self):
        server_has_empty = [server.has_empty() for server in self.get_servers()]
        return server_has_empty

    # at least one server with at least one empty enclosure
    def has_empty(self):
        server_has_empty = self.get_server_has_empty()
        site_has_empty = any(server_has_empty)
        return site_has_empty

    # power that is lost due to nameplate capacity for site
    def get_site_ceiling_loss(self):
        site_ceiling_loss = sum(self.get_server_ceiling_loss())
        return site_ceiling_loss

    # add FRUs to site
    def populate(self, new_servers=None, existing_servers=None):
        # servers already exist
        if existing_servers is not None:
            self.populate_existing(existing_servers)
            
        # servers are new
        else:
            self.populate_new(new_servers)

        # prepare log book storage
        self.monitor.set_up(self.servers)

        # set system size
        self.system_size = self.get_system_size() ##contract.target_size

    # add existing FRUs to site
    def populate_existing(self, existing_servers):
        # house existing frus in corresponding servers
        for server_number in existing_servers.get_server_numbers():
            # loop through servers
            server_model = existing_servers[server_number]['model']
            nameplate_needed = existing_servers[server_number]['nameplate']
            n_enclosures = len(existing_servers[server_number]['frus'])
           
            server = self.shop.create_server(self.number, server_number, server_model_class=server_model,
                                             nameplate_needed=nameplate_needed, n_enclosures=n_enclosures)
                
            for fru_number in existing_servers.get_enclosure_numbers(server_number):
                # loop through power modules
                enclosure_number = server.get_empty_enclosure()

                performance = existing_servers[server_number, fru_number]['performance']
                operating_time = existing_servers[server_number, fru_number]['operating time']
                fru_fit = {'performance': performance, 'operating time': operating_time.years + operating_time.months}

                install_date = existing_servers[server_number, fru_number]['install date']
                current_date = install_date + relativedelta(months=len(performance))

                fru_model, fru_mark, fru_model_number =\
                   self.shop.get_latest_model('module', server.model, install_date, match_server_model=True)

                fru = self.shop.create_fru(fru_model, fru_mark, fru_model_number,
                                           install_date, self.number, server_number, enclosure_number,
                                           initial=True, current_date=current_date, fit=fru_fit,
                                           reason='populating enclosure')

                server.replace_fru(enclosure_number, fru)
                
            # add server to site
            self.add_server(server)

    # add new FRUs to site
    def populate_new(self, new_servers):
        # no existing FRUs, start site from scratch

        for server_number in new_servers.get_server_numbers():
            server = self.shop.create_server(self.number, server_number, server_model_number=new_servers[server_number]['model_number'])
            
            for enclosure_number in new_servers.get_enclosure_numbers(server_number):
                fru_model, fru_mark, fru_model_number =\
                   self.shop.get_latest_model('module', server.model, self.get_date(), match_server_model=True)

                fru = self.shop.create_fru(fru_model, fru_mark, fru_model_number,
                                           self.get_date(), self.number, server_number, enclosure_number,
                                           initial=True, reason='populating enclosure')

                server.replace_fru(enclosure_number, fru)

            self.add_server(server)
       
    # return usable FRUs at end of contract
    def decommission(self):
        for server in self.get_servers():
            for enclosure in server.enclosures:
                if enclosure.is_filled():
                    old_fru = self.replace_fru(server.number, enclosure.number, None)
                    deviated = old_fru.is_deviated(self.shop.thresholds['deviated'])
                    self.shop.store_fru(old_fru, self.number, server.number, enclosure.number, final=True, repair=deviated, reason='end of contract')
        return
        
    # move FRUs between enclosures
    def swap_frus(self, server_1, enclosure_1, server_2, enclosure_2):
        # starting ceiling loss
        ceiling_loss_start = self.get_site_ceiling_loss()
        
        # take out first fru
        fru_1 = self.replace_fru(server_1, enclosure_1, None)
        # swap first fru and second fru
        fru_2 = self.replace_fru(server_2, enclosure_2, fru_1)
        # reinstall second fru
        self.replace_fru(server_1, enclosure_1, fru_2)

        # ending ceiling loss
        ceiling_loss_end = self.get_site_ceiling_loss()

        reason = 'minimizing ceiling loss from {:0.1f}kw to {:0.1f}kw'.format(ceiling_loss_start, ceiling_loss_end)

        # record movements
        if fru_1:
            self.shop.balance_frus(fru_1, self.number, server_1, enclosure_1, server_2, enclosure_2, reason=reason)

        if fru_2:
            self.shop.balance_frus(fru_2, self.number, server_2, enclosure_2, server_1, enclosure_1, reason=reason)

        return fru_1, fru_2

    # swap FRU and send old one to shop (if not empty)
    def replace_fru(self, server_number, enclosure_number, fru):
        server = self.servers[server_number]
        old_fru = server.replace_fru(enclosure_number=enclosure_number, fru=fru)

        # check if enclosure rating can handle FRU model
        if (fru is not None) and (fru.get_power() > server.enclosures[enclosure_number].nameplate):
            self.shop.upgrade_enclosures(self.number, server, fru, reason='more power needed than enclosure nameplate limit')
        return old_fru

    # move FRUs around to minimize ceiling loss
    def balance_site(self):
        swaps = Inspector.look_for_balance(self)

        if (not swaps['balanced']) and swaps['balanceable']:
            [(server_1, enclosure_1), (server_2, enclosure_2)] = swaps['balance swap']

            # swap frus
            fru_1, fru_2 = self.swap_frus(server_1, enclosure_1, server_2, enclosure_2)

    def replace_and_balance(self, server_n, enclosure_n, new_fru, reason=None):
        # there is a FRU that meets ceiling loss requirements
        if new_fru is not None:

            # swap out old FRU and store if not empty
            old_fru = self.replace_fru(server_n, enclosure_n, new_fru)
            if old_fru is not None:
                # FRU replaced an existing module
                self.shop.store_fru(old_fru, self.number, server_n, enclosure_n, reason=reason)
            
            # FRU was added to empty enclosure, so check for overloading
            self.balance_site()
        
    # store performance at FRU and site level
    def store_performance(self):
        self.store_fru_performance()
        commitments, fails = self.store_site_performance()
        return commitments, fails
    
    # store cumulative, windowed and instantaneous TMO and efficiency
    def store_site_performance(self):
        if self.limits['window']:
            window_start = max(0, self.get_month() - self.limits['window'])

        self.monitor.store_result('performance', 'year', self.get_month(), self.get_year())

        power = self.get_site_power()
        self.monitor.store_result('performance', 'power', self.get_month(), power)
        self.monitor.store_result('power', 'total', self.get_month(), power)

        ctmo = self.monitor.get_result('performance', 'power', self.get_month(), function='mean') / self.system_size
        self.monitor.store_result('performance', 'CTMO', self.get_month(), ctmo)

        if self.windowed:
            wtmo = self.monitor.get_result('performance', 'power', self.get_month(), start_month=window_start, function='mean') / self.system_size
            self.monitor.store_result('performance', 'WTMO', self.get_month(), wtmo)
        else:
            wtmo = None

        ptmo = power / self.system_size
        self.monitor.store_result('performance', 'PTMO', self.get_month(), ptmo)
        
        efficiency = self.get_site_efficiency()
        fuel = self.monitor.get_result('performance', 'power', self.get_month()) / efficiency if efficiency else 0
        self.monitor.store_result('performance', 'fuel', self.get_month(), fuel)
        self.monitor.store_result('efficiency', 'total', self.get_month(), efficiency)

        total_fuel = self.monitor.get_result('performance', 'fuel', self.get_month(), function='sum')
        ceff = self.monitor.get_result('performance', 'power', self.get_month(), function='sum') / total_fuel if total_fuel else 0
        self.monitor.store_result('performance', 'Ceff', self.get_month(), ceff)

        if self.windowed:
            weff = self.monitor.get_result('performance', 'power', self.get_month(), start_month=window_start, function='sum') \
                / self.monitor.get_result('performance', 'fuel', self.get_month(), start_month=window_start, function='sum')
            self.monitor.store_result('performance', 'Weff', self.get_month(), weff)
        else:
            weff = None

        peff = efficiency
        self.monitor.store_result('performance', 'Peff', self.get_month(), peff)
        
        self.monitor.store_result('performance', 'ceiling loss', self.get_month(), self.get_site_ceiling_loss())

        pairs = [[ctmo, self.limits['CTMO']], [wtmo, self.limits['WTMO']], [ptmo, self.limits['PTMO']],
                 [ceff, self.limits['Ceff']], [weff, self.limits['Weff']], [peff, self.limits['Peff']]]

        ctmo_fail, wtmo_fail, ptmo_fail, ceff_fail, weff_fail, peff_fail = Inspector.check_fails(self, pairs)

        commitments = {'CTMO': ctmo, 'WTMO': wtmo, 'PTMO': ptmo,
                       'Ceff': ceff, 'Weff': weff, 'Peff': peff}
        fails = {'TMO': ctmo_fail | wtmo_fail | ptmo_fail, 'efficiency': ceff_fail | weff_fail,
                 'CTMO': ctmo_fail, 'WTMO': wtmo_fail, 'PTMO': ptmo_fail,
                 'Ceff': ceff_fail, 'Weff': weff_fail, 'Peff': peff}

        return commitments, fails

	# store power and efficiency at each FRU and server
    def store_fru_performance(self):
        for server in self.get_servers():
            for enclosure in server.enclosures:
                if enclosure.is_filled():
                    power = enclosure.fru.get_power()
                    efficiency = enclosure.fru.get_efficiency()
                else:
                    power = nan
                    efficiency = nan

                self.monitor.store_result('power', 'ES{}|ENC{}'.format(server.number, enclosure.number), self.get_month(), power)
                self.monitor.store_result('efficiency', 'ES{}|ENC{}'.format(server.number, enclosure.number), self.get_month(), efficiency)
                
            self.monitor.store_result('power', 'ES{}|='.format(server.number), self.get_month(), server.get_power())
            self.monitor.store_result('power', 'ES{}|-'.format(server.number), self.get_month(), server.get_ceiling_loss())

    # use inspector to check site
    def check_site(self):
        Inspector.check_site(self)

    # degrade each server
    def degrade(self):       
        for server in self.get_servers():
            server.degrade()

        # move to next month
        self.month += 1