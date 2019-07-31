# physical sites were energy servers are installed

import pandas
from dateutil.relativedelta import relativedelta
from math import ceil, floor
from inspection import LogBook, Inspector
from structure import StopWatch

# group of energy servers
class Site:    
    def __init__(self, number, shop, contract): 

        self.number = number
        self.shop = shop

        self.target_size = contract.target_size
        self.system_size = 0

        self.start_date = contract.start_date

        self.contract_length = contract.length

        self.log_book = LogBook(self.number, self.start_date, self.contract_length)

        self.limits = contract.limits
        if self.limits['window'] is None:
            self.limits['window'] = 1

        self.server_model = None

        self.non_replace = contract.non_replace

        self.servers = []

        self.start_month = int(contract.start_month)
        self.start_ctmo = contract.start_ctmo

        self.month = int(contract.start_month)

    def __str__(self):
        m_string = max(0, self.month-1)
        month_string = 'Contract month {}'.format(self.month+1)
        ceiling_loss = self.get_site_ceiling_loss()
        ceiling_string = ' | {:0.3f}MW ceiling loss'.format(ceiling_loss/1000) if ceiling_loss > 0 else ''
        site_string = 'SITE: {:0.3f}MW @ {:0.3f}MW | CTMO {:0.1%}, PTMO {:0.1%}, eff {:0.1%}{}'\
            .format(self.system_size/1000, self.performance.loc[m_string, 'power']/1000,
                    self.performance.loc[m_string, 'CTMO'],
                    self.performance.loc[m_string, 'PTMO'],
                    self.performance.loc[m_string, 'cumu eff'],
                    ceiling_string)

        line_string = '='*20
        server_string = '\n'.join([line_string] + [str(server) for server in self.servers] + [line_string])
        string = '{}\n{}'.format(site_string, server_string)
        return string

    # return current date for FRU installation
    def get_date(self):
        date = self.start_date + relativedelta(months=self.month)
        return date

    # return years into the contract
    def get_years_passed(self):
        passed = self.month/12
        return passed

    # return monst left in the contract
    def get_months_remaining(self):
        remaining = self.contract_length * 12 - self.month
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
        expired = self.get_years_passed() >= self.contract_length
        return expired

    # sum up to nameplate rating of each installed energy server
    def get_system_size(self):
        size = sum(server.nameplate if not server.is_empty() else 0 for server in self.servers)
        return size

    # add a server with empty enclosures to site
    def add_server(self, server):
        self.servers.append(server)

    # current power output of all frus on site
    def get_fru_power(self):
        fru_power = pandas.DataFrame(data=[[enclosure.fru.get_power() if enclosure.is_filled() else 0 \
            for enclosure in server.enclosures] for server in self.servers])

        return fru_power

    # current overall power output of site
    def get_site_power(self):
        # find potential power output of frus at each server
        server_power = self.get_fru_power().sum('columns')
        
        # cap server power output at nameplate rating
        server_nameplates = self.get_server_nameplates()
        max_server_power = server_power.where(server_power<server_nameplates, server_nameplates)
        site_power = max_server_power.sum()

        return site_power

    # estimate the remaining energy in all FRUs
    def get_fru_energy(self):
        fru_energy = pandas.DataFrame(data=[[enclosure.fru.get_energy() if enclosure.is_filled() else 0 \
            for enclosure in server.enclosures] for server in self.servers])
        
        return fru_energy

    # caculate energy already produced at all servers
    def get_energy_produced(self):
        ctmo = self.performance.loc[self.month - 1, 'CTMO'] if self.month > 0 else self.start_ctmo
        site_energy = ctmo * self.month * self.system_size 
        return site_energy

    # estimate the remaining energy in all servers
    def get_energy_remaining(self):
        site_energy = sum(server.get_energy(months=self.get_months_remaining()) for server in self.servers)
        return site_energy

    # series of server nameplate ratings
    def get_server_nameplates(self):
        server_nameplates = pandas.Series([server.nameplate for server in self.servers])
        return server_nameplates

    # current efficiency of all FRUs on site
    def get_fru_efficiency(self):
        fru_efficiency = pandas.DataFrame(data=[[enclosure.fru.get_efficiency() if enclosure.is_filled() else 0 \
            for enclosure in server.enclosures] for server in self.servers])

        return fru_efficiency

    # current overall efficiency output of site
    def get_site_efficiency(self):
        # find potential power output of FRUs at each server
        fru_power = self.get_fru_power()
        
        # find weighted average efficiency
        fru_efficiency = self.get_fru_efficiency()

        site_efficiency = (fru_power * fru_efficiency).sum().sum() / fru_power.sum().sum()

        return site_efficiency

    # power that is lost due to nameplate capacity per server
    def get_server_ceiling_loss(self):
        server_ceiling_loss = [server.get_ceiling_loss() for server in self.servers]
        return server_ceiling_loss

    # power available due to nameplate capacity per server
    def get_server_headroom(self):    
        server_headroom = [server.get_headroom() for server in self.servers]
        return server_headroom

    # anticipate where modules could go without going over
    def get_max_headroom(self):
        return

    # servers with at least one empty enclosure
    def get_server_has_empty(self):
        server_has_empty = [server.has_empty() for server in self.servers]
        return server_has_empty

    # power that is lost due to nameplate capacity for site
    def get_site_ceiling_loss(self):
        site_ceiling_loss = sum(self.get_server_ceiling_loss())
        return site_ceiling_loss

    # add FRUs to site
    def populate(self, new_servers=None, existing_servers=None):
        # servers already exist
        if existing_servers is not None: #len(existing_servers.get('df')):
            self.server_model = self.shop.get_server_model(existing_servers['model'])
            self.populate_existing(existing_servers)
            
        # servers are new
        elif new_servers is not None:
            self.server_model = self.shop.get_server_model(new_servers['model'])
            self.populate_new(new_servers)

        # prepare performance storage
        reindex = ['date'] + ['ES{}|{}'.format(s_n, e_n) \
            for s_n in range(len(self.servers)) \
            for e_n in ['ENC{}'.format(f_n) for f_n in range(len(self.servers[s_n].enclosures))] + ['=', '-']]
        self.log_book.power = self.log_book.power.reindex(reindex, axis='columns')
        self.log_book.efficiency = self.log_book.efficiency.reindex(reindex, axis='columns')

    # add existing FRUs to site
    def populate_existing(self, existing_servers):
        # house existing frus in corresponding servers
        df = existing_servers['df']
        server_numbers = df['server #'].unique().tolist()
            
        for server_n in server_numbers:
            # loop through servers
            server_model = existing_servers['model']

            server_details = df[df['server #']==server_n]
            n_enclosures = len(server_details)
            server_number = server_numbers.index(server_n)
            server = self.shop.create_server(self.number, server_number, server_model)
                
            enclosure_number = 0
            for pwm in server_details.index:
                # loop through power modules
                enclosure = self.shop.create_enclosure(self.number, server, enclosure_number)
                server.add_enclosure(enclosure)
                if not (pandas.isnull(server_details.loc[pwm, 'FRU model'])):
                    # install FRU if enclosure not empty
                    fru_model, fru_mark, fru_power, fru_date = server_details.loc[pwm, ['FRU model', 'FRU mark', 'FRU pwr', 'install date']]
                    operating_time = relativedelta(self.get_date(), fru_date)
                    fru_fit = {'operating time': operating_time.years*12 + operating_time.months, 'current power': fru_power}
                    fru = self.shop.create_fru(fru_model, fru_mark, fru_date, self.number, server_number, enclosure_number,
                                                initial=True, fit=fru_fit)

                    server.replace_fru(enclosure_number, fru)
                enclosure_number += 1
                
            # add server to site
            self.add_server(server)
            
        # set system size
        self.system_size = self.target_size

    # add new FRUs to site
    def populate_new(self, new_servers):
        # no existing FRUs, start site from scratch
        # divide power needed by server nameplate to determine number of servers needed
        server_model_number, servers_needed = self.shop.prepare_servers(new_servers, self.target_size)

        # add servers needed to hit target size
        for server_number in range(servers_needed):
            server = self.shop.create_server(self.number, server_number, server_model_number=server_model_number)
            self.add_server(server)

            # add FRUs to hit target power
            while self.servers[server_number].has_empty() and \
                (self.servers[server_number].get_power() < server.nameplate) and (self.get_site_power() < self.target_size):
                enclosure_number = self.servers[server_number].get_empty_enclosure()
                power_needed = self.target_size - self.get_site_power()

                fru = self.shop.get_best_fit_fru(self.server_model, self.get_date(), self.number, server_number, enclosure_number,
                                                 power_needed=power_needed, initial=True)
                self.replace_fru(server_number, enclosure_number, fru)

        self.system_size = self.get_system_size()       
       
    # return usable FRUs at end of contract
    def decommission(self):
        for server in self.servers:
            for enclosure in server.enclosures:
                if enclosure.is_filled():
                    old_fru = self.replace_fru(server.number, enclosure.number, None)
                    deviated = old_fru.is_deviated(self.shop.thresholds['deviated'])
                    self.shop.store_fru(old_fru, self.number, server.number, enclosure.number, final=True, repair=deviated)
        return
        

    # add an enclosure to energy server
    def add_enclosure(self, server_number, plus_one=False):
        server = self.servers[server_number]
        enclosure_number = len(server.enclosures)
        enclosure = self.shop.create_enclosure(self.number, server, enclosure_number, plus_one)
        server.add_enclosure(enclosure)

    # move FRUs between enclosures
    def swap_frus(self, server1, enclosure1, server2, enclosure2):
        # take out first fru
        fru1 = self.replace_fru(server1, enclosure1, None)
        # swap first fru and second fru
        fru2 = self.replace_fru(server2, enclosure2, fru1)
        # reinstall second fru
        self.replace_fru(server1, enclosure1, fru2)

    # swap FRU and send old one to shop (if not empty)
    def replace_fru(self, server_number, enclosure_number, fru):
        old_fru = self.servers[server_number].replace_fru(enclosure_number=enclosure_number, fru=fru)
        return old_fru

    # see if swapping FRUs minimizes ceiling loss
    def is_balanceable(self):
        # calculate ceiling loss, headroom and empty enclosures
        server_ceiling_loss = self.get_server_ceiling_loss()
        server_headroom = self.get_server_headroom()
        server_has_empty = self.get_server_has_empty()
        server_available = [server_headroom[s] if server_has_empty[s] else 0 for s in range(len(server_headroom))]

        # see if any server is overloaded
        max_loss = max(server_ceiling_loss)

        # see if any server has room and an empty slot
        max_room = max(server_available)

        # check if there is potential to minimize ceiling loss
        balanceable = (max_loss > 0) and (max_room > 0)

        if balanceable:
            # start with highest overloaded site
            server_over = server_ceiling_loss.index(max_loss)
            # find the highest underloaded site
            server_under = server_available.index(max_room)
            # take out smallest module that is greater than or equal to ceiling loss and move to server with an empty slot
            fru_power = self.servers[server_over].get_fru_power()
            enclosure_over = fru_power.index(min(fru_power))
            enclosure_under = self.servers[server_under].get_empty_enclosure()

            # check if swapping modules improves ceiling loss
            ceiling_loss_pre = self.get_site_ceiling_loss()
            self.swap_frus(server_over, enclosure_over, server_under, enclosure_under)
            ceiling_loss_post = self.get_site_ceiling_loss()
            self.swap_frus(server_over, enclosure_over, server_under, enclosure_under)
            balanceable = ceiling_loss_pre - ceiling_loss_post > 0

        if not balanceable:
            server_over = None
            enclosure_over = None
            server_under = None
            enclosure_under = None

        return balanceable, server_over, enclosure_over, server_under, enclosure_under

    # move FRUs around to minimize ceiling loss
    def balance_site(self):
        # check balance
        balanceable, server_over, enclosure_over, server_under, enclosure_under = self.is_balanceable()
        
        # if there is a potential for intersite redeploy, move FRUs
        while balanceable:
            # move FRUs to minimize ceiling loss
            fru = self.replace_fru(server_over, enclosure_over, None)
            self.replace_fru(server_under, enclosure_under, fru)
            
            # record transaction
            self.shop.balance_frus(fru, self.number, server_over, enclosure_over, server_under, enclosure_under)

            # see if more swaps can be made
            balanceable, server_over, enclosure_over, server_under, enclosure_under = self.is_balanceable()
        
    # store performance at FRU and site level
    def store_performance(self):
        self.store_fru_performance()
        commitments, fails = self.store_site_performance()
        return commitments, fails
    
    # store cumulative, windowed and instantaneous TMO and efficiency
    def store_site_performance(self):
        self.log_book.performance.loc[self.month, 'year'] = self.get_year()

        power = self.get_site_power()
        self.log_book.performance.loc[self.month, 'power'] = power

        ctmo_adj = self.start_month/(self.month+1)
        ctmo = (self.log_book.performance['power'].loc[:self.month].mean() / self.system_size)*(1-ctmo_adj) + (self.start_ctmo)*ctmo_adj
        self.log_book.performance.loc[self.month, 'CTMO'] = ctmo

        wtmo = self.log_book.performance['power'].loc[max(0,self.month-self.limits['window']):self.month].mean() / self.system_size
        self.log_book.performance.loc[self.month, 'WTMO'] = wtmo

        ptmo = power / self.system_size
        self.log_book.performance.loc[self.month, 'PTMO'] = ptmo
        
        efficiency = self.get_site_efficiency()
        self.log_book.performance.loc[self.month, 'fuel'] = self.log_book.performance.loc[self.month, 'power'] / efficiency

        ceff = self.log_book.performance['power'].loc[:self.month].sum() / self.log_book.performance['fuel'].loc[:self.month].sum()
        self.log_book.performance.loc[self.month, 'Ceff'] = ceff

        weff = self.log_book.performance['power'].loc[max(0,self.month-self.limits['window']):self.month].sum() \
            / self.log_book.performance['fuel'].loc[max(0,self.month-self.limits['window']):self.month].sum()
        self.log_book.performance.loc[self.month, 'Weff'] = weff

        peff = efficiency
        self.log_book.performance.loc[self.month, 'Peff'] = peff
        
        self.log_book.performance.loc[self.month, 'ceiling loss'] = self.get_site_ceiling_loss()

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
        for server in self.servers:
            for enclosure in server.enclosures:
                if enclosure.is_filled():
                    power = enclosure.fru.get_power()
                    efficiency = enclosure.fru.get_efficiency()
                else:
                    power = pandas.np.nan
                    efficiency = pandas.np.nan

                self.log_book.power.loc[self.month, 'ES{}|ENC{}'.format(server.number, enclosure.number)] = power
                self.log_book.efficiency.loc[self.month, 'ES{}|ENC{}'.format(server.number, enclosure.number)] = efficiency
                
            self.log_book.power.loc[self.month, 'ES{}|='.format(server.number)] = server.get_power()
            self.log_book.power.loc[self.month, 'ES{}|-'.format(server.number)] = server.get_ceiling_loss()

    # use inspector to check site
    def check_site(self):
        Inspector.check_site(self)

    # degrade each server
    def degrade(self):       
        for server in self.servers:
            server.degrade()

        # move to next month
        self.month += 1

