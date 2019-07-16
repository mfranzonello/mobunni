# physical sites were energy servers are installed 

import pandas
from dateutil.relativedelta import relativedelta
from math import ceil, floor

# group of energy servers
class Site:    
    def __init__(self, number, shop, target_size, start_date, contract_length, limits, repair, server_model,
                 max_enclosures=6, start_month=0, start_ctmo=1.0, non_replace=None,
                 thresholds={'degraded': 0.0, 'inefficient': 0.0, 'deviated': 0.0, 'early deploy': 1}):

        self.number = number
        self.shop = shop

        self.target_size = target_size
        self.system_size = 0

        self.start_date = start_date

        self.contract_length = contract_length

        date_range = pandas.date_range(start=start_date, periods=contract_length*12, freq='MS')
        self.performance = pandas.DataFrame(columns=['site', 'date', 'year', 'power', 'CTMO', 'WTMO', 'PTMO', 'fuel', 'Ceff', 'Weff', 'Peff', 'ceiling loss'],
                                    index=range(contract_length*12),
                                    data=0)

        self.performance.loc[:, 'site'] = self.number + 1
        self.performance.loc[:, 'date'] = date_range

        self.power = pandas.DataFrame(columns=['date'])
        self.power.loc[:, 'date'] = date_range
        self.efficiency = self.power.copy()

        self.limits = limits
        if self.limits['window'] is None:
            self.limits['window'] = 1

        self.degraded_threshold = thresholds['degraded']
        self.inefficient_threshold = thresholds['inefficient']
        self.deviated_threshold = thresholds['deviated']
        self.early_deploy = thresholds['early deploy']
        self.repair = repair

        self.server_model = server_model
        self.max_enclosures = max_enclosures

        self.non_replace = non_replace

        self.servers = []

        self.start_month = start_month
        self.start_ctmo = start_ctmo

        self.month = start_month

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
    def get_time_passed(self):
        year = self.month/12
        return year

    # return years left in the contract
    def get_time_remaining(self):
        remaining = self.contract_length - self.get_year() - 1
        return remaining

    # return number of years into contract
    def get_year(self):
        year = floor(self.get_time_passed()) + 1
        return year

    # contract has expired
    def is_expired(self):
        expired = self.get_time_passed() >= self.contract_length
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

    # estimate the remaining energy in all servers
    def get_site_energy(self):
        site_energy = sum(server.get_energy() for server in self.servers)
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

    # servers with at least one empty enclosure
    def get_server_has_empty(self):
        server_has_empty = [server.has_empty() for server in self.servers]
        return server_has_empty

    # power that is lost due to nameplate capacity for site
    def get_site_ceiling_loss(self):
        site_ceiling_loss = sum(self.get_server_ceiling_loss())
        return site_ceiling_loss

    # add FRUs to site
    def populate(self, existing_servers, plus_one_empty=False):
        # servers already exist
        if len(existing_servers.get('df')):
            self.populate_existing(existing_servers)

        # servers are new
        else:
            self.populate_new(plus_one_empty)

        # prepare performance storage
        reindex = ['date'] + ['ES{}|{}'.format(s_n, e_n) \
            for s_n in range(len(self.servers)) \
            for e_n in ['ENC{}'.format(f_n) for f_n in range(len(self.servers[s_n].enclosures))] + ['=', '-']]
        self.power = self.power.reindex(reindex, axis='columns')
        self.efficiency = self.efficiency.reindex(reindex, axis='columns')

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
    def populate_new(self, plus_one_empty):
        # no existing FRUs, start site from scratch
        # divide power needed by server nameplate to determine number of servers needed
        server_model_number, servers_needed = self.shop.prepare_servers(self.server_model, self.target_size)
        #servers_needed = ceil(self.target_size / nameplate)

        # add servers needed to hit target size
        for server_number in range(servers_needed):
            server = self.shop.create_server(self.number, server_number, server_model_number=server_model_number)
            for enclosure_number in range(self.max_enclosures):
                plus_one = plus_one_empty and (enclosure_number == self.max_enclosures-1)
                enclosure = self.shop.create_enclosure(self.number, server, enclosure_number, plus_one=plus_one)
                server.add_enclosure(enclosure)
            self.add_server(server)

            # add FRUs to hit target power
            while self.servers[server_number].has_empty() and \
                (self.servers[server_number].get_power() < server.nameplate) and (self.get_site_power() < self.target_size):
                enclosure_number = self.servers[server_number].get_empty_enclosure()
                power_needed = self.target_size - self.get_site_power()
                fru = self.shop.best_fit_fru(self.server_model, self.get_date(), self.number, server_number, enclosure_number,
                                                power_needed=power_needed, initial=True)
                self.replace_fru(server_number, enclosure_number, fru)

        self.system_size = self.get_system_size()       
       
    # return usable FRUs at end of contract
    def decommission(self):
        for server in self.servers:
            for enclosure in server.enclosures:
                if enclosure.is_filled():
                    old_fru = self.replace_fru(server.number, enclosure.number, None)
                    deviated = old_fru.is_deviated(self.deviated_threshold)
                    self.shop.store_fru(old_fru, self.number, server.number, enclosure.number, final=True, repair=deviated)
        return
        
    # FRUs that have degraded or are less efficienct
    def get_replaceable_frus(self, by):
        if by in ['power', 'energy']:
            replaceable = [[enclosure.fru.is_degraded(self.degraded_threshold) \
                if enclosure.is_filled() else True for enclosure in server.enclosures] for server in self.servers]

        elif by in ['efficiency']:
            replaceable = [[enclosure.fru.is_inefficient(self.inefficient_threshold) \
                if enclosure.is_filled() else True for enclosure in server.enclosures] for server in self.servers]

        replaceable_frus = pandas.DataFrame(data=replaceable)

        return replaceable_frus

    # location of the worst performing FRU
    def get_worst_fru(self, by):
        fillable_servers = [s for s in range(len(self.servers)) if self.servers[s].has_empty(dead=True)]

        if len(fillable_servers):
            # if there is an empty slot, pick this first!
            headroom = [self.servers[server].get_headroom() for server in fillable_servers]
            server_number = fillable_servers[headroom.index(max(headroom))]
            enclosure_number = self.servers[server_number].get_empty_enclosure(dead=True)

        else:
            # no empty enclosures

            # ignore FRUs that are too new to be replaced
            replaceable_frus = self.get_replaceable_frus(by)

            if by == 'power':
                # for PTMO failure
                power = self.get_fru_power()
                
                # ignore servers that are at capacity
                server_nameplates = self.get_server_nameplates()
                replaceable_servers = power.where(power.sum('columns') < server_nameplates, float('nan'))
                replaceable_enclosures = replaceable_servers.where(replaceable_frus, float('nan'))

            elif by == 'energy':
                # CTMO or WTMO failure, for early deploy
                energy = self.get_fru_energy()
                replaceable_enclosures = energy.where(replaceable_frus, float('nan'))
                
            elif by == 'efficiency':
                efficiency = self.get_fru_efficiency()
                replaceable_enclosures = efficiency.where(replaceable_frus, float('nan'))

            # pick least well performing FRU
            if replaceable_enclosures.any().any():
                # there is a FRU that can be replaced
                server_number, enclosure_number = replaceable_enclosures.stack().idxmin()
            else:
                # there are no FRUs that can be replaced
                server_number = None   
                enclosure_number = None
                
        return server_number, enclosure_number

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
        
    # check if a commitment is missed
    def check_fail(self, value, limit):
        fail = (limit is not None) and (value < limit)
        return fail

    # store performance at FRU and site level
    def store_performance(self):
        self.store_fru_performance()
        commitments, fails = self.store_site_performance()
        return commitments, fails
    
    # store cumulative, windowed and instantaneous TMO and efficiency
    def store_site_performance(self):
        self.performance.loc[self.month, 'year'] = self.get_year()

        power = self.get_site_power()
        self.performance.loc[self.month, 'power'] = power

        ctmo_adj = self.start_month/(self.month+1)
        ctmo = (self.performance['power'].loc[:self.month].mean() / self.system_size)*(1-ctmo_adj) + (self.start_ctmo)*ctmo_adj
        self.performance.loc[self.month, 'CTMO'] = ctmo

        wtmo = self.performance['power'].loc[max(0,self.month-self.limits['window']):self.month].mean() / self.system_size
        self.performance.loc[self.month, 'WTMO'] = wtmo

        ptmo = power / self.system_size
        self.performance.loc[self.month, 'PTMO'] = ptmo
        
        efficiency = self.get_site_efficiency()
        self.performance.loc[self.month, 'fuel'] = self.performance.loc[self.month, 'power'] / efficiency

        ceff = self.performance['power'].loc[:self.month].sum() / self.performance['fuel'].loc[:self.month].sum()
        self.performance.loc[self.month, 'Ceff'] = ceff

        weff = self.performance['power'].loc[max(0,self.month-self.limits['window']):self.month].sum() \
            / self.performance['fuel'].loc[max(0,self.month-self.limits['window']):self.month].sum()
        self.performance.loc[self.month, 'Weff'] = weff

        peff = efficiency
        self.performance.loc[self.month, 'Peff'] = peff
        
        self.performance.loc[self.month, 'ceiling loss'] = self.get_site_ceiling_loss()

        ctmo_fail = self.check_fail(ctmo, self.limits['CTMO'])
        wtmo_fail = self.check_fail(wtmo, self.limits['WTMO'])
        ptmo_fail = self.check_fail(ptmo, self.limits['PTMO'])
        
        ceff_fail = self.check_fail(ceff, self.limits['Ceff'])
        weff_fail = self.check_fail(weff, self.limits['Weff'])
        peff_fail = self.check_fail(peff, self.limits['Peff'])

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

                self.power.loc[self.month, 'ES{}|ENC{}'.format(server.number, enclosure.number)] = power
                self.efficiency.loc[self.month, 'ES{}|ENC{}'.format(server.number, enclosure.number)] = efficiency
                
            self.power.loc[self.month, 'ES{}|='.format(server.number)] = server.get_power()
            self.power.loc[self.month, 'ES{}|-'.format(server.number)] = server.get_ceiling_loss()

    # check if FRUs need to be repaired, replaced or redeployed
    def check_site(self):
        # store current status
        commitments, fails = self.store_performance()

        # check if FRUs can be replaced this year
        if (self.non_replace is None) or (len(self.non_replace) == 0) or \
            (self.get_year() not in range(self.non_replace[0], self.non_replace[-1]+1)):

            # check if FRUs need to be repaired
            if self.repair:
                commitments, fails = self.check_repairs()

            # check for early deploy opportunity
            if (self.limits['CTMO'] is not None) and (self.early_deploy > self.get_time_remaining()):
                commitments, fails = self.check_deploys(commitments)

            # check for replaceable FRU
            if fails['TMO'] or fails['efficiency']:
                server_p, enclosure_p = self.get_worst_fru('power')
                server_e, enclosure_e = self.get_worst_fru('efficiency')
            else:
                server_p = None
                server_e = None

            while ((server_p is not None) and fails['TMO']) or ((server_e is not None) and fails['efficiency']):
                # replace worst FRUs until TMO threshold hit or exhaustion
                if (server_p is not None) and fails['TMO']:
                    commitments, fails, server_p, enclosure_p, server_e, enclosure_e = self.check_tmo(commitments, fails, server_p, enclosure_p)

                if (server_e is not None) and fails['efficiency']:
                    commitments, fails, server_p, enclosure_p, server_e, enclosure_e = self.check_efficiency(commitments, server_e, enclosure_e)
        return
        
    # look for repair opportunities
    def check_repairs(self):
        for server in self.servers:
            for enclosure in server.enclosures:
                if enclosure.is_filled() and enclosure.fru.is_deviated(self.deviated_threshold):
                    # FRU must be repaired
                    # pull the old FRU
                    old_fru = self.replace_fru(server.number, enclosure.number, None)

                    # store the old FRU
                    self.shop.store_fru(old_fru, self.number, server.number, enclosure.number, repair=True)

        commitments, fails = self.store_performance()

        return commitments, fails

    # look for early deploy opportunities
    def check_deploys(self, commitments):
        # estimate final CTMO if FRUs degrade as expected and add FRUs if needed
        expected_ctmo = (commitments['CTMO']*self.get_time_passed() + self.get_site_energy())/self.contract_length
        if self.check_fail(expected_ctmo, self.limits['CTMO']):
            additional_energy = self.limits['CTMO'] * self.contract_length - (commitments['CTMO']*self.get_time_passed() + self.get_site_energy())
            server_d, enclosure_d = self.get_worst_fru('energy')
            energy_needed = additional_energy - self.servers[server_d].enclosures[enclosure_d].get_energy()
            
            new_fru = self.shop.best_fit_fru(self.server_model, self.get_date(), self.number, server_d, enclosure_d,
                                             energy_needed=energy_needed, time_needed=self.get_time_remaining())

            # swap out old FRU and store if not empty
            old_fru = self.replace_fru(server_d, enclosure_d, new_fru)
            if old_fru is not None:
                # FRU replaced an existing module
                self.shop.store_fru(old_fru, self.number, server_d, enclosure_d)
            else:
                # FRU was added to empty enclosure, so check for overloading
                self.balance_site()

        commitments, fails = self.store_performance()

        return commitments, fails

    # look for FRU replacements to meet TMO commitments
    def check_tmo(self, commitments, fails, server_p, enclosure_p):
        power_pulled = self.servers[server_p].enclosures[enclosure_p].fru.get_power() \
            if self.servers[server_p].enclosures[enclosure_p].is_filled() else 0
        
        if fails['CTMO']:
            power_needed = ((self.limits['CTMO'] - commitments['CTMO']) * self.system_size + power_pulled) * self.month

        elif fails['WTMO']:
            power_needed = ((self.limits['WTMO'] - commitments['WTMO']) * self.system_size + power_pulled) * min(self.month, self.limits['window'])

        elif fails['PTMO']:
            power_needed = (self.limits['PTMO'] - commitments['PTMO']) * self.system_size + power_pulled

        new_fru = self.shop.best_fit_fru(self.server_model, self.get_date(), self.number, server_p, enclosure_p,
                                         power_needed=power_needed)
                    
        # swap out old FRU and store if not empty
        old_fru = self.replace_fru(server_p, enclosure_p, new_fru)

        if old_fru is not None:
            # FRU replaced an existing module
            self.shop.store_fru(old_fru, self.number, server_p, enclosure_p)
        else:
            # FRU was added to empty enclosure, so check for overloading
            self.balance_site()

        # find next worst FRU
        commmitments, fails, server_p, enclosure_p, server_e, enclosure_e = self.check_worst_fru()

        return commmitments, fails, server_p, enclosure_p, server_e, enclosure_e

    # look for FRUs replacements to meet efficiency commitment
    def check_efficiency(self, commitments, server_e, enclosure_e):
        # match power, energy and remaining life of replacing FRU
        server = self.servers[server_e]
        if server.enclosures[enclosure_e].is_filled():
            # replace an inefficient FRU with a similar model
            replacing_fru = server.enclosures[enclosure_e].fru

            if replacing_fru.is_dead():
                # FRU is already dead
                # replace with original FRU rating
                new_fru = self.shop.best_fit_fru(server.model, self.get_date(), self.number, server_e, enclosure_e,
                                                 power_needed=replacing_fru.rating)
            else:
                # FRU has life left
                new_fru = self.shop.best_fit_fru(server.model, self.get_date(), self.number, server_e, enclosure_e,
                             power_needed=replacing_fru.get_power(), energy_needed=replacing_fru.get_energy(), time_needed=replacing_fru.get_expected_life())
            old_fru = self.replace_fru(server_e, enclosure_e, new_fru)
            # FRU replaced an existing module
            self.shop.store_fru(old_fru, self.number, server_e, enclosure_e)
        else:
            # put in a brand new FRU
            new_fru = self.shop.best_fit_fru(server.model, self.get_date(), self.number, server_e, enclosure_e)
            self.replace_fru(server_e, enclosure_e, new_fru)
            
            # FRU was added to empty enclosure, so check for overloading
            self.balance_site()

        # find next worst FRU
        commitments, fails, server_p, enclosure_p, server_e, enclosure_e = self.check_worst_fru()

        return commitments, fails, server_p, enclosure_p, server_e, enclosure_e

    # look at TMO and efficiency and find next worst FRU
    def check_worst_fru(self):
        commitments, fails = self.store_performance()
        server_p, enclosure_p = self.get_worst_fru('power')
        server_e, enclosure_e = self.get_worst_fru('efficiency')
        return commitments, fails, server_p, enclosure_p, server_e, enclosure_e

    # degrade each server
    def degrade(self):       
        for server in self.servers:
            server.degrade()

        # move to next month
        self.month += 1