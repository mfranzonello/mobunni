#

import pandas

#
class LogBook:
    def __init__(self, site_number, start_date, contract_length):
        date_range = pandas.date_range(start=start_date, periods=contract_length*12, freq='MS')
        self.performance = pandas.DataFrame(columns=['site', 'date', 'year', 'power', 'CTMO', 'WTMO', 'PTMO', 'fuel', 'Ceff', 'Weff', 'Peff', 'ceiling loss'],
                                    index=range(contract_length*12),
                                    data=0)

        self.performance.loc[:, 'site'] = site_number + 1
        self.performance.loc[:, 'date'] = date_range

        self.power = pandas.DataFrame(columns=['date'])
        self.power.loc[:, 'date'] = date_range
        self.efficiency = self.power.copy()

    def get_results(self, value):
        results = {'performance': self.performance,
                   'power': self.power,
                   'efficiency': self.efficiency}[value].copy()
        return results

# 
class Inspector:
    # check if a commitment is missed
    def check_fail(self, value, limit):
        fail = (limit is not None) and (value < limit)
        return fail

    # check if commitments are missed
    def check_fails(self, pairs):
        fails = []
        for value, limit in pairs:
            fails.append(Inspector.check_fail(self, value, limit))

        return fails

    # FRUs that have degraded or are less efficienct
    def get_replaceable_frus(self, by):
        if by in ['power', 'energy']:
            replaceable = [[enclosure.fru.is_degraded(self.shop.thresholds['degraded']) \
                if enclosure.is_filled() else True for enclosure in server.enclosures] for server in self.servers]

        elif by in ['efficiency']:
            replaceable = [[enclosure.fru.is_inefficient(self.shop.thresholds['inefficient']) \
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

    # check if FRUs need to be repaired, replaced or redeployed
    def check_site(self):
        # store current status
        commitments, fails = self.store_performance()

        # check if FRUs can be replaced this year
        if ((self.non_replace is None) or (len(self.non_replace) == 0) or \
            not (self.non_replace[0] <= self.get_year() <= self.non_replace[-1])) and \
            self.get_years_remaining() > self.shop.thresholds['no deploy']:
            
            # check if FRUs need to be repaired
            if self.shop.repair:
                StopWatch.timer('check repairs')
                commitments, fails = self.Inspector.check_repairs(self)
                StopWatch.timer('check repairs')

            # check for early deploy opportunity
            if (self.limits['CTMO'] is not None) and (self.get_years_remaining() <= self.shop.thresholds['early deploy']):
                StopWatch.timer('check early deploy')
                commitments, fails = Inspector.check_deploys(self, commitments)
                StopWatch.timer('check early deploy')

            # check for replaceable FRU
            if fails['TMO'] or fails['efficiency']:
                StopWatch.timer('get worst power FRU')
                server_p, enclosure_p = Inspector.get_worst_fru(self, 'power')
                StopWatch.timer('get worst power FRU')

                StopWatch.timer('get worst efficiency FRU')
                server_e, enclosure_e = Inpsector.get_worst_fru(self, 'efficiency')
                StopWatch.timer('get worst efficiency FRU')

            else:
                server_p = None
                server_e = None

            while ((server_p is not None) and fails['TMO']) or ((server_e is not None) and fails['efficiency']):
                # replace worst FRUs until TMO threshold hit or exhaustion
                StopWatch.timer('solve commitments')
                if (server_p is not None) and fails['TMO']:
                    commitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_tmo(self, commitments, fails, server_p, enclosure_p)

                if (server_e is not None) and fails['efficiency']:
                    commitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_efficiency(self, commitments, server_e, enclosure_e)
                StopWatch.timer('solve commitments')

        return
        
    # look for repair opportunities
    def check_repairs(self):
        for server in self.servers:
            for enclosure in server.enclosures:
                if enclosure.is_filled() and enclosure.fru.is_deviated(self.shop.thresholds['deviated']):
                    # FRU must be repaired
                    # pull the old FRU
                    old_fru = self.replace_fru(server.number, enclosure.number, None)

                    # store the old FRU
                    self.shop.store_fru(old_fru, self.number, server.number, enclosure.number, repair=True)

        commitments, fails = self.store_performance()

        return commitments, fails

    # look for early deploy opportunities
    def check_deploys(self, commitments):
        # estimate final CTMO if FRUs degrade as expected and add FRUs if needed, with padding
        StopWatch.timer('get expected CTMO')
        expected_ctmo = (self.get_energy_produced() + self.get_energy_remaining()) / (self.contract_length * 12) / self.system_size
        StopWatch.timer('get expected CTMO')

        # CHECK PTMO??
        #expected_ptmo = 

        # CHECK IF THERE WILL BE CEILING LOSS

        if Inspector.check_fail(self, expected_ctmo, self.limits['CTMO'] + self.shop.thresholds['ctmo pad']):
            additional_energy = (self.limits['CTMO'] + self.shop.thresholds['ctmo pad']) * self.contract_length * 12 * self.system_size \
                - (self.get_energy_produced() + self.get_energy_remaining())
            
            server_d, enclosure_d = self.get_worst_fru('energy')
            energy_needed = additional_energy - self.servers[server_d].enclosures[enclosure_d].get_energy(months=self.get_months_remaining())
            
            StopWatch.timer('get best fit FRU [early deploy]')
            new_fru = self.shop.get_best_fit_fru(self.server_model, self.get_date(), self.number, server_d, enclosure_d,
                                                 energy_needed=energy_needed, time_needed=self.get_months_remaining())
            StopWatch.timer('get best fit FRU [early deploy]')



            # there is a FRU that meets ceiling loss requirements
            if new_fru is not None:

                # swap out old FRU and store if not empty
                old_fru = self.replace_fru(server_d, enclosure_d, new_fru)
                if old_fru is not None:
                    # FRU replaced an existing module
                    self.shop.store_fru(old_fru, self.number, server_d, enclosure_d)
                else:
                    # FRU was added to empty enclosure, so check for overloading
                    self.balance_site()

        StopWatch.timer('store_performance')
        commitments, fails = self.store_performance()
        StopWatch.timer('store_performance')

        return commitments, fails

    # look for FRU replacements to meet TMO commitments
    def check_tmo(self, commitments, fails, server_p, enclosure_p):
        StopWatch.timer('get power pulled [TMO]')
        power_pulled = self.servers[server_p].enclosures[enclosure_p].fru.get_power() \
            if self.servers[server_p].enclosures[enclosure_p].is_filled() else 0
        StopWatch.timer('get power pulled [TMO]')

        StopWatch.timer('get power needed [TMO]')
        if fails['CTMO']:
            power_needed = ((self.limits['CTMO'] - commitments['CTMO']) * self.system_size + power_pulled) * self.month

        elif fails['WTMO']:
            power_needed = ((self.limits['WTMO'] - commitments['WTMO']) * self.system_size + power_pulled) * min(self.month, self.limits['window'])

        elif fails['PTMO']:
            power_needed = (self.limits['PTMO'] - commitments['PTMO']) * self.system_size + power_pulled
        StopWatch.timer('get power needed [TMO]')

        StopWatch.timer('get best fit [TMO]')
        new_fru = self.shop.get_best_fit_fru(self.server_model, self.get_date(), self.number, server_p, enclosure_p,
                                             power_needed=power_needed)
        StopWatch.timer('get best fit [TMO]')
        
        # swap out old FRU and store if not empty
        old_fru = self.replace_fru(server_p, enclosure_p, new_fru)

        if old_fru is not None:
            # FRU replaced an existing module
            self.shop.store_fru(old_fru, self.number, server_p, enclosure_p)
        else:
            # FRU was added to empty enclosure, so check for overloading
            StopWatch.timer('balance_site [TMO]')
            self.balance_site()
            StopWatch.timer('balance_site [TMO]')

        # find next worst FRU
        StopWatch.timer('check worst FRU [TMO]')
        commmitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_worst_fru(self)
        StopWatch.timer('check worst FRU [TMO]')

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
                new_fru = self.shop.get_best_fit_fru(server.model, self.get_date(), self.number, server_e, enclosure_e,
                                                     power_needed=replacing_fru.rating)
            else:
                # FRU has life left
                new_fru = self.shop.get_best_fit_fru(server.model, self.get_date(), self.number, server_e, enclosure_e,
                                                     power_needed=replacing_fru.get_power(), energy_needed=replacing_fru.get_energy(),
                                                     time_needed=replacing_fru.get_expected_life())
            old_fru = self.replace_fru(server_e, enclosure_e, new_fru)
            # FRU replaced an existing module
            self.shop.store_fru(old_fru, self.number, server_e, enclosure_e)
        else:
            # put in a brand new FRU
            new_fru = self.shop.get_best_fit_fru(server.model, self.get_date(), self.number, server_e, enclosure_e, initial=True)
            self.replace_fru(server_e, enclosure_e, new_fru)
            
            # FRU was added to empty enclosure, so check for overloading
            self.balance_site()

        # find next worst FRU
        commitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_worst_fru(self)

        return commitments, fails, server_p, enclosure_p, server_e, enclosure_e

    # look at TMO and efficiency and find next worst FRU
    def check_worst_fru(self):
        commitments, fails = self.store_performance()
        server_p, enclosure_p = self.get_worst_fru('power')
        server_e, enclosure_e = self.get_worst_fru('efficiency')
        return commitments, fails, server_p, enclosure_p, server_e, enclosure_e



