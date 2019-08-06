# tools to record and check if sites are performing to contract specifications

from pandas import DataFrame, date_range

from debugging import StopWatch

# performance, power and efficiency of a site
class Monitor:
    def __init__(self, site_number, start_date, contract_length, start_ctmo=1.0, start_eff=1.0):
        self._starting_cumulative = {'tmo': start_ctmo,
                                     'efficiency': start_eff}

        contract_date_range = date_range(start=start_date, periods=contract_length*12, freq='MS')
        self._performance = DataFrame(columns=['site', 'date', 'year',
                                              'power', 'CTMO', 'WTMO', 'PTMO',
                                              'fuel', 'Ceff', 'Weff', 'Peff',
                                              'ceiling loss'],
                                    index=range(contract_length*12),
                                    data=0)

        self._performance.loc[:, 'site'] = site_number + 1
        self._performance.loc[:, 'date'] = contract_date_range

        self._power = DataFrame(columns=['date'])
        self._power.loc[:, 'date'] = contract_date_range
        self._efficiency = self._power.copy()

    def set_up(self, servers):
        reindex = ['date'] + ['ES{}|{}'.format(s_n, e_n) \
            for s_n in range(len(servers)) \
            for e_n in ['ENC{}'.format(f_n) for f_n in range(len(servers[s_n].enclosures))] + ['=', '-']]
        self._power = self._power.reindex(reindex, axis='columns')
        self._efficiency = self._efficiency.reindex(reindex, axis='columns')

    def get_starting_cumulative(self, table):
        value = self._starting_cumulative[table]
        return value

    def store_result(self, table, column, month, value):
        {'performance': self._performance,
         'power': self._power,
         'efficiency': self._efficiency}[table].loc[month, column] = value

    def get_result(self, table, column, month, start_month=0, function=None):
        df = {'performance': self._performance,
              'power': self._power,
              'efficiency': self._efficiency}[table]
        
        if function is None:
            result = df.loc[month, column]

        else:
            partial_result = df.loc[start_month:month, column]
            if function == 'mean':
                result = partial_result.mean()
            elif function == 'sum':
                result = partial_result.sum()

        return result

    def get_results(self, table):
        results = {'performance': self._performance,
                   'power': self._power,
                   'efficiency': self._efficiency}[table].copy()
        return results

# methods to see if a site is performing according to contract
class Inspector:
    # see if swapping FRUs minimizes ceiling loss
    def is_balanceable(site):
        # calculate ceiling loss, headroom and empty enclosures
        server_ceiling_loss = site.get_server_ceiling_loss()
        server_headroom = site.get_server_headroom()
        server_has_empty = site.get_server_has_empty()
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
            fru_power = site.servers[server_over].get_fru_power()
            enclosure_over = fru_power.index(min(fru_power))
            enclosure_under = site.servers[server_under].get_empty_enclosure()

            # check if swapping modules improves ceiling loss
            ceiling_loss_pre = site.get_site_ceiling_loss()
            site.swap_frus(server_over, enclosure_over, server_under, enclosure_under)
            ceiling_loss_post = site.get_site_ceiling_loss()
            site.swap_frus(server_over, enclosure_over, server_under, enclosure_under)
            balanceable = ceiling_loss_pre - ceiling_loss_post > 0

        if not balanceable:
            server_over = None
            enclosure_over = None
            server_under = None
            enclosure_under = None

        return balanceable, server_over, enclosure_over, server_under, enclosure_under

    # check if a commitment is missed
    def check_fail(site, value, limit):
        fail = (limit is not None) and (value < limit)
        return fail

    # check if commitments are missed
    def check_fails(site, pairs):
        fails = []
        for value, limit in pairs:
            fails.append(Inspector.check_fail(site, value, limit))

        return fails

    # FRUs that have degraded or are less efficienct
    def get_replaceable_frus(site, by):
        if by in ['power', 'energy']:
            replaceable = [[enclosure.fru.is_degraded(site.shop.thresholds['degraded']) \
                if enclosure.is_filled() else True for enclosure in server.enclosures] for server in site.servers]

        elif by in ['efficiency']:
            replaceable = [[enclosure.fru.is_inefficient(site.shop.thresholds['inefficient']) \
                if enclosure.is_filled() else True for enclosure in server.enclosures] for server in site.servers]

        replaceable_frus = DataFrame(data=replaceable)

        return replaceable_frus

    # location of the worst performing FRU
    def get_worst_fru(site, by):
        fillable_servers = [s for s in range(len(site.servers)) if site.servers[s].has_empty(dead=True)]

        if len(fillable_servers):
            # if there is an empty slot, pick this first!
            headroom = [site.servers[server].get_headroom() for server in fillable_servers]
            server_number = fillable_servers[headroom.index(max(headroom))]
            enclosure_number = site.servers[server_number].get_empty_enclosure(dead=True)

        else:
            # no empty enclosures

            # ignore FRUs that are too new to be replaced
            replaceable_frus = Inspector.get_replaceable_frus(site, by)

            if by == 'power':
                # for PTMO failure
                power = site.get_fru_power()
                
                # ignore servers that are at capacity
                server_nameplates = site.get_server_nameplates()
                replaceable_servers = power.where(power.sum('columns') < server_nameplates, float('nan'))
                replaceable_enclosures = replaceable_servers.where(replaceable_frus, float('nan'))

            elif by == 'energy':
                # CTMO or WTMO failure, for early deploy
                energy = site.get_fru_energy()
                replaceable_enclosures = energy.where(replaceable_frus, float('nan'))
                
            elif by == 'efficiency':
                efficiency = site.get_fru_efficiency()
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
    def check_site(site):
        # store current status
        commitments, fails = site.store_performance()

        # check if FRUs can be replaced this year
        if site.contract.is_replaceable_year(site.get_year()):
            early_deploys = (site.get_years_remaining() > site.shop.thresholds['no deploy']) ###
            
            # check if FRUs need to be repaired
            if site.shop.repair:
                StopWatch.timer('check repairs')
                commitments, fails = site.Inspector.check_repairs(site)
                StopWatch.timer('check repairs')

            # check for early deploy opportunity
            if (site.limits['CTMO'] is not None) and (site.get_years_remaining() <= site.shop.thresholds['early deploy']):
                StopWatch.timer('check early deploy')
                commitments, fails = Inspector.check_deploys(site, commitments)
                StopWatch.timer('check early deploy')

            # check for replaceable FRU
            if fails['TMO'] or fails['efficiency']:
                StopWatch.timer('get worst power FRU')
                server_p, enclosure_p = Inspector.get_worst_fru(site, 'power')
                StopWatch.timer('get worst power FRU')

                StopWatch.timer('get worst efficiency FRU')
                server_e, enclosure_e = Inspector.get_worst_fru(site, 'efficiency')
                StopWatch.timer('get worst efficiency FRU')

            else:
                server_p = None
                server_e = None

            while ((server_p is not None) and fails['TMO']) or ((server_e is not None) and fails['efficiency']):
                # replace worst FRUs until TMO threshold hit or exhaustion
                StopWatch.timer('solve commitments')
                if (server_p is not None) and fails['TMO']:
                    commitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_tmo(site, commitments, fails, server_p, enclosure_p)

                if (server_e is not None) and fails['efficiency']:
                    commitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_efficiency(site, commitments, server_e, enclosure_e)
                StopWatch.timer('solve commitments')

        return
        
    # look for repair opportunities
    def check_repairs(site):
        for server in site.servers:
            for enclosure in server.enclosures:
                if enclosure.is_filled() and enclosure.fru.is_deviated(site.shop.thresholds['deviated']):
                    # FRU must be repaired
                    # pull the old FRU
                    old_fru = site.replace_fru(server.number, enclosure.number, None)

                    # store the old FRU
                    site.shop.store_fru(old_fru, site.number, server.number, enclosure.number, repair=True)

        commitments, fails = site.store_performance()

        return commitments, fails

    # look for early deploy opportunities
    def check_deploys(site, commitments):
        # estimate final CTMO if FRUs degrade as expected and add FRUs if needed, with padding
        StopWatch.timer('get expected CTMO')
        expected_ctmo = (site.get_energy_produced() + site.get_energy_remaining()) / (site.contract.length * 12) / site.system_size
        StopWatch.timer('get expected CTMO')

        # CHECK PTMO??
        #expected_ptmo = 

        # CHECK IF THERE WILL BE CEILING LOSS

        if Inspector.check_fail(site, expected_ctmo, site.limits['CTMO'] + site.shop.thresholds['ctmo pad']):
            additional_energy = (site.limits['CTMO'] + site.shop.thresholds['ctmo pad']) * site.contract.length * 12 * site.system_size \
                - (site.get_energy_produced() + site.get_energy_remaining())
            
            server_d, enclosure_d = Inspector.get_worst_fru(site, 'energy')
            energy_needed = additional_energy - site.servers[server_d].enclosures[enclosure_d].get_energy(months=site.get_months_remaining())
            
            StopWatch.timer('get best fit FRU [early deploy]')
            new_fru = site.shop.get_best_fit_fru(site.server_model, site.get_date(), site.number, server_d, enclosure_d,
                                                 energy_needed=energy_needed, time_needed=site.get_months_remaining())
            StopWatch.timer('get best fit FRU [early deploy]')



            # there is a FRU that meets ceiling loss requirements
            if new_fru is not None:

                # swap out old FRU and store if not empty
                old_fru = site.replace_fru(server_d, enclosure_d, new_fru)
                if old_fru is not None:
                    # FRU replaced an existing module
                    site.shop.store_fru(old_fru, site.number, server_d, enclosure_d)
                else:
                    # FRU was added to empty enclosure, so check for overloading
                    site.balance_site()

        StopWatch.timer('store_performance')
        commitments, fails = site.store_performance()
        StopWatch.timer('store_performance')

        return commitments, fails

    # look for FRU replacements to meet TMO commitments
    def check_tmo(site, commitments, fails, server_p, enclosure_p):
        StopWatch.timer('get power pulled [TMO]')
        power_pulled = site.servers[server_p].enclosures[enclosure_p].fru.get_power() \
            if site.servers[server_p].enclosures[enclosure_p].is_filled() else 0
        StopWatch.timer('get power pulled [TMO]')

        StopWatch.timer('get power needed [TMO]')
        if fails['CTMO']:
            power_needed = ((site.limits['CTMO'] - commitments['CTMO']) * site.system_size + power_pulled) * site.month

        elif fails['WTMO']:
            power_needed = ((site.limits['WTMO'] - commitments['WTMO']) * site.system_size + power_pulled) * min(site.month, site.limits['window'])

        elif fails['PTMO']:
            power_needed = (site.limits['PTMO'] - commitments['PTMO']) * site.system_size + power_pulled
        StopWatch.timer('get power needed [TMO]')

        StopWatch.timer('get best fit [TMO]')
        new_fru = site.shop.get_best_fit_fru(site.server_model, site.get_date(), site.number, server_p, enclosure_p,
                                             power_needed=power_needed)
        StopWatch.timer('get best fit [TMO]')
        
        # swap out old FRU and store if not empty
        old_fru = site.replace_fru(server_p, enclosure_p, new_fru)

        if old_fru is not None:
            # FRU replaced an existing module
            site.shop.store_fru(old_fru, site.number, server_p, enclosure_p)
        else:
            # FRU was added to empty enclosure, so check for overloading
            StopWatch.timer('balance_site [TMO]')
            site.balance_site()
            StopWatch.timer('balance_site [TMO]')

        # find next worst FRU
        StopWatch.timer('check worst FRU [TMO]')
        commmitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_worst_fru(site)
        StopWatch.timer('check worst FRU [TMO]')

        return commmitments, fails, server_p, enclosure_p, server_e, enclosure_e

    # look for FRUs replacements to meet efficiency commitment
    def check_efficiency(site, commitments, server_e, enclosure_e):
        # match power, energy and remaining life of replacing FRU
        server = site.servers[server_e]
        if server.enclosures[enclosure_e].is_filled():
            # replace an inefficient FRU with a similar model
            replacing_fru = server.enclosures[enclosure_e].fru

            if replacing_fru.is_dead():
                # FRU is already dead
                # replace with original FRU rating
                new_fru = site.shop.get_best_fit_fru(server.model, site.get_date(), site.number, server_e, enclosure_e,
                                                     power_needed=replacing_fru.rating)
            else:
                # FRU has life left
                new_fru = site.shop.get_best_fit_fru(server.model, site.get_date(), site.number, server_e, enclosure_e,
                                                     power_needed=replacing_fru.get_power(), energy_needed=replacing_fru.get_energy(),
                                                     time_needed=replacing_fru.get_expected_life())
            old_fru = site.replace_fru(server_e, enclosure_e, new_fru)
            # FRU replaced an existing module
            site.shop.store_fru(old_fru, site.number, server_e, enclosure_e)
        else:
            # put in a brand new FRU
            new_fru = site.shop.get_best_fit_fru(server.model, site.get_date(), site.number, server_e, enclosure_e, initial=True)
            site.replace_fru(server_e, enclosure_e, new_fru)
            
            # FRU was added to empty enclosure, so check for overloading
            site.balance_site()

        # find next worst FRU
        commitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_worst_fru(site)

        return commitments, fails, server_p, enclosure_p, server_e, enclosure_e

    # look at TMO and efficiency and find next worst FRU
    def check_worst_fru(site):
        commitments, fails = site.store_performance()
        server_p, enclosure_p = Inspector.get_worst_fru(site, 'power')
        server_e, enclosure_e = Inspector.get_worst_fru(site, 'efficiency')
        return commitments, fails, server_p, enclosure_p, server_e, enclosure_e