# tools to record and check if sites are performing to contract specifications

from pandas import DataFrame, Series, date_range
from numpy import nan

from debugging import StopWatch

# performance, power and efficiency of a site
class Monitor:
    performance_columns = ['site', 'date', 'year',
                           'power', 'CTMO', 'WTMO', 'PTMO',
                           'fuel', 'Ceff', 'Weff', 'Peff',
                           'ceiling loss']
    power_eff_columns = ['date'] + ['expected PTMO', 'expected CTMO'] + ['total']

    def __init__(self, site_number, start_date, contract_length, windowed, start_ctmo=1.0, start_eff=1.0):
        self._starting_cumulative = {'tmo': start_ctmo,
                                     'efficiency': start_eff}

        self.contract_date_range = date_range(start=start_date, periods=contract_length*12, freq='MS').date
        self._performance = DataFrame(columns=Monitor.performance_columns,
                                    index=range(contract_length*12),
                                    data=0)

        if not windowed:
            self._performance.drop(['WTMO', 'Weff'], axis='columns', inplace=True)

        self._performance.loc[:, 'site'] = site_number + 1
        self._performance.loc[:, 'date'] = self.contract_date_range

        self._power = None
        self._efficiency = None

    # set up matrix for power and efficiency output
    def set_up(self, servers):
        ceiling = ['=', '-']

        power_eff = DataFrame(columns=['date'])
        power_eff.loc[:, 'date'] = self.contract_date_range

        reindex = Monitor.power_eff_columns + ['ES{}|{}'.format(s_n, e_n) \
            for s_n in range(len(servers)) \
            for e_n in ['ENC{}'.format(f_n) for f_n in range(len(servers[s_n].enclosures))] + ceiling]

        drop = ['ES{}|{}'.format(s_n, e_n) for s_n in range(len(servers)) for e_n in ceiling]

        self._power = power_eff.reindex(reindex, axis='columns')
        self._efficiency = self._power.drop(drop, axis='columns')

    # return the starting cumulative TMO and cumulative efficiency
    def get_starting_cumulative(self, table):
        value = self._starting_cumulative[table]
        return value

    # add a result from a site inspection
    def store_result(self, table, column, month, value):
        {'performance': self._performance,
         'power': self._power,
         'efficiency': self._efficiency}[table].loc[month, column] = value

    # return a result for a site inspection
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

    # return a set of results
    def get_results(self, table):
        results = {'performance': self._performance,
                   'power': self._power,
                   'efficiency': self._efficiency}[table].copy()
        return results

# methods to see if a site is performing according to contract
class Inspector:
    # see if swapping FRUs minimizes ceiling loss
    def look_for_balance(site):
        nameplates = Series([server.nameplate for server in site.servers])
        fru_powers = site.get_fru_power()
        server_powers = fru_powers.sum(axis='columns')

        headroom = nameplates - server_powers
        plus_ones = fru_powers.min('columns') == 0
        initial_headroom = headroom.where(plus_ones).max()
        initial_ceiling_loss = (server_powers - nameplates).where(server_powers > nameplates).sum()
       
        max_ceiling_loss = site.shop.thresholds.get('ceiling loss', 0)

        is_balanceable = False

        swaps = {'balanced': (headroom >= max_ceiling_loss).all(),
                 'balanceable': is_balanceable,
                 'balance swap': None,
                 'headroom swap': None,
                 'max headroom': initial_headroom}

        servers = fru_powers.index
        enclosures = fru_powers.columns

        for server_1 in servers: ## probably a faster way to exit -- or search in descending order
            for enclosure_1 in enclosures:
                improvements = fru_powers.loc[server_1, enclosure_1] - fru_powers
                potentials = improvements.where(improvements > 0).where(improvements < headroom)
                potentials.loc[server_1, :] = nan

                potentials_unstacked = potentials.unstack().isnull()

                if potentials_unstacked.isnull().all():
                    enclosure_2, server_2 = [None, None]
                else:
                    enclosure_2, server_2 = potentials_unstacked.idxmax()

                    frus_swapped = fru_powers.copy()
                    fru_power_1 = frus_swapped.loc[server_1, enclosure_1]
                    fru_power_2 = frus_swapped.loc[server_2, enclosure_2]
                    frus_swapped.loc[server_1, enclosure_1] = fru_power_2
                    frus_swapped.loc[server_2, enclosure_2] = fru_power_1

                    final_server_powers = frus_swapped.sum(axis='columns')
                    final_headroom = nameplates - final_server_powers
                    final_ceiling_loss = (final_server_powers - nameplates).where(final_server_powers > nameplates).sum()

                    plus_ones = frus_swapped.min('columns') == 0
                    max_headroom = final_headroom.where(plus_ones).max()

                    is_balanceable = final_ceiling_loss < initial_ceiling_loss
                    if (not swaps['balanced']) and is_balanceable:
                        swaps['balanceable'] = is_balanceable
                        swaps['balance swap'] = [(server_1, enclosure_1), (server_2, enclosure_2)]
                        initial_ceiling_loss = final_ceiling_loss

                    headroom_improvement = (max_headroom > swaps['max headroom'])
                    no_additional_loss = (final_headroom.where(final_headroom < max_ceiling_loss, 0) >= headroom.where(headroom < max_ceiling_loss, 0)).all()
                    if headroom_improvement and no_additional_loss:
                        swaps['headroom swap'] = [(server_1, enclosure_1), (server_2, enclosure_2)]
                        swaps['max headroom'] = max_headroom

        return swaps

    # check if a commitment is missed
    def check_fail(site, value, limit, pad=0):
        fail = (limit is not None) and (value < limit + pad)
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
                ##print('ENERGY REPLACEABLES')
                ##print(replaceable_enclosures)
                
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
        replaceable = site.contract.is_replaceable_year(site.get_year())
        early_replaceable = site.get_years_remaining() > site.shop.thresholds['no deploy']
        last_replaceable = site.get_months_remaining() == site.shop.thresholds['no deploy']*12 + 1
        if replaceable and early_replaceable:
            # check if FRUs need to be repaired
            if site.shop.repair:
                commitments, fails = site.Inspector.check_repairs(site)

            # check for early deploy opportunity
            if site.shop.early_deploy:
                commitments, fails = Inspector.check_deploys(site, commitments, cumulative=early_replaceable, periodic=last_replaceable)

            # check for replaceable FRU
            if fails['TMO'] or fails['efficiency']:
                server_p, enclosure_p = Inspector.get_worst_fru(site, 'power')
                server_e, enclosure_e = Inspector.get_worst_fru(site, 'efficiency')

            else:
                server_p = None
                server_e = None

            while (Inspector.check_exists(server_p) and fails['TMO']) or (Inspector.check_exists(server_e) and fails['efficiency']):
                # replace worst FRUs until TMO threshold hit or exhaustion
                if Inspector.check_exists(server_p, enclosure_p) and fails['TMO']:
                    commitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_tmo(site, commitments, fails, server_p, enclosure_p)

                if Inspector.check_exists(server_e, enclosure_e) and fails['efficiency']:
                    commitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_efficiency(site, commitments, server_e, enclosure_e)

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

    # check how much power can be installed without ceiling loss for plus ones and if a new FRU exists
    def check_max_power(site, new_fru=False):
        # ensure no ceiling loss if there are plus ones
        max_power = Inspector.look_for_balance(site).get('max headroom') if site.has_empty() else None
        installable = (new_fru is not None) and ((max_power is None) or (max_power > 0))

        return max_power, installable

    # look for early deploy opportunities
    def check_deploys(site, commitments, cumulative=False, periodic=False):
        lookahead = site.get_months_remaining()

        #print()
        #print('LAST PTMO: {:0.2%}'.format(site.monitor.get_result('performance', 'PTMO', site.month)))
        #for m in range(lookahead):
        #    print('NEXT PTMO ({}): {:0.2%}'.format(m+1, site.get_site_power(lookahead=m+1) / site.system_size))

        # check during any period before no deployments allowed
        if cumulative:
            # estimate final CTMO if FRUs degrade as expected and add FRUs if needed, with padding

            ##StopWatch.timer('get expected CTMO')
            expected_ctmo = (site.get_energy_produced() + site.get_energy_remaining()) / (site.contract.length * 12) / site.system_size
            server_dc, enclosure_dc = Inspector.get_worst_fru(site, 'energy')
            max_power, installable = Inspector.check_max_power(site)
            ##StopWatch.timer('get expected CTMO')

            while installable and \
                Inspector.check_fail(site, expected_ctmo, site.limits['CTMO'], pad=site.shop.thresholds['tmo pad']) and \
                Inspector.check_exists(server_dc, enclosure_dc):
                
                ##print()
                ##print(Inspector.get_replaceable_frus(site, by='energy'))
                ##print('max power: {}, installable: {}, expected ctmo: {}, server_dc, enclosure_dc: {}'.format(max_power, installable, expected_ctmo, [server_dc, enclosure_dc]))

                additional_energy = (site.limits['CTMO'] + site.shop.thresholds['tmo pad']) * site.contract.length * 12 * site.system_size \
                    - (site.get_energy_produced() + site.get_energy_remaining())
            
                # there is an empty enclosure or a FRU can be replaced
                energy_pulled = site.servers[server_dc].enclosures[enclosure_dc].get_energy(months=lookahead)
                energy_needed = additional_energy - energy_pulled
           
                StopWatch.timer('get best fit FRU [early deploy]')
                reason = 'early deploy: expected CTMO {:0.02%} below target {:0.02%}'.format(expected_ctmo, site.limits['CTMO'] + site.shop.thresholds['tmo pad'])
                new_fru = site.shop.get_best_fit_fru(site.server_model, site.get_date(), site.number, server_dc, enclosure_dc,
                                                        energy_needed=energy_needed, time_needed=lookahead, max_power=max_power, reason=reason)
                StopWatch.timer('get best fit FRU [early deploy]')

                site.replace_and_balance(server_dc, enclosure_dc, new_fru, reason=reason)
                expected_ctmo = (site.get_energy_produced() + site.get_energy_remaining()) / (site.contract.length * 12) / site.system_size
                server_dc, enclosure_dc = Inspector.get_worst_fru(site, 'energy')
                max_power, installable = Inspector.check_max_power(site, new_fru=new_fru)

                ##print(Inspector.get_replaceable_frus(site, by='energy'))
                ##print('max power: {}, installable: {}, expected ctmo: {}, server_dc, enclosure_dc: {}'.format(max_power, installable, expected_ctmo, [server_dc, enclosure_dc]))

                site.monitor.store_result('power', 'expected CTMO', site.get_month(), expected_ctmo)

            ## estimate final cumulative efficiency if FRUs degrade as expected and add FRUs if needed, with padding
            #expected_ceff = 0
            #if Inspector.check_fail(site, expected_ceff, site.limits['Ceff'], pad=site.shop.thresholds['eff pad']):
            #    additional_efficiency = site.limits['Ceff'] - expected_ceff
            
            #    server_de, enclosure_de = Inspector.get_worst_fru(site, 'efficiency')

            #    if (server_de is not None) and (enclosure_de is not None):
            #        # there is an empty enclosure or a FRU can be replaced
            #        energy_pulled = site.servers[server_dc].enclosures[enclosure_dc].get_energy(months=lookahead)
            #        energy_needed = additional_energy - energy_pulled
            
            #        reason = 'early deploy: expected Ceff {:0.02%} below target {:0.02%}'.format(expected_ceff, site.limits['Ceff'] + site.shop.thresholds['eff pad'])
            #        new_fru = site.shop.get_best_fit_fru(site.server_model, site.get_date(), site.number, server_dc, enclosure_dc,
            #                                             efficiency_needed=energy_needed, time_needed=lookahead, max_power=max_power, reason=reason)

            #        site.replace_and_balance(server_de, enclosure_de, new_fru, reason=reason)
            #        expected_ceff = 0
            #        server_de, enclosure_de = Inspector.get_worst_fru(site, 'efficiency')
            #        max_power, installable = Inspector.check_max_power(site, new_fru=new_fru)

        # check during last periods before no deployments allowed
        if periodic:
            # estimate final PTMO if FRUs degrade as expected and add FRUs if needed, with padding
            expected_ptmo = site.get_site_power(lookahead=lookahead) / site.system_size
            server_dp, enclosure_dp = Inspector.get_worst_fru(site, 'power')
            max_power, installable = Inspector.check_max_power(site)

            while installable and \
                Inspector.check_fail(site, expected_ptmo, site.limits['PTMO'], pad=site.shop.thresholds['tmo pad']) and \
                Inspector.check_exists(server_dp, enclosure_dp):

                ##print()
                ##print(Inspector.get_replaceable_frus(site, by='power'))
                ##print('max power: {}, installable: {}, expected ptmo: {}, server_dp, enclosure_dp: {}'.format(max_power, installable, expected_ptmo, [server_dp, enclosure_dp]))

                additional_power = (site.limits['PTMO'] + site.shop.thresholds['tmo pad'] - expected_ptmo) * site.system_size
                   
                power_pulled = site.servers[server_dp].enclosures[enclosure_dp].get_power(lookahead=lookahead)
                power_needed = additional_power + power_pulled

                reason = 'early deploy: expected PTMO {:0.02%} below target {:0.02%}'.format(expected_ptmo, site.limits['PTMO'] + site.shop.thresholds['tmo pad'])
                new_fru = site.shop.get_best_fit_fru(site.server_model, site.get_date(), site.number, server_dp, enclosure_dp,
                                        power_needed=power_needed, time_needed=lookahead, max_power=max_power, reason=reason)

                site.replace_and_balance(server_dp, enclosure_dp, new_fru, reason=reason)
                expected_ptmo = site.get_site_power(lookahead=lookahead) / site.system_size
                server_dp, enclosure_dp = Inspector.get_worst_fru(site, 'power')
                max_power, installable = Inspector.check_max_power(site, new_fru=new_fru)

                ##print(Inspector.get_replaceable_frus(site, by='power'))
                ##print('max power: {}, installable: {}, expected ptmo: {}, server_dp, enclosure_dp: {}'.format(max_power, installable, expected_ptmo, [server_dp, enclosure_dp]))

                site.monitor.store_result('power', 'expected PTMO', site.get_month(), expected_ptmo)

        commitments, fails = site.store_performance()

        return commitments, fails

    # look for FRU replacements to meet TMO commitments
    def check_tmo(site, commitments, fails, server_p, enclosure_p):
        power_pulled = site.servers[server_p].enclosures[enclosure_p].fru.get_power() \
            if site.servers[server_p].enclosures[enclosure_p].is_filled() else 0

        if fails['CTMO']:
            power_needed = ((site.limits['CTMO'] - commitments['CTMO']) * site.system_size + power_pulled) * site.get_month()
            reason_fail = 'CTMO'

        elif fails['WTMO']:
            power_needed = ((site.limits['WTMO'] - commitments['WTMO']) * site.system_size + power_pulled) * min(site.get_month(), site.limits['window'])
            reason_fail = 'WTMO'

        elif fails['PTMO']:
            power_needed = (site.limits['PTMO'] - commitments['PTMO']) * site.system_size + power_pulled
            reason_fail = 'PTMO'

        reason = '{} {:0.02%} below limit {:0.02%}'.format(reason_fail, commitments[reason_fail], site.limits[reason_fail])
        new_fru = site.shop.get_best_fit_fru(site.server_model, site.get_date(), site.number, server_p, enclosure_p,
                                             power_needed=power_needed, reason=reason)
        
        # swap out old FRU and store if not empty
        old_fru = site.replace_fru(server_p, enclosure_p, new_fru)

        if old_fru is not None:
            # FRU replaced an existing module
            site.shop.store_fru(old_fru, site.number, server_p, enclosure_p, reason=reason)
        else:
            # FRU was added to empty enclosure, so check for overloading
            site.balance_site()

        # find next worst FRU
        commmitments, fails, server_p, enclosure_p, server_e, enclosure_e = Inspector.check_worst_fru(site)

        return commmitments, fails, server_p, enclosure_p, server_e, enclosure_e

    # look for FRUs replacements to meet efficiency commitment
    def check_efficiency(site, commitments, server_e, enclosure_e):
        # match power, energy and remaining life of replacing FRU
        reason = None

        server = site.servers[server_e]
        if server.enclosures[enclosure_e].is_filled():
            # replace an inefficient FRU with a similar model
            replacing_fru = server.enclosures[enclosure_e].fru

            if replacing_fru.is_dead():
                # FRU is already dead
                # replace with original FRU rating
                new_fru = site.shop.get_best_fit_fru(server.model, site.get_date(), site.number, server_e, enclosure_e,
                                                     power_needed=replacing_fru.rating, reason=reason)
            else:
                # FRU has life left
                new_fru = site.shop.get_best_fit_fru(server.model, site.get_date(), site.number, server_e, enclosure_e,
                                                     power_needed=replacing_fru.get_power(), energy_needed=replacing_fru.get_energy(),
                                                     time_needed=replacing_fru.get_expected_life(), reason=reason)
            old_fru = site.replace_fru(server_e, enclosure_e, new_fru)
            # FRU replaced an existing module
            site.shop.store_fru(old_fru, site.number, server_e, enclosure_e, reason=reason)
        else:
            # put in a brand new FRU
            new_fru = site.shop.get_best_fit_fru(server.model, site.get_date(), site.number, server_e, enclosure_e, initial=True, reason=reason)

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

    def check_exists(*servers_and_enclosures):
        exists = all(s_or_e is not None for s_or_e in servers_and_enclosures)
        return exists