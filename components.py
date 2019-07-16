# physical field replaceable unit power modules (FRUs) and energy servers (with enclosure cabinets) 

import pandas
from dateutil.relativedelta import relativedelta

# power module (field replaceable unit)
class FRU:
    def __init__(self, serial, model, base, mark, power_curves, efficiency_curve, install_date, current_date,
                 fit=None):
        # FRU defined by sampled power curve at given installation year
        # FRUs are typically assumed to be new and starting at time 0, otherwise they follow the best fit power curve
        self.serial = serial
        self.model = model
        self.base = base
        self.mark = mark

        self.install_date = install_date
        operating_time = relativedelta(current_date, install_date)
        self.month = operating_time.years*12 + operating_time.months
        
        self.power_curves = None
        self.ideal_curve = None
        self.efficiency_curve = None
        self.rating = 0
        self.max_efficiency = 0
        self.set_curves(power_curves, efficiency_curve)

        self.power_curve = self.power_curves.pick_curve(allowed=[0,1], fit=fit)
        
    def __str__(self):
        if self.is_dead():
            age_string = ' XXX '
        else:
            age_string = '{:0.1f}yrs'.format(self.month/12)
        string = 'PWM: {:0.0f}@{:0.1f}kw ({:0.1%}tmo, {:0.1%}eff) - {}'.format(self.rating,
                                                                               self.get_power(),
                                                                               self.get_power()/self.rating,
                                                                               self.get_efficiency(),
                                                                               age_string)
        return string

    # set power and efficiency curves, rating and starting efficiency for new or overhauled FRU
    def set_curves(self, power_curves, efficiency_curve):
        self.power_curves = power_curves
        self.ideal_curve = power_curves.pick_curve(allowed='ideal')
        self.rating = self.ideal_curve[0]

        self.efficiency_curve = efficiency_curve
        self.max_efficiency = efficiency_curve[0]

    # power at current degradation level
    def get_power(self, ideal=False):
        if self.is_dead():
            power = 0
        else:
            if ideal:
                power = self.ideal_curve[self.month]
            else:
                power = self.power_curve[self.month]
        
        return power

    # estimate the power curvey in deployed FRU
    def get_expected_curve(self):
        curve = self.power_curves.get_expected_curve(self.month, self.get_power())
        curve.index = range(len(curve))
        return curve

    # estimate the remaining energy in deployed FRU
    def get_energy(self, months=None):
        time_needed = self.get_expected_life() if months is None else months      
        energy = self.power_curves.get_expected_energy(operating_time=self.month, observed_power=self.get_power(), time_needed=time_needed)
        return energy

    # get efficiency of FRU
    def get_efficiency(self):
        efficiency = self.efficiency_curve[min(self.month, len(self.efficiency_curve)-1)]
        return efficiency

    # estimated months left of FRU life
    def get_expected_life(self):
        curve = self.get_expected_curve()
        life = len(curve)
        return life

    # FRU is too old for use
    def is_dead(self):
        dead = self.month >= len(self.power_curve)
        return dead

    # determine if the power module has degraded already
    def is_degraded(self, threshold=0):
        degraded = self.get_power() < self.rating - threshold
        return degraded

    # determine if the power module is inefficient already
    def is_inefficient(self, threshold=0):     
        inefficient = self.get_efficiency() < self.max_efficiency - threshold
        return inefficient

    # determine if a FRU needs to be repaired
    def is_deviated(self, threshold=0):
        if self.is_dead():
            # FRU is at end of life and unrepairable
            deviated = False
        else:
            deviated = 1 - self.get_power() / self.get_power(ideal=True) > threshold
     
        return deviated

    # move to the next operating month
    def degrade(self):
        self.month += 1
        return

    # bring power curve to median
    def repair(self):
        if not self.is_dead():
            self.power_curve = self.power_curves.pick_curve(allowed=[0.5, 0.9])
        return

    # shift power and efficiency curves forward during storage
    def store(self, months):
        self.month += months
        return

    # replace stacks and choose new power curves for bespoke options
    def overhaul(self, mark, power_curves, efficiency_curves):
        # give new bespoke mark
        self.mark = mark
        # set new power and efficiency curves
        self.set_curves(power_curves, efficiency_curve)
        # reset month
        self.month = 0
        
# cabinet in energy server that can house a FRU
class Enclosure:  
    def __init__(self, serial, number):
        self.serial = serial
        self.number = number
        self.fru = None

    # enclosure can hold a FRU
    def is_empty(self):
        empty = self.fru is None
        return empty

    # enclosure is holding a FRU
    def is_filled(self):
        filled = not self.is_empty()
        return filled

    # put a FRU in enclosure
    def add_fru(self, fru):
        self.fru = fru
    
    # take a FRU out of enclosure
    def remove_fru(self):
        if not self.is_empty():
            old_fru = self.fru
            self.fru = None
            return old_fru
        return

# housing unit for power modules
class Server:
    def __init__(self, serial, number, model, model_number, nameplate):
        self.serial = serial
        self.number = number
        self.model = model
        self.model_number = model_number
        self.nameplate = nameplate
        self.enclosures = []

    def __str__(self):
        ceiling_loss = self.get_ceiling_loss()
        ceiling_string = ' | {:0.1f}kW ceiling loss'.format(ceiling_loss) if ceiling_loss > 0 else ''
        server_string = 'ES: {:0.1f}kW (nameplate {:0.0f}kW){}'.format(self.get_power(), self.nameplate, ceiling_string)

        enclosure_string = ' '+' \n '.join(str(fru) if fru is not None else 'EMPTY' for fru in self.enclosures)
        line_string = '-'*20
        string = '\n'.join([server_string, line_string, enclosure_string, line_string])
        return string

    # add an empty enclosure for a FRU or plus-one
    def add_enclosure(self, enclosure):
        self.enclosures.append(enclosure)
        return
    
    # replace FRU in enclosure with new FRU
    def replace_fru(self, enclosure_number=None, fru=None):
        if enclosure_number is None:
            # remove last FRU
            filled_enclosures = [enclosure.is_filled() for enclosure in self.enclosures]
            enclosure_number = len(filled_enclosures) - filled_enclosures[::-1].index(True) - 1

        old_fru = self.enclosures[enclosure_number].remove_fru()
        self.enclosures[enclosure_number].add_fru(fru)

        return old_fru

    # return array of FRU powers
    def get_fru_power(self):
        fru_power = [enclosure.fru.get_power() if enclosure.is_filled() else 0 for enclosure in self.enclosures]
        return fru_power

    # get total power of all FRUs, capped at nameplate rating
    def get_power(self, cap=True):
        power = sum(self.get_fru_power())
        if cap:
            power = min(self.nameplate, power)

        return power

    # estimate the remaining energy in server FRUs
    def get_energy(self, months=None):
        curves = pandas.concat([enclosure.fru.get_expected_curve() for enclosure in self.enclosures \
            if enclosure.is_filled() and not enclosure.fru.is_dead()], axis='columns') 

        if months is not None:
            curves = curves.loc[:months, :]
        
        # cap at nameplate rating
        potential = curves.sum('columns')
        energy = potential.where(potential < self.nameplate, self.nameplate).sum()

        return energy

    # max potential gain before hitting ceiling loss
    def get_headroom(self):      
        power = self.get_power()
        headroom = self.nameplate - power
        return headroom

    # power that is lost due to nameplate capacity
    def get_ceiling_loss(self):       
        ceiling_loss = self.get_power(cap=False) - self.get_power()
        return ceiling_loss

    # server has an empty enclosure or an enclosure with a dead FRU
    def has_empty(self, dead=False):
        # server has at least one empty enclosure
        empty = any(enclosure.is_empty() for enclosure in self.enclosures)
        
        # check if there is a dead module
        if dead:
            empty |= any(enclosure.fru.is_dead() for enclosure in self.enclosures if enclosure.is_filled())

        return empty

    # server is full if there are no empty enclosures, it is at nameplate capacity or adding another FRU won't leave a slot free
    def is_full(self, plus_one_empty=False):
        full = (not self.has_empty()) or (self.get_power() >= self.nameplate) or \
            (plus_one_empty and (sum([enclosure.is_empty() for enclosure in self.enclosures]) <= 1))
        return full

    # server has no FRUs and is just empty enclosures
    def is_empty(self):
        empty = all(enclosure.is_empty() for enclosure in self.enclosures)
        return empty

    # return sequence number of empty enclosure
    def get_empty_enclosure(self, dead=False):
        dead_frus = [enclosure.fru.is_dead() if enclosure.is_filled() else False for enclosure in self.enclosures]

        if self.has_empty():
            enclosure_number = min(enclosure.number for enclosure in self.enclosures if enclosure.is_empty())
        elif dead and len(dead_frus):
            enclosure_number =  dead_frus.index(True)
        else:
            enclosure_number = None

        return enclosure_number

    # degrade each FRU in server
    def degrade(self):
        for enclosure in self.enclosures:
            if enclosure.is_filled():
                enclosure.fru.degrade()
        return