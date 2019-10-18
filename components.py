# physical field replaceable unit power modules (FRUs) and energy servers (with enclosure cabinets)

from pandas import concat
from dateutil.relativedelta import relativedelta

from debugging import StopWatch

class Component:
    '''
    A component is a physical object with a model (base)
    and model number (specific version). It can also have
    a mark, essentially a subcategory of a base model.
    Each component has a serial number for blockchain tracking.
    '''
    def __init__(self, serial, model, model_number, **kwargs):
        self.serial = serial
        self.model = model
        self.model_number = model_number
        
        if 'mark' in kwargs:
            self.mark = kwargs['mark']

        if 'nameplate' in kwargs:
            self.nameplate = kwargs['nameplate']
        if 'rating' in kwargs:
            self.rating = kwargs['rating']

        if 'number' in kwargs:
            self.number = kwargs['number']

# power module (field replaceable unit)
class FRU(Component):
    '''
    A FRU (Field Replaceable Unit) is an object to represent
    a power module, which can either be "revenue" (installed
    with a new energy server) or "FRU" (installed as a
    replacement for an original module).
    '''
    def __init__(self, serial, model, mark, model_number, rating, power_curves, efficiency_curves, install_date, current_date,
                 fit=None):
        # FRU defined by sampled power curve at given installation year
        # FRUs are typically assumed to be new and starting at time 0, otherwise they follow the best fit power curve
        Component.__init__(self, serial, model, model_number, mark=mark, rating=rating)

        self.install_date = install_date
        self.month = 0
        
        self.power_curves = power_curves
        self.power_curve = self.power_curves.pick_curve(allowed=[0,1], fit=fit)
        self.ideal_curve = self.power_curves.pick_curve(allowed='ideal')

        self.efficiency_curves = efficiency_curves
        self.efficiency_curve = self.efficiency_curves.pick_curve(fit=fit)

        self.max_efficiency = self.efficiency_curve.max()
      
    # month to look at
    def get_month(self, lookahead=None):
        if lookahead is None:
            lookahead = 0
        month = int(self.month + lookahead)
        return month

    # power at current degradation level
    def get_power(self, ideal=False, lookahead=None):
        if self.is_dead(lookahead=lookahead):
            power = 0

        else:
            month = self.get_month(lookahead=lookahead)

            if ideal:
                curve = self.ideal_curve.copy()
            elif lookahead:
                curve = self.get_expected_curve()
            else:
                curve = self.power_curve.copy()

            if month in curve:
                power = curve[month]
            else:
                power = 0
      
        return power

    # estimate the power curve in deployed FRU
    def get_expected_curve(self):
        curve = self.power_curve.copy()
        #curve = self.power_curves.get_expected_curve(self.get_month(), self.get_power())
        return curve

    # estimate the remaining energy in deployed FRU
    def get_energy(self, months=None):
        time_needed = self.get_expected_life() if months is None else months      
        energy = self.power_curves.get_expected_energy(operating_time=self.get_month(), observed_power=self.get_power(), time_needed=time_needed)
        return energy

    # get efficiency of FRU
    def get_efficiency(self, lookahead=None):
        month = self.get_month(lookahead=lookahead)
        efficiency = self.efficiency_curve[min(month, len(self.efficiency_curve)-1)]
        return efficiency

    # estimated months left of FRU life
    def get_expected_life(self):
        curve = self.get_expected_curve()
        life = len(curve) - self.get_month() - 1
        return life

    # FRU is too old for use
    def is_dead(self, lookahead=None):
        month = self.get_month(lookahead=lookahead)
        dead = month > len(self.power_curve)
        return dead

    # determine if the power module has degraded already
    def is_degraded(self, threshold:int=0) -> bool:
        '''
        If a power module is outputting less power than its initial rating
        then it is degraded and can be replaced. Default threshold
        is zero kW below initial rating.
        '''
        degraded = self.get_power() < self.rating - threshold
        return degraded

    # determine if the power module is inefficient already
    def is_inefficient(self, threshold:int=0) -> bool:     
        '''
        If a power module is operating at a lower efficiencing than initially
        then it is inefficient and can be replaced. Default threshold
        is zero percent below initial rating.
        '''
        inefficient = self.get_efficiency() < self.max_efficiency - threshold
        return inefficient

    # determine if a FRU needs to be repaired
    def is_deviated(self, threshold:int=0) -> bool:
        '''
        If a power module is outputting power too far below what the
        ideal curve would be outputting, then it is deviated and can
        be replaced. Default threshold is zero percent below ideal.
        '''
        if self.is_dead() or (self.get_power() == 0):
            # FRU is at end of life and unrepairable
            deviated = False
        else:
            deviated = 1 - self.get_power() / self.get_power(ideal=True) > threshold ## DIV BY ZERO??
     
        return deviated

    # move to the next operating month
    def degrade(self):
        self.month += 1
        return

    # bring power curve to median
    def repair(self):
        '''
        If a power module is not fully dead, then it can
        be repaired to some curve between the median and the
        ideal. A dead module can only be overhauled.
        '''
        if not self.is_dead():
            self.power_curve = self.power_curves.pick_curve(allowed=[0.5, 0.9])
        return

    # shift power and efficiency curves forward during storage
    def store(self, months):
        '''
        When a power module is stored for future redeploys,
        it moves forward on its power and efficiency curves
        due to storage loss.
        '''
        self.month += months
        return

    # replace stacks and choose new power curves for bespoke options
    def overhaul(self, model_number, power_curves, efficiency_curves):
        '''
        An overhauled FRU starts its life over with new power
        and efficiency curves.
        '''
        # give new bespoke model number
        self.model_number = model_number
        # set new power and efficiency curves
        self.set_curves(power_curves, efficiency_curve)
        # reset month
        self.month = 0

    # create a copy of the base FRU
    def copy(self, serial, install_date, current_date, fit=None):
        '''
        The shop uses a template FRU to produce
        new versions.
        '''
        fru = FRU(serial, self.model, self.mark, self.model_number, self.rating, self.power_curves, self.efficiency_curves, install_date, current_date, fit=fit)
        return fru
        
# cabinet in energy server that can house a FRU
class Enclosure(Component):  
    def __init__(self, serial, number, model, model_number, nameplate):
        Component.__init__(self, serial, model, model_number, nameplate=nameplate, number=number)
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

    # get power of FRU if not empty
    def get_power(self, lookahead=None):
        '''
        The output power of an enclosure is limited by its nameplate.
        '''
        if self.is_empty():
            power = 0
        else:
            power = min(self.nameplate, self.fru.get_power(lookahead=lookahead))

        return power

    # get expected energy of FRU if not empty
    def get_energy(self, months=None):
        energy = self.fru.get_energy(months=months) if not self.is_empty() else 0
        return energy

    # get efficiency of FRU if not empty
    def get_efficiency(self, lookahead=None):
        if self.is_empty():
            efficiency = 0
        else:
            efficiency = self.fru.get_efficiency(lookahead=lookahead)

        return efficiency

    # upgrade enclosure model type
    def upgrade_enclosure(self, model, model_number, nameplate):
        self.model = model
        self.model_number = model_number
        self.nameplate = nameplate

# housing unit for power modules
class Server(Component):
    def __init__(self, serial, number, model, model_number, nameplate):
        Component.__init__(self, serial, model, model_number, nameplate=nameplate, number=number)
        self.enclosures = []

    # add an empty enclosure for a FRU or plus-one
    def add_enclosure(self, enclosure):
        self.enclosures.append(enclosure)
        return
    
    # replace FRU in enclosure with new FRU
    def replace_fru(self, enclosure_number=None, fru=None):
        if enclosure_number is None:
            # add to next available slot
            enclosure_number = self.get_empty_enclosure()
            
        if enclosure_number is not None:
            old_fru = self.enclosures[enclosure_number].remove_fru()
            self.enclosures[enclosure_number].add_fru(fru)

        else:
            old_fru = None

        return old_fru

    # return array of FRU powers
    def get_fru_power(self, lookahead=None):
        fru_power = [enclosure.get_power(lookahead=lookahead) for enclosure in self.enclosures]
        return fru_power

    # get total power of all FRUs, capped at nameplate rating
    def get_power(self, cap=True, lookahead=None):
        power = sum(self.get_fru_power(lookahead=lookahead))
        if cap:
            power = min(self.nameplate, power)

        return power

    # estimate the remaining energy in server FRUs
    def get_energy(self, months=None):
        curves_to_concat = [enclosure.fru.get_expected_curve()[enclosure.fru.get_month():] for enclosure in self.enclosures \
            if enclosure.is_filled() and not enclosure.fru.is_dead()]

        if not len(curves_to_concat): # bug with no live FRUs?
            energy = 0

        else:
            curves = concat(curves_to_concat, axis='columns', ignore_index=True)
            #curves.index = range(len(curves))

            if months is not None:
                curves = curves.iloc[:months, :]

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
        '''
        After a month of operation, all the enclosures are moved
        forward in time.
        '''
        for enclosure in self.enclosures:
            if enclosure.is_filled():
                enclosure.fru.degrade()
        return