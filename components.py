# physical field replaceable unit power modules (FRUs) and energy servers (with enclosure cabinets)

# built-in imports
from __future__ import annotations
from inspect import signature
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Union

# add-on imports
from pandas import DataFrame, Series, concat

# self-defined imports
from powerful import PowerCurves, EfficiencyCurves

class Component:
    refurb_tag = '-R'
    '''
    A component is a physical object with a model (base)
    and model number (specific version). It can also have
    a mark, essentially a subcategory of a base model.
    Each component has a serial number for blockchain tracking.
    '''
    def __init__(self, serial: str, model: str, model_number: str, **kwargs):
        self.serial = serial
        self.model = model
        self.model_number = model_number
        self.refurbed = False

        if 'mark' in kwargs:
            self.mark = kwargs['mark']

        if 'nameplate' in kwargs:
            self.nameplate = kwargs['nameplate']
        if 'rating' in kwargs:
            self.rating = kwargs['rating']

        if 'number' in kwargs:
            self.number = kwargs['number']

    def get_model_number(self) -> str:
        '''
        A refurbed component is tagged with -R
        '''
        model_number = self.model_number + (Component.refurb_tag if self.refurbed else '')
        return model_number

    # create a copy of the base component
    def copy(self, serial: str, **kwargs) -> Component:
        '''
        The shop uses a template FRU to produce
        new versions.
        '''
        signatures = [p for p in signature(self.__class__.__init__).parameters if p != 'self']
        dictionary = self.__dict__
        attributes = {d: dictionary[d] for d in dictionary if d in signatures}
        updates = {k: kwargs[k] for k in kwargs if k in signatures}
        attributes.update(updates)
        attributes['serial'] = serial

        component = self.__class__(**attributes)
        return component

# power module (field replaceable unit)
class FRU(Component):
    '''
    A FRU (Field Replaceable Unit) is an object to represent
    a power module, which can either be "revenue" (installed
    with a new energy server) or "FRU" (installed as a
    replacement for an original module).
    '''
    def __init__(self, serial: str, model: str, mark: str, model_number: str,
                 rating: float, power_curves: PowerCurves, efficiency_curves: EfficiencyCurves, stacks: int,
                 install_date: date, current_date: date,
                 fit: dict = None):
        # FRU defined by sampled power curve at given installation year
        # FRUs are typically assumed to be new and starting at time 0, otherwise they follow the best fit power curve
        Component.__init__(self, serial, model, model_number, mark=mark, rating=rating)

        self.install_date = install_date
        self.month = 0
        
        self.stacks = stacks
        self.stack_reducer = 1

        self.power_curves = power_curves
        self.power_curve = self.power_curves.pick_curve(allowed=[0,1], fit=fit, stack_reducer=self.stack_reducer)
        self.ideal_curve = self.power_curves.pick_curve(allowed='ideal', fit=fit, stack_reducer=self.stack_reducer)

        self.efficiency_curves = efficiency_curves
        self.efficiency_curve = self.efficiency_curves.pick_curve(fit=fit)

        self.max_efficiency = self.efficiency_curve.max()
      
    # month to look at
    def get_month(self, lookahead: int = None) -> int:
        '''
        Current or future state of component
        '''
        if lookahead is None:
            lookahead = 0
        month = int(self.month + lookahead)
        return month

    # power at current degradation level
    def get_power(self, ideal: bool = False, lookahead: int = None) -> float:
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

    # get performance fit so far
    def get_performance(self) -> Dict[str, Union[DataFrame, int]]:
        fit = {'performance': self.power_curve[:self.get_month()].to_frame('kw'),
               'operating time': self.get_month()}

        return fit

    # estimate the power curve in deployed FRU
    def get_expected_curve(self) -> Series:
        curve = self.power_curves.get_expected_curve(fit=self.get_performance(),
                                                     stack_reducer=self.stack_reducer)
        return curve

    # estimate the remaining energy in deployed FRU
    def get_energy(self, months: int = None) -> float:
        time_needed = self.get_expected_life() if months is None else months      
        energy = self.power_curves.get_expected_energy(fit=self.get_performance(),
                                                       time_needed=time_needed, stack_reducer=self.stack_reducer)
        return energy

    # get efficiency of FRU
    def get_efficiency(self, lookahead: int = None) -> float:
        month = self.get_month(lookahead=lookahead)
        efficiency = self.efficiency_curve[min(month, len(self.efficiency_curve)-1)]
        return efficiency

    # get deviation of FRU
    def get_deviation(self, lookahead: int = None) -> float:
        ideal_power = self.get_power(ideal=True, lookahead=lookahead)
        if ideal_power == 0:
            deviation = 0
        else:
            deviation = max(0, ideal_power - self.get_power(lookahead=lookahead)) # 1 - get_power / ideal_power

        return deviation

    # estimated months left of FRU life
    def get_expected_life(self) -> int:
        curve = self.get_expected_curve()
        life = len(curve) - self.get_month() - 1
        return life

    # FRU is too old for use
    def is_dead(self, lookahead: int = None) -> bool:
        month = self.get_month(lookahead=lookahead)
        dead = month > len(self.power_curve)
        return dead

    # determine if the power module has degraded already
    def is_degraded(self, threshold: float = 0) -> bool:
        '''
        If a power module is outputting less power than its initial rating
        then it is degraded and can be replaced. Default threshold
        is zero kW below initial rating.
        '''
        degraded = self.get_power() < self.rating - threshold
        return degraded

    # determine if the power module is inefficient already
    def is_inefficient(self, threshold: float = 0) -> bool:     
        '''
        If a power module is operating at a lower efficiencing than initially
        then it is inefficient and can be replaced. Default threshold
        is zero percent below initial rating.
        '''
        inefficient = self.get_efficiency() < self.max_efficiency - threshold
        return inefficient

    # determine if a FRU needs to be repaired
    def is_deviated(self, threshold: float = 0) -> bool:
        '''
        If a power module is outputting power too far below what the
        ideal curve would be outputting, then it is deviated and can
        be replaced. Default threshold is zero kW below ideal.
        '''
        if self.is_dead() or (self.get_power() == 0):
            # FRU is at end of life and unrepairable
            deviated = False
        else:
            deviated = self.get_deviation() > threshold
     
        return deviated

    # move to the next operating month
    def degrade(self):
        '''
        At the end of a period, the FRU is moved ahead on its curves.
        '''
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
    def store(self, months: int):
        '''
        When a power module is stored for future redeploys,
        it moves forward on its power and efficiency curves
        due to storage loss.
        '''
        self.month += months
        return

    # replace stacks and choose new power curves for bespoke options
    def overhaul(self, new_stacks: int):
        '''
        An overhauled FRU starts its life over with new power
        and efficiency curves.
        '''
        # set new power and efficiency curves
        self.stack_reducer = new_stacks/self.stacks
       
        self.power_curve = self.power_curves.pick_curve(allowed=[0,1], stack_reducer=self.stack_reducer)
        self.ideal_curve = self.power_curves.pick_curve(allowed='ideal', stack_reducer=self.stack_reducer) ##

        self.efficiency_curve = self.efficiency_curves.pick_curve()

        # mark as refurbished
        self.refurb = True
        
        # reset month
        self.month = 0
      
# cabinet in energy server that can house a FRU
class Enclosure(Component):
    '''
    An enclosure connects a power module and an energy server.
    It can be empty or occupied. FRUs can be added and removed.
    It has a nameplate rating that limits how much power is outputted
    by the FRU.
    '''
    def __init__(self, serial: str, number: str, model: str, model_number: str, nameplate: float):
        Component.__init__(self, serial, model, model_number, nameplate=nameplate, number=number)
        self.fru = None

    # enclosure can hold a FRU
    def is_empty(self) -> bool:
        empty = self.fru is None
        return empty

    # enclosure is holding a FRU
    def is_filled(self) -> bool:
        filled = not self.is_empty()
        return filled

    # put a FRU in enclosure
    def add_fru(self, fru: FRU):
        self.fru = fru
        return
    
    # take a FRU out of enclosure
    def remove_fru(self) -> FRU:
        if not self.is_empty():
            old_fru = self.fru
            self.fru = None
            return old_fru

    # get power of FRU if not empty
    def get_power(self, lookahead: int = None) -> float:
        '''
        The output power of an enclosure is limited by its nameplate.
        '''
        if self.is_empty():
            power = 0
        else:
            power = min(self.nameplate, self.fru.get_power(lookahead=lookahead))

        return power

    # get expected energy of FRU if not empty
    def get_energy(self, months: int = None) -> float:
        energy = self.fru.get_energy(months=months) if not self.is_empty() else 0
        return energy

    # get efficiency of FRU if not empty
    def get_efficiency(self, lookahead: int = None) -> float:
        if self.is_empty():
            efficiency = 0
        else:
            efficiency = self.fru.get_efficiency(lookahead=lookahead)

        return efficiency

    # upgrade enclosure model type
    def upgrade_enclosure(self, model: str, model_number: str, nameplate: str):
        self.model = model
        self.model_number = model_number
        self.nameplate = nameplate

# housing unit for power modules
class Server(Component):
    '''
    An energy server is made up of a variable number of enclosures.
    The energy server has an inverter with a nameplate that limits how
    much power is outputted.
    '''
    def __init__(self, serial: str, number: str, model: str, model_number: str, nameplate: float):
        Component.__init__(self, serial, model, model_number, nameplate=nameplate, number=number)
        self.enclosures = {}

    # return array of servers
    def get_enclosures(self) -> List[Enclosure]:
        enclosures = [self.enclosures[enclosure_number] for enclosure_number in self.enclosures]
        return enclosures

    # return array of server numbers
    def get_enclosure_numbers(self) -> List[str]:
        enclosure_numbers = [enclosure_number for enclosure_number in self.enclosures]
        return enclosure_numbers

    # add an empty enclosure for a FRU or plus-one
    def add_enclosure(self, enclosure: Enclosure):
        self.enclosures[enclosure.number] = enclosure
        return
    
    # replace FRU in enclosure with new FRU
    def replace_fru(self, enclosure_number: str = None, fru: FRU = None) -> FRU:
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
    def get_fru_power(self, lookahead: int = None) -> float:
        fru_power = [enclosure.get_power(lookahead=lookahead) for enclosure in self.get_enclosures()]
        return fru_power

    # get total power of all FRUs, capped at nameplate rating
    def get_power(self, cap: bool = True, lookahead: int = None) -> float:
        power = sum(self.get_fru_power(lookahead=lookahead))
        if cap:
            power = min(self.nameplate, power)

        return power

    # estimate the remaining energy in server FRUs
    def get_energy(self, months: int = None) -> float:
        curves_to_concat = [enclosure.fru.get_expected_curve()[enclosure.fru.get_month():] for enclosure in self.get_enclosures() \
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
    def get_headroom(self) -> float:     
        power = self.get_power()
        headroom = self.nameplate - power
        return headroom

    # power that is lost due to nameplate capacity
    def get_ceiling_loss(self) -> float:
        ceiling_loss = self.get_power(cap=False) - self.get_power()
        return ceiling_loss

    # server has an empty enclosure or an enclosure with a dead FRU
    def has_empty(self, dead: bool = False) -> bool:
        # server has at least one empty enclosure
        empty = any(enclosure.is_empty() for enclosure in self.get_enclosures())
        
        # check if there is a dead module
        if dead:
            empty |= any(enclosure.fru.is_dead() for enclosure in self.get_enclosures() if enclosure.is_filled())

        return empty

    # server is full if there are no empty enclosures, it is at nameplate capacity or adding another FRU won't leave a slot free
    def is_full(self, plus_one_empty: bool = False) -> bool:
        full = (not self.has_empty()) or (self.get_power() >= self.nameplate) or \
            (plus_one_empty and (sum([enclosure.is_empty() for enclosure in self.get_enclosures()]) <= 1))
        return full

    # server has no FRUs and is just empty enclosures
    def is_empty(self) -> bool:
        empty = all(enclosure.is_empty() for enclosure in self.get_enclosures())
        return empty

    # return sequence number of empty enclosure
    def get_empty_enclosure(self, dead: bool = False) -> str:
        dead_frus = [enclosure.fru.is_dead() if enclosure.is_filled() else False for enclosure in self.get_enclosures()]

        if self.has_empty():
            enclosure_number = min(enclosure.number for enclosure in self.get_enclosures() if enclosure.is_empty())
        elif dead and len(dead_frus):
            enclosure_number =  self.get_enclosure_numbers()[dead_frus.index(True)]
        else:
            enclosure_number = None

        return enclosure_number

    # degrade each FRU in server
    def degrade(self):
        '''
        After a month of operation, all the enclosures are moved
        forward in time.
        '''
        for enclosure in self.get_enclosures():
            if enclosure.is_filled():
                enclosure.fru.degrade()
        return