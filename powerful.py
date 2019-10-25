# definitions for power and efficiency curves, power modules, hot boxes and energy servers

# built-in imports
from typing import List, Tuple, Union
from datetime import date
from dateutil.relativedelta import relativedelta

# add-on imports
from pandas import DataFrame, Series, concat
from numpy import random as nprandom

# self-defined imports
from structure import SQLDB

class Curves:
    def __init__(self, curves: DataFrame):
        self.curves = curves

# power curves for a model type
class PowerCurves(Curves):
    def __init__(self, curves: DataFrame):
        Curves.__init__(self, curves)
        self.percentiles = curves.columns.to_list()
        
        self.ideal = max(self.percentiles)
        self.worst = min(self.percentiles)
        self.extrema = {'ideal': max(self.percentiles),
                        'worst': min(self.percentiles)}

        self.probabilities = self.get_probabilities(self.percentiles)

    # calculate probability of each percentile
    def get_probabilities(self, percentiles: list) -> Series:
        probabilities = DataFrame(data=[0] + percentiles, columns=['percentile'])
        probabilities.loc[0, 'top'] = 0

        for i in range(1, len(probabilities)):
            probabilities.loc[i, 'top'] = 2 * probabilities.loc[i, 'percentile'] - probabilities.loc[i-1, 'top']
        probabilities.loc[:, 'probability'] = probabilities['top'].diff()
        probabilities.dropna(inplace=True)
        probabilities.index = percentiles
        probabilities = probabilities['probability']

        return probabilities

    # checks if allowed parameter is an extrema
    def is_extrema(self, allowed: Union[str, list]) -> bool:
        extrema = (type(allowed) is str) and (allowed in self.extrema)
        return extrema

    # return range of curves probable based on current observation
    def get_allowed_curves(self, allowed: Union[str, list] = [0, 1], fit: dict = None, stack_reducer: float = 1) -> DataFrame:
        if fit is None:
            # new FRU
            
            if self.is_extrema(allowed):
                allowed_curves = self.curves[[self.extrema[allowed]]]
            else:
                allowed_curves = self.curves[[percentile for percentile in self.percentiles \
                    if (percentile >= allowed[0]) & (percentile <= allowed[-1])]]

            allowed_curves = allowed_curves.mul(stack_reducer)

        else:
            # FRU has already been in the field, find least error
            max_operating_time = min(len(self.curves)-1, fit['operating time'])
            ##if ('performance' in fit) and ('operating time' in fit):
            # pulled from API
            to_fit = fit['performance'].iloc[len(fit['performance'])-fit['operating time']:, :].reset_index()['kw']

            errors = self.curves.mul(stack_reducer).loc[0:len(fit)-1, :].T.sub(to_fit).T.pow(2).sum()

            if self.is_extrema(allowed):
                filtered_curves = [self.extrema[allowed]]

            else:
                filtered_curves = self.curves.loc[:, errors[errors == errors.min()].index.to_list()].columns

            allowed_curves = DataFrame(data=[fit['performance']['kw'].to_list() + self.curves.loc[max_operating_time:, c].to_list() for c in filtered_curves],
                                        index=filtered_curves).T
        
        return allowed_curves
        
    # normalize probabilities for percentile selection
    def normalize_probabilties(self, percentiles: list) -> list:
        probabilities = self.probabilities.loc[percentiles]

        probabilities_normalized = [p/probabilities.sum() for p in probabilities]
        return probabilities_normalized

    # pick power curve for power module
    def pick_curve(self, allowed: Union[str, list] = [0, 1], fit: dict = None, stack_reducer: float = 1) -> Series:
        allowed_curves = self.get_allowed_curves(allowed, fit)

        probabilities_normalized = self.normalize_probabilties(allowed_curves.columns)
        chosen_percentile = nprandom.choice(allowed_curves.columns.to_list(), p=probabilities_normalized)
        
        curve = allowed_curves[chosen_percentile].mul(stack_reducer)
        
        # remove months where power is gone
        curve = curve[curve != 0].dropna()
       
        return curve

    # get expected power curve of a power module
    def get_expected_curve(self, fit: Union[str, list] = None, stack_reducer: float = 1) -> Series:
        allowed_curves = self.get_allowed_curves(fit=fit, stack_reducer=stack_reducer)
        probabilities_normalized = self.normalize_probabilties(allowed_curves.columns)

        operating_time = 0 if fit is None else fit['operating time']
        expected_curve = allowed_curves.loc[operating_time:].mul(probabilities_normalized).sum('columns')

        return expected_curve

    # get expected energy for time period
    def get_expected_energy(self, fit: Union[str, list] = None, time_needed: int = 0, stack_reducer: float = 1) -> float:
        expected_curve = self.get_expected_curve(fit, stack_reducer)

        operating_time = 0 if fit is None else fit['operating time']
        if (time_needed > 0) and (time_needed <= len(expected_curve) - operating_time):
            energy = expected_curve.iloc[operating_time:operating_time+time_needed].sum()
        else:
            energy = expected_curve.iloc[operating_time:].sum()
        return energy

# efficiency curves for a model type
class EfficiencyCurves(Curves):
    def __init__(self, curves):
        self.curves = curves

    def pick_curve(self, fit: Union[str, list] = None) -> Series:
        if (fit is not None) and (('performance' in fit) and ('operating time' in fit)):
            # pulled from API
            curve = Series(data=fit['performance']['pct'].to_list() + self.curves.loc[fit['operating time']:].to_list())

        else:
            curve = self.curves.copy()

        return curve

class DataSheets:
    def __init__(self, sql_db: SQLDB):
        self.sql_db = sql_db

    # get alternative name for special servers
    def get_alternative_model(self, server_model: str) -> str:
        alternative_name = self.sql_db.get_alternative_server_model(server_model)
        return alternative_name

    # get power modules that work with energy server
    def get_compatible_modules(self, server_model: str) -> Series:
        allowed_modules = self.sql_db.get_compatible_modules(server_model)
        return allowed_modules

# details of power modules
class PowerModules(DataSheets):
    def __init__(self, sql_db):
        DataSheets.__init__(self, sql_db)

    # get power and efficiency curves
    def get_curves(self, model: str, mark: str, model_number: str) -> Tuple[PowerCurves, EfficiencyCurves]:
        power_curves = PowerCurves(self.sql_db.get_power_curves(model, mark, model_number))
        efficiency_curves = EfficiencyCurves(self.sql_db.get_efficiency_curve(model, mark, model_number))
        return power_curves, efficiency_curves

    # find best new power module available
    def get_model(self, install_date: date, wait_period: bool = False,
                  power_needed: float = 0, max_power: float = None, energy_needed: float = 0, time_needed: int = 0, best: bool = False,
                  server_model: str = None, roadmap: DataFrame = None, match_server_model: bool = False) -> Tuple[str, str, str]:

        # establish technology roadmap
        if roadmap is None:
            allowed = self.sql_db.get_default_modules()
        else:
            allowed = roadmap

        # make sure the roadmap allows for the server type
        server_model = self.get_alternative_model(server_model)
        allowed = allowed.append(self.sql_db.get_default_modules().query('model == @server_model')).drop_duplicates()

        # get modules that are compatible and available
        buildable_modules = self.sql_db.get_buildable_modules(install_date, server_model=server_model, allowed=allowed, wait_period=wait_period)
        
        if match_server_model and (server_model is not None):
            # pick the original module for the server type
            buildable_modules.query('model == @server_model', inplace=True)

            if buildable_modules.empty:
                # server was available ahead of schedule
                print('{} not available on {}, using earliest possible module'.format(server_model, install_date))
                buildable_modules = self.sql_db.get_buildable_modules(install_date=0, server_model=server_model, allowed=allowed)
                buildable_modules.query('model == @server_model', inplace=True)
        
        if not buildable_modules.empty:
            # get rating and energy
            buildable_modules['rating'], buildable_modules['energy'] = [None]*2
            buildable_modules.loc[:, ['rating', 'energy']] = buildable_modules.apply(lambda x: (self.get_rating(x['model'], x['mark'], x['model_number']),
                                                                                                self.get_energy(x['model'], x['mark'], x['model_number'], time_needed)),
                                                                                     axis='columns', result_type='expand').values

            # check power requirements
            max_rating = buildable_modules['rating'].max()
            if (max_rating >= power_needed) and (not best):
                # if there is a model big enough to handle the load, choose it
                power_filter = 'rating >= @power_needed'
            else:
                # choose the biggest model available
                power_filter = 'rating == @max_rating'
            buildable_modules.query(power_filter, inplace=True)

            if max_power is not None:
                buildable_modules.query('rating <= @max_power', inplace=True)

            # check energy requirements
            max_energy = buildable_modules['energy'].max()
            if (max_energy >= energy_needed) and (not best):
                # if there is a model big enough to handle the load, choose it
                buildable_modules.query('energy >= @energy_needed', inplace=True)

            else:
                buildable_modules.query('energy == @max_energy', inplace=True)

            if len(buildable_modules):
                # there is at least one model that matches requirements
                model, mark, model_number = buildable_modules.iloc[0][['model', 'mark', 'model_number']]
                
                return model, mark, model_number

    # return initial power rating of a given module
    def get_rating(self, model: str, mark: str, model_number: str) -> int:
        rating = self.sql_db.get_module_rating(model, mark, model_number)
        return rating

    # return expected energy output of a given model
    def get_energy(self, model: str, mark: str, model_number: str, time_needed: int) -> float:
        curves = PowerCurves(self.sql_db.get_power_curves(model, mark, model_number))

        energy = curves.get_expected_energy(time_needed=time_needed)
        return energy

    # return initial efficiency contribution of a given module
    def get_efficiency(self, model: str, mark: str, model_number: str) -> float:
        efficiency = self.sql_db.get_module_efficiency(model, mark, model_number)
        rating = self.sql_db.get_module_rating(model, mark, model_number)
        efficiency = efficiency * rating
        return efficiency

    # return stacks of a given module
    def get_stacks(self, model: str, mark: str, model_number: str) -> List[int]:
        stacks = self.sql_db.get_module_stacks(model, mark, model_number)
        return stacks

# details of energy enclosures
class HotBoxes(DataSheets):
    def __init__(self, sql_db):
        DataSheets.__init__(self, sql_db)

    def get_model_details(self, server_model: str) -> DataFrame:
        model_number, nameplate = self.sql_db.get_enclosure_model_number(server_model)
        return model_number, nameplate
    
# details of energy servers
class EnergyServers(DataSheets):
    def __init__(self, sql_db:SQLDB):
        DataSheets.__init__(self, sql_db)

    # get base model of a server model number
    def get_server_model(self, **kwargs) -> DataFrame:
        if kwargs.get('server_model_class') is not None:
            kwargs['server_model_class'] = self.get_alternative_model(kwargs['server_model_class'])
        server_model = self.sql_db.get_server_model(**kwargs)
        return server_model

    # get lasted server model class
    def get_latest_server_model(self, install_date: date, target_model: str) -> str:
        latest_server_model_class = self.sql_db.get_latest_server_model(install_date, target_model=target_model)
        return latest_server_model_class
        
    # get default nameplate sizes   
    def get_server_nameplates(self, latest_server_model_class: str, target_size: float) -> DataFrame:
        server_nameplates = self.sql_db.get_server_nameplates(latest_server_model_class, target_size)
        return server_nameplates