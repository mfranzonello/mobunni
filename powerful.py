# definitions for power and efficiency curves, power modules, hot boxes and energy servers

from pandas import DataFrame, Series, concat
from numpy import random as nprandom

from debugging import StopWatch

# power curves for a model type
class PowerCurves:
    def __init__(self, curves):
        self.curves = curves
        self.percentiles = curves.columns.to_list()
        
        self.ideal = max(self.percentiles)
        self.worst = min(self.percentiles)

        self.probabilities = self.get_probabilities(self.percentiles)

    # calculate probability of each percentile
    def get_probabilities(self, percentiles):
        probabilities = DataFrame(data=[0] + percentiles, columns=['percentile'])
        probabilities.loc[0, 'top'] = 0

        for i in range(1, len(probabilities)):
            probabilities.loc[i, 'top'] = 2 * probabilities.loc[i, 'percentile'] - probabilities.loc[i-1, 'top']
        probabilities.loc[:, 'probability'] = probabilities['top'].diff()
        probabilities.dropna(inplace=True)
        probabilities.index = percentiles
        probabilities = probabilities['probability']

        return probabilities

    # return range of curves probable based on current observation
    def get_allowed_curves(self, allowed=[0, 1], fit=None):
        if fit is None:
            # new FRU
            if allowed == 'ideal':
                allowed_curves = self.curves[[self.ideal]]
            elif allowed == 'worst':
                allowed_curves = self.curves[[self.worst]]
            else:
                allowed_curves = self.curves[[percentile for percentile in self.percentiles \
                    if (percentile >= allowed[0]) & (percentile <= allowed[-1])]]

        else:
            # FRU has already been in the field, find least error
            max_operating_time = min(len(self.curves)-1, fit['operating time'])
            if ('performance' in fit) and ('operating time' in fit):
                # pulled from API
                to_fit = fit['performance'].iloc[len(fit['performance'])-fit['operating time']:, :].reset_index()['kw']

                errors = self.curves.loc[0:len(fit)-1, :].T.sub(to_fit).T.pow(2).sum()

                filtered_curves = self.curves.loc[:, errors[errors == errors.min()].index.to_list()].columns

                allowed_curves = DataFrame(data=[fit['performance']['kw'].to_list() + self.curves.loc[max_operating_time:, c].to_list() for c in filtered_curves],
                                           index=filtered_curves).T

            elif ('operating time' in fit) and ('current power' in fit):
                # blind to starting curve
                expected_range = self.curves.loc[max_operating_time]
                observed_power = fit['current power']

                if observed_power > expected_range.max():
                    # operating better than expected, so choose ideal curve
                    allowed_curves = self.get_allowed_curves(allowed='ideal').copy()

                elif observed_power < expected_range.min():
                    # operating worse than expected, so choose worst curve and scale down
                    allowed_curves = self.get_allowed_curves(allowed='worst').copy()
                    allowed_curves.loc[0:, :] *= (observed_power / expected_range.min())

                else:
                    # operating in expected range, so choose from range of possibilities
                    allowed_curves = self.curves[\
                        ((self.curves.loc[max_operating_time] >= fit['current power']) & \
                         (self.curves.loc[max_operating_time] <= fit['current power'])).index]
        
        return allowed_curves
        
    # normalize probabilities for percentile selection
    def normalize_probabilties(self, percentiles):
        probabilities = self.probabilities.loc[percentiles]

        probabilities_normalized = [p/probabilities.sum() for p in probabilities]
        return probabilities_normalized

    # pick power curve for power module
    def pick_curve(self, allowed=[0, 1], fit=None):
        allowed_curves = self.get_allowed_curves(allowed, fit)

        probabilities_normalized = self.normalize_probabilties(allowed_curves.columns)
        chosen_percentile = nprandom.choice(allowed_curves.columns.to_list(), p=probabilities_normalized)
        
        curve = allowed_curves[chosen_percentile]
        
        # remove months where power is gone
        curve = curve[curve != 0].dropna()
       
        return curve

    # get expected power curve of a power module
    def get_expected_curve(self, operating_time=0, observed_power=0):
        if operating_time == 0:
            fit = None
        else:
            fit = {'operating time': operating_time, 'current power': observed_power}

        allowed_curves = self.get_allowed_curves(fit=fit)
        probabilities_normalized = self.normalize_probabilties(allowed_curves.columns)
        expected_curve = allowed_curves.loc[operating_time:].mul(probabilities_normalized).sum('columns')

        return expected_curve

    # get expected energy for time period
    def get_expected_energy(self, operating_time=0, observed_power=0, time_needed=0):
        expected_curve = self.get_expected_curve(operating_time, observed_power)
        if (time_needed > 0) and (time_needed <= len(expected_curve) - operating_time):
            energy = expected_curve.iloc[operating_time:operating_time+time_needed].sum()
        else:
            energy = expected_curve.iloc[operating_time:].sum() ## numpy.float64 error
        return energy

# efficiency curves for a model type
class EfficiencyCurves:
    def __init__(self, curve):
        self.curve = curve

    def pick_curve(self, fit=None):
        if (fit is not None) and (('performance' in fit) and ('operating time' in fit)):
            # pulled from API
            curve = Series(data=fit['performance']['pct'].to_list() + self.curve.loc[fit['operating time']:].to_list())

        else:
            curve = self.curve.copy()

        return curve


# details of power modules
class PowerModules:
    def __init__(self, sql_db):
        self.sql_db = sql_db

    # get power and efficiency curves
    def get_curves(self, model, mark, model_number):
        power_curves = PowerCurves(self.sql_db.get_power_curves(model, mark, model_number))
        efficiency_curves = EfficiencyCurves(self.sql_db.get_efficiency_curve(model, mark, model_number))
        return power_curves, efficiency_curves

    # find best new power module available
    def get_model(self, install_date, wait_period=None, power_needed=0, max_power=None, energy_needed=0, time_needed=0, best=False,
                  server_model=None, roadmap=None, match_server_model=False):

        if roadmap is None:
            allowed = self.sql_db.get_default_modules()
        else:
            allowed = roadmap

        buildable_modules = self.sql_db.get_buildable_modules(install_date, server_model=server_model, allowed=allowed, wait_period=wait_period)
        
        if match_server_model and (server_model is not None):
            buildable_modules.query('model == @server_model', inplace=True)

            if not len(buildable_modules):
                print('{} not available on {}, using earliest possible module'.format(server_model, install_date))
                buildable_modules = self.sql_db.get_buildable_modules(install_date=0, server_model=server_model, allowed=allowed)
                buildable_modules.query('model == @server_model', inplace=True)
        
        if not buildable_modules.empty:
            buildable_modules.loc[:, 'rating'] = buildable_modules.apply(lambda x: self.get_rating(x['model'], x['mark'], x['model_number']),
                                                                         axis='columns')

            buildable_modules.loc[:, 'energy'] = buildable_modules.apply(lambda x: self.get_energy(x['model'], x['mark'], x['model_number'], time_needed),
                                                                         axis='columns')

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
    def get_rating(self, model, mark, model_number):
        rating = self.sql_db.get_module_rating(model, mark, model_number)
        return rating

    # return initial power rating of all marks of a module
    def get_ratings(self, model, install_date):
        marks = self.get_marks(model, install_date)
        ratings = [self.get_rating(model, mark) for mark in marks]
        return ratings

    # return expected energy output of a given model
    def get_energy(self, model, mark, model_number, time_needed):
        curves = PowerCurves(self.sql_db.get_power_curves(model, mark, model_number))

        energy = curves.get_expected_energy(time_needed=time_needed)
        return energy

    # return expected energy output of all model versions of a module
    def get_energies(self, model, mark, install_date, time_needed):
        model_numbers = self.sql_db.get_module_model_numbers(model, mark)
        energies = [self.get_energy(model, mark, model_number, time_needed) for model_number in model_numbers]
        return energies

# details of energy enclosures
class HotBoxes:
    def __init__(self, sql_db):
        self.sql_db = sql_db

    def get_model_number(self, server_model):
        model_number, rating = self.sql_db.get_enclosure_model_number(server_model)
        return model_number, rating

# details of energy servers
class EnergyServers:
    def __init__(self, sql_db):
        self.sql_db = sql_db

    # get power modules that work with energy server
    def get_compatible_modules(self, server_model):
        allowed_modules = self.sql_db.get_compatible_modules(server_model)
        return allowed_modules

    # get base model of a server model number
    def get_server_model(self, **kwargs):
        server_model = self.sql_db.get_server_model(**kwargs)
        return server_model

    # get lasted server model class
    def get_latest_server_model(self, install_date, target_model):
        latest_server_model_class = self.sql_db.get_latest_server_model(install_date, target_model=target_model)
        return latest_server_model_class
        
    # get default nameplate sizes   
    def get_server_nameplates(self, latest_server_model_class, target_size):
        server_nameplates = self.sql_db.get_server_nameplates(latest_server_model_class, target_size)
        return server_nameplates