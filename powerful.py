# definitions for power and efficiency curves, power modules and energy servers

import pandas
from numpy import random as nprandom

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
        probabilities = pandas.DataFrame(data=[0] + percentiles, columns=['percentile'])
        probabilities.loc[0, 'top'] = 0

        for i in range(1, len(probabilities)):
            probabilities.loc[i, 'top'] = 2*probabilities.loc[i, 'percentile'] - probabilities.loc[i-1, 'top']
        probabilities.loc[:, 'probability'] = probabilities['top'].diff()
        probabilities.dropna(inplace=True)
        probabilities.index = percentiles
        probabilities = probabilities['probability']

        return probabilities

    # return range of curves probable based on current observation
    def get_allowed_curves(self, allowed=[0,1], fit=None):
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
            # FRU has alreay been in the field
            expected_range = self.curves.loc[min(len(self.curves)-1, fit['operating time'])]
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
                    ((self.curves.loc[fit['operating time']] >= fit['current power']) & \
                     (self.curves.loc[fit['operating time']] <= fit['current power'])).index]

        return allowed_curves
        
    # normalize probabilities for percentile selection
    def normalize_probabilties(self, percentiles):
        probabilities = self.probabilities.loc[percentiles]

        probabilities_normalized = [p/probabilities.sum() for p in probabilities]
        return probabilities_normalized

    # pick power curve for power module
    def pick_curve(self, allowed=[0,1], fit=None):
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
            energy = expected_curve.iloc[operating_time:].sum()
        return energy

# details of power modules
class PowerModules:
    def __init__(self, sql_db):
        self.sql_db = sql_db

    # find best new power module available
    def get_model(self, install_date, power_needed=0, energy_needed=0, time_needed=0, best=False, server_model=None, allowed_fru_models=None): ## FILTERED
        buildable_modules = self.sql_db.get_buildable_modules(install_date, server_model=server_model, allowed=allowed_fru_models)

        buildable_modules.loc[:, 'rating'] = buildable_modules.apply(lambda x: self.get_rating(x['model'], x['mark']),
                                                                     axis='columns')
        buildable_modules.loc[:, 'energy'] = buildable_modules.apply(lambda x: self.get_energy(x['model'], x['mark'], time_needed),
                                                                     axis='columns')

        # check power requirements
        max_rating = buildable_modules['rating'].max()
        if (max_rating >= power_needed) and (not best):
            # if there is a model big enough to handle the load, choose the 
            filtered_modules = buildable_modules[buildable_modules['rating'] >= power_needed]
        else:
            # choose the biggest model available
            filtered_modules = buildable_modules[buildable_modules['rating'] == max_rating]

        # check energy requirements
        if energy_needed > 0:
            filter = filtered_modules.apply(lambda x: self.get_energy(x['model'], x['mark'], time_needed) >= energy_needed,
                                                                      axis='columns')
            filtered_modules = filtered_modules[filter]

        module = filtered_modules.iloc[0, :]
        model = module['model']
        mark = module['mark']

        return model, mark

    # find bespoke options for a power module model
    def get_bespokes(self, model, base, install_date):
        marks = [base] + self.sql_db.get_module_bespokes(model, base, install_date)
        return marks

    # return initial power rating of a given module
    def get_rating(self, model, mark):
        rating = self.sql_db.get_module_rating(model, mark)
        return rating

    # return initial power rating of all marks of a module
    def get_ratings(self, model, install_date):
        marks = self.get_marks(model, install_date)
        ratings = [self.get_rating(model, mark) for mark in marks]
        return ratings

    # return expected energy output of a given model
    def get_energy(self, model, mark, time_needed):
        curves = PowerCurves(self.sql_db.get_power_curves(model, mark))
        energy = curves.get_expected_energy(time_needed=time_needed)
        return energy

    # return expected energy output of all marks of a module
    def get_energies(self, model, install_date, time_needed):
        marks = self.get_marks(model, install_date)
        energies = [self.get_energy(model, mark, time_needed) for mark in marks]
        return energies

# details of energy servers
class EnergyServers:
    pass