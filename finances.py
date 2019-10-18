# banking functions for costs

from pandas import DataFrame
from datetime import date

# assign costs during simulation
class Bank:
    def __init__(self, sql_db):
        self.sql_db = sql_db

    # pull cost from database
    def get_cost(self, date, action, component=None, **kwargs):
        if component is not None:
            kwargs['model'] = getattr(component, 'model', None)
            kwargs['mark'] = getattr(component, 'mark', None)

        cost = self.sql_db.get_cost(action, date, **kwargs)
        return cost

# assign fleet costs for cash flow
class Cash:
    def __init__(self, sql_db):
        self.sql_db = sql_db

    # pull start cost and escalator from database
    def get_escalator(self, item, date, escalator_basis=None):
        start_value, escalator = self.sql_db.get_line_item(item, date, escalator_basis)
        return start_value, escalator

    # generate cash flow and transpose
    def generate_cash_flow(self, cost_tables, size):
        first_columns = ['year', 'fru replacement schedule']
        cost_columns = {'fru costs': ['fru replacement costs',
                                      'fru repair costs',
                                      'fru deployment costs'],
                        'non-fru costs': ['department spend',
                                          'other maintenance']}
        last_columns = ['total costs', '$/kW']


        escalator_bases = {'fru costs': {'item': 'fru material cost',
                                         'escalator': 'acceptances'}}

        # drop total rows
        dollars = cost_tables['dollars'].iloc[:-1]
        quants = cost_tables['quants'].iloc[:-1]

        cash_flow = DataFrame(columns=first_columns + [x for y in [cost_columns[col] + ['total {}'.format(col)] for col in cost_columns] for x in y])

        # get FRU costs (from simulation)
        cash_flow.loc[:, 'year'] = quants['year']
        if 'created FRU' in quants.columns:
            cash_flow.loc[:, 'fru replacement schedule'] = quants['created FRU'].values
            cash_flow.loc[:, 'fru replacement costs'] = dollars['created FRU'].values
        if 'repaired FRU' in dollars.columns:
            cash_flow.loc[:, 'fru repair costs'] = dollars['repaired FRU'].values

        for sub_col in ['deployed FRU', 'stored FRU']:
            if sub_col in dollars.columns:
                cash_flow.loc[:, 'fru deployment costs'] += dollars[sub_col].values

        # get non-FRU costs (from per MW)
        for sub_col in cost_columns['non-fru costs']:
            escalation = DataFrame(columns=['year', 'start value', 'escalator'], index=cash_flow.index)

            escalation.loc[:, ['start value', 'escalator']] = \
                cash_flow['year'].apply(lambda x: self.get_escalator(sub_col, date(x, 1, 1))).fillna(method = 'ffill').to_list()

            cash_flow.loc[:, sub_col] = escalation['start value'].mul(escalation['escalator'].fillna(0).add(1).cumprod()).div(size/1000)

        # sum FRU costs and sum non-FRU costs
        for col in cost_columns:
            cash_flow.loc[:, 'total {}'.format(col)] = cash_flow.loc[:, cost_columns[col]].sum(axis=1)

        # sum all costs
        cash_flow.loc[:, 'total costs'] = cash_flow[['total {}'.format(col) for col in cost_columns]].sum(axis=1)
        cash_flow.loc[:, '$/kw'] = cash_flow['total costs'].div(size)

        # transpose
        cash_flow = cash_flow.T
        cash_flow.columns = range(1, cash_flow.shape[-1] + 1)

        return cash_flow
