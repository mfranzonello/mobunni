# banking functions for costs

# built-in imports
from typing import Tuple, Dict

# add-on imports
from pandas import DataFrame
from datetime import date

# built-in imports
from structure import SQLDB
from components import Component

# assign costs during simulation
class Bank:
    def __init__(self, sql_db: SQLDB):
        self.sql_db = sql_db

    # pull cost from database
    def get_cost(self, action_date: date, action: str, component: Component = None, **kwargs) -> float:
        if component is not None:
            kwargs['model'] = getattr(component, 'model', None)
            kwargs['mark'] = getattr(component, 'mark', None)

        cost = self.sql_db.get_cost(action, action_date, **kwargs)
        return cost

# assign fleet costs for cash flow
class Cash:
    def __init__(self, sql_db: SQLDB):
        self.sql_db = sql_db

    # pull start cost and escalator from database
    def get_escalator(self, item: str, action_date: date, escalator_basis: str= None) -> Tuple[float, float]:
        start_value, escalator = self.sql_db.get_line_item(item, action_date, escalator_basis)
        return start_value, escalator

    # generate cash flow and transpose
    def generate_cash_flow(self, cost_tables: Dict[str, DataFrame], size: float, years: list) -> DataFrame:
        first_columns = ['year', 'fru replacement schedule', 'fru replacement kw']
        cost_columns = {'fru costs': ['fru replacement costs',
                                      'fru repair costs',
                                      'fru overhaul costs',
                                      'fru deployment costs',
                                      'component costs',
                                      ],
                        'non-fru costs': ['department spend',
                                          'other maintenance',
                                          ]}
        last_columns = ['total costs', '$/kW']


        escalator_bases = {'fru costs': {'item': 'fru material cost',
                                         'escalator': 'acceptances'}}

        # drop total rows
        dollars = cost_tables['dollars'].iloc[:-1]
        quants = cost_tables['quants'].iloc[:-1]
        power = cost_tables['power'].iloc[:-1]

        cash_flow = DataFrame(columns=first_columns + [x for y in [cost_columns[col] + ['total {}'.format(col)] for col in cost_columns] for x in y])

        # get FRU costs (from simulation)
        cash_flow.loc[:, 'year'] = quants['year']
        if 'created FRU' in quants.columns:
            cash_flow.loc[:, 'fru replacement schedule'] = quants['created FRU'].values
            cash_flow.loc[:, 'fru replacement costs'] = dollars['created FRU'].values
        for sub_col in ['created FRU', 'deployed FRU']:
            if sub_col in power.columns:
                cash_flow.loc[:, 'fru replacement kw'] += power[sub_col].values
        if 'repaired FRU' in dollars.columns:
            cash_flow.loc[:, 'fru repair costs'] = dollars['repaired FRU'].values
        if 'overhauled FRU' in dollars.columns:
            cash_flow.loc[:, 'fru overhaul costs'] = dollars['repaired FRU'].values
        for sub_col in ['deployed FRU', 'stored FRU', 'pulled FRU', 'moved FRU']:
            if sub_col in dollars.columns:
                cash_flow.loc[:, 'fru deployment costs'] += dollars[sub_col].values
        for sub_col in ['upgraded ES', 'increased ENC']:
            if sub_col in dollars.columns:
                cash_flow.loc[:, 'component costs'] += dollars[sub_col].values

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
        cash_flow = cash_flow.query('year in @years').T
        cash_flow.columns = range(1, cash_flow.shape[-1] + 1)

        cash_flow = cash_flow.dropna(how='all').fillna(0)

        return cash_flow
