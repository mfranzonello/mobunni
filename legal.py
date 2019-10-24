# contracts

# built-in imports
from __future__ import annotations
from datetime import date

# add-on imports
from pandas import DataFrame

# self-defined imports
from structure import SQLDB

# legal commitments for a site or group of sites
class Contract:
    limits_values = ['PTMO', 'WTMO', 'CTMO', 'Peff', 'Weff', 'Ceff', 'window']
    def __init__(self, number:int, deal:str, length:int, target_size:float, start_date:date, start_month:int, limits:dict):
        self.number = number
        self.deal = deal

        self.length = length
        self.target_size = target_size
        self.start_date = start_date
        self.start_month = start_month

        self.limits = limits
        self.windowed = (limits['WTMO'] or limits['Weff']) and limits['window']

    # change the terms of the contract
    def change_terms(self, **kwargs) -> Contract:
        contract = Contract(length=kwargs.get('length', self.length),
                            target_size=kwargs.get('length', self.target_size),
                            start_date=kwargs.get('length', self.start_date),
                            start_month=kwargs.get('start_month', self.start_month),
                            limits={value: kwargs.get(value, self.limits[value]) for value in Contract.limits_values})

        return contract

    # FRUs can be installed during given year of contract
    def is_replaceable_time(self, **kwargs) -> bool:
        replaceable = all([kwargs.get('month', 0) >= self.start_month,
                           any([kwargs['eoc']['allowed'],
                                kwargs.get('years_remaining') >= kwargs['eoc']['years']])])

        return replaceable

# collection of contracts across sites
class Portfolio:
    limits_values = Contract.limits_values
    def __init__(self, sql_db:SQLDB):
        self.contracts = []
        self.number = 0
        self.sql_db = sql_db

    def get_number(self) -> int:
        self.number += 1
        number = self.number
        return number

    def generate_contract(self, target_size:float, start_date:date, start_month:int,
                          deal:str=None, length:int=None, limits:dict=None) -> Contract:

        number = self.get_number()

        if deal is None:
            contract_values = self.sql_db.get_contract()
            deal = contract_values.get('deal', 'CapEx')
            length = contract_values.get('length', 10)
            limits = {lv: contract_values.get(lv) for lv in Portfolio.limits_values}

        contract = Contract(number, deal, length, target_size, start_date, start_month, limits)
        self.contracts.append(contract)

        return contract

