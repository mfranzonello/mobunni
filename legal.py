# contracts


# collection of contracts across sites
class Portfolio:
    def __init__(self):
        self.contracts = []
        self.number = 0

    def generate_contract(self, deal, length, target_size, start_date, start_month,
                 non_replace, limits, start_ctmo=1.0):
        self.number += 1
        contract = Contract(self.number, deal, length, target_size, start_date, start_month,
                            non_replace, limits, start_ctmo)
        self.contracts.append(contract)
        return contract

# legal commitments for a site or group of sites
class Contract:
    limits_values = ['PTMO', 'WTMO', 'CTMO', 'Peff', 'Weff', 'Ceff', 'window']
    def __init__(self, number, deal, length, target_size, start_date, start_month,
                 non_replace, limits, start_ctmo):
        self.number = number
        self.deal = deal

        self.length = length
        self.target_size = target_size
        self.start_date = start_date
        self.start_month = start_month
        self.non_replace = non_replace
        self.limits = limits
        self.windowed = (limits['WTMO'] or limits['Weff']) and limits['window']
        self.start_ctmo = start_ctmo

    # change the terms of the contract
    def change_terms(self, **kwargs):
        contract = Contract(length=kwargs.get('length', self.length),
                            target_size=kwargs.get('length', self.target_size),
                            start_date=kwargs.get('length', self.start_date),
                            start_month=kwargs.get('start_month', self.start_month),
                            non_replace=kwargs.get('non_replace', self.non_replace),
                            limits={value: kwargs.get(value, self.limits[value]) for value in Contract.limits_values},
                            start_ctmo=self.start_ctmo)

        return contract

    # FRUs can be installed during given year of contract
    def is_replaceable_year(self, year):
        downside = (self.non_replace is None) or (len(self.non_replace) == 0) or \
            not (self.non_replace[0] <= year <= self.non_replace[-1])

        return downside
