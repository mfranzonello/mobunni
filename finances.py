# banking functions for costs

class Bank:
    def __init__(self, sql_db):
        self.sql_db = sql_db

    def get_cost(self, date, action, component=None, **kwargs):
        if component is not None:
            kwargs['model'] = getattr(component, 'model', None)
            kwargs['mark'] = getattr(component, 'base', getattr(component, 'mark', None))

        cost = self.sql_db.get_cost(action, date, **kwargs)
        return cost