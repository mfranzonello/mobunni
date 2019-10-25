# class for interatcting with networked databases and APIs

# built-in imports
from typing import Tuple

class URL:
    databases = {'sqlite-local': {'parameters': {'connector': 'sqlite',
                                                 'database': 'bpm'},
                                  'string format': '{connector}:///{database}.db'},

                 'sqlite-network': {'parameters': {'connector': 'sqlite',
                                                   'database': 'mobunni',
                                                   'path': r'\\Denali\Departments\Marketing\Product_Management\Service Costs'},
                                    'string format': '{connector}:///{path}\{database}.db'},
                 }

    apc_tmo = {'host': 'https://tmo-portal.ionamerica.priv',
               'port': '4433'}

    def get_database(db_type: str) -> str:
        url = URL.databases[db_type]['string format'].format(**URL.databases[db_type]['parameters'])

        return url

    def get_apc() -> Tuple[str, str]:
        url ='{host}'.format(**URL.apc_tmo)
        endpoint = '{host}:{port}'.format(**URL.apc_tmo)

        return url, endpoint