from secret import passcodes

class URL:
    databases = {'sqlite': {'parameters': {'connector': 'sqlite',
                                           'database': 'bpm'},
                            'string format': '{connector}:///{database}.db'},
                            
                 'remotemysql': {'parameters': {'connector': 'mysql',
                                                'username': passcodes['remotemysql']['username'],
                                                'password': passcodes['remotemysql']['password'],
                                                'host': 'remotemysql.com',
                                                'port': '3306',
                                                'database': passcodes['remotemysql']['username']},
                                 'string format': '{connector}://{username}:{password}@{host}:{port}/{database}'},

                 'mysql': {'parameters': {'connector': 'mysql',
                                          'username': passcodes['mysql']['username'],
                                          'password': passcodes['mysql']['password'],
                                          'host': '127.0.0.1',
                                          'port': '3306',
                                          'database': 'servicecosts'},
                           'string format': '{connector}://{username}:{password}@{host}:{port}/{database}'},
                 }

    apc_tmo = {'host': 'https://tmo-portal.ionamerica.priv',
               'port': '4433'}

    def get_database(db_type: str):
        url = URL.databases[db_type]['string format'].format(**URL.databases[db_type]['parameters'])

        return url

    def get_apc():
        url ='{host}'.format(**URL.apc_tmo)
        endpoint = '{host}:{port}'.format(**URL.apc_tmo)

        return url, endpoint