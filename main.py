# main script to read inputs, set up structure, run simulation and print results

# inputs
structure_db = {1: 'sqlite-local', # locally stored SQLite
                2: 'sqlite-network', # network stored SQLite
                }[2]

open_results = True # open Excel file when done running

# add-on imports
from structure import Project, SQLDB
from layout import APC, ExistingServers, NewServers
from xl_inputs import ExcelInt, ExcelSQL
from xl_outputs import Excelerator, ExcelePaint
from groups import Details, Commitments, Technology, Tweaks, Thresholds
from simulate import Scenario, Simulation

# service cost model
class ServiceModel:
    '''
    Main object to run fleet management service cost generation.
    <<FUTURE>> This script should become an application
    '''
    version = 3
    welcome_text = ['Bloom Service Cost Model v{}'.format(version),
                    '~ created by Michael Franzonello et al ~']

    ''' START UP FUNCTIONS '''

    def welcome():
        print('\n'.join(ServiceModel.welcome_text))
        print()

    ''' ALL CASE FUNCTIONS '''

    # ask for project
    def get_project() -> Project:
        '''
        This function can be replaced with a web-based input UI.
        It uses the command prompt to ask the user which Excel input file to read
        and returns values for each scenario.
        '''
        project = Project() # class to get project with slight UI
        project.ask_project() # get project
        
        return project

    # read database
    def get_database(structure_db: str) -> SQLDB:
        '''
        This function sets up a connection to the database for cost values,
        power and efficiency curves, compatibility, etc and for writing new
        data.
        '''
        sql_db = SQLDB(structure_db)
        return sql_db

    ''' SIMULATOR FUNCTIONS '''

    # get inputs for simulator
    def get_inputs(project: Project) -> ExcelInt:
        '''
        This function pulls from excel file for inputs for a project
        to be simulaton.
        <<FUTURE>> This should be replaced with a web-based UI.
        '''
        excel_int = ExcelInt(project.path) # pull values from Excel file for corresponding project
        return excel_int

    # read structure
    def get_structure(sql_db: SQLDB) -> [SQLDB, Thresholds]:
        '''
        This function retrieves special threshold values from the database.
        values.
        '''
        thresholds = Thresholds(sql_db.get_thresholds())
        return thresholds

    # build details
    def get_details(excel_int: ExcelInt) -> Details:
        '''
        This function gets values for the refinement of the monte-carlo
        simulations and the total number of scenarios to run.
        '''
        print ('Getting project details')
        n_sites, n_years, n_runs = excel_int.get_details()
        n_scenarios = excel_int.count_scenarios()
        details = Details(n_sites, n_years, n_runs, n_scenarios)

        return details

    # build scenario
    def get_scenario(excel_int: ExcelInt, scenario_number: int, apc: APC):
        '''
        This function gets the specific values for each monte-carlo siulation
        including contract details, site layout and technology roadmap.
        It downloads from APC-TMO if required if there is a network connection.
        '''
        print('Getting scenario {} details'.format(scenario_number+1))
        scenario_name, limits, start_date, contract_length, contract_deal, non_replace, \
            site_code, servers, roadmap, multiplier, \
            repair, redeploy, best, early_deploy, eoc_deploy = excel_int.get_scenario(scenario_number)

        existing_servers = ExistingServers(apc.get_site_performance(site_code))
        new_servers = NewServers(servers)

        if existing_servers.exist():
            # model an existing site
            target_size = existing_servers.get_size()
            start_date, start_month = existing_servers.get_dates() # replace start date with API value
        elif new_servers.exist():
            # model a new site
            target_size = new_servers.get_size()
            start_month = 0
            print(new_servers)

        if existing_servers.exist() or new_servers.exist():
            # there is something to model
            commitments = Commitments(length=contract_length, target_size=target_size, start_date=start_date,
                                      start_month=start_month, non_replace=non_replace, limits=limits, deal=contract_deal)

            technology = Technology(new_servers=new_servers, existing_servers=existing_servers, roadmap=roadmap, site_code=site_code, multiplier=multiplier)
            tweaks = Tweaks(repair=repair, redeploy=redeploy, best=best, early_deploy=early_deploy, eoc_deploy=eoc_deploy)

        else:
            # there is nothing to model
            commitments, technology, tweaks = [None]*3
            
        scenario = Scenario(scenario_number, scenario_name, commitments, technology, tweaks)
        return scenario

    # run simulation
    def run_simulation(details: Details, scenario: Scenario, sql_db: SQLDB, thresholds: Thresholds) -> Simulation:
        '''
        This function runs a simulation of a specific scenario.
        The results of the simulation are stored within the object.
        '''
        simulation = Simulation(details, scenario, sql_db, thresholds)
        simulation.run_scenario()
        return simulation

    # run scenarios
    def run_scenarios(project: Project, excel_int: ExcelInt, details: Details, sql_db: SQLDB, thresholds: Thresholds, apc: APC):
        '''
        This function runs each scenario through the simulator and
        stores the specific results.
        '''
        for scenario_number in range(details.n_scenarios):
            scenario = ServiceModel.get_scenario(excel_int, scenario_number, apc)
        
            if scenario.is_runnable():
                # run simulation
                simulation = ServiceModel.run_simulation(details, scenario, sql_db, thresholds)
                ServiceModel.save_results(project, scenario, simulation)
            else:
                # not enough details
                print('Not enough details in scenario or missing connection ... skipping!')

    # output results
    def save_results(project: Project, scenario: Scenario, simulation: Simulation):
        '''
        This function takes the stored values in a simulation and
        packages them in an Excel format.
        <<FUTURE>> Some of this can be replaced with a web-based output UI.
        '''
        inputs, site_performance, cost_tables, fru_power, fru_efficiency, transactions, cash_flow = simulation.get_results()
    
        # set up output
        excelerator = Excelerator(path=None, filename='bpm_results_{}_{}'.format(project.name, scenario.name), extension='xlsx')
    
        # assemble output
        data, print_index, formats, charts, tabs = ExcelePaint.get_paints(scenario.windowed, scenario.commitments.limits, inputs,
                                                                          site_performance, cost_tables, fru_power, fru_efficiency, transactions, cash_flow)
        excelerator.store_data(data, print_index)
        excelerator.store_formats(formats)
        excelerator.store_charts(charts)
        excelerator.store_tabs(tabs)
        excelerator.to_excel(start=open_results)

    ''' DATA ADDING FUNCTIONS '''

    # get inputs for new data
    def get_data(project: Project, sql_db: SQLDB) -> ExcelSQL:
        '''
        This function gets module data and power and efficiency curves
        from an Excel input to add to the database.
        <<FUTURE>> This should be replaced with a web-based UI.
        '''
        excel_sql = ExcelSQL(project.path, sql_db) # pull values from Excel file for new data
        return excel_sql

    # add new data to database
    def add_data(excel_sql: ExcelSQL):
        '''
        This function adds module data and power and efficiency curves
        to the database. It converts matrix curve inputs to single arrays.
        <<FUTURE>> This should be replaced with a web-based UI.
        '''
        print('Updating database')
        excel_sql.import_all_curves()
        return

    ''' MAIN FUNCTON '''

    # main code
    def run_model():
        '''
        This function excutes the functions above.
        '''
        ServiceModel.welcome()

        project  = ServiceModel.get_project()

        if project.file_type is not None:
            # project runnable
            sql_db = ServiceModel.get_database(structure_db)

        if project.file_type == 'simulation':
            # project is simulation
            excel_int = ServiceModel.get_inputs(project)
            thresholds = ServiceModel.get_structure(sql_db)
            details = ServiceModel.get_details(excel_int)
            apc = APC(sql_db)
            apc.add_to_db()
            
            ServiceModel.run_scenarios(project, excel_int, details, sql_db, thresholds, apc)

        elif project.file_type == 'database':
            # project is adding data
            excel_sql = ServiceModel.get_data(project, sql_db)

            ServiceModel.add_data(excel_sql)

if __name__ == '__main__':
    ServiceModel.run_model()