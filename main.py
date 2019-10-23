# main script to read inputs, set up structure, run simulation and print results

# inputs
structure_db = {1: 'sqlite-local', # locally stored SQLite
                2: 'sqlite-network', # network stored SQLite`
                3: 'remotemysql', # web-based MySQL
                4: 'mysql', # server-based MySQL
                }[2]

open_results = True # open Excel file when done running

# built-in imports
from math import floor

# add-on imports
from structure import Project, SQLDB
from layout import APC, ExistingServers, NewServers
from xl_inputs import ExcelInt
from xl_outputs import Excelerator, ExcelePaint
from groups import Details, Commitments, Technology, Tweaks, Thresholds
from simulate import Scenario, Simulation

# service cost model
class ServiceModel:
    '''
    Main object to run fleet management service cost generation.
    '''

    # ask for project
    def get_project() -> [Project, ExcelInt]:
        '''
        This function can be replaced with a web-based input UI.
        It uses the command prompt to ask the user which Excel input file to read
        and returns values for each scenario.
        '''
        project = Project() # class to get project with slight UI
        project.ask_project() # get project
        excel_int = ExcelInt(project.path) # pull values from Excel file for corresponding project
        return project, excel_int

    # read structure
    def get_structure(structure_db: str) -> [SQLDB, Thresholds]:
        '''
        This function sets up a connection to the database for cost values,
        power and efficiency curves, compatibility, etc, and special threshold
        values.
        '''
        sql_db = SQLDB(structure_db)
        thresholds = Thresholds(sql_db.get_thresholds())
        return sql_db, thresholds

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
            target_size = existing_servers.get_size()
            start_date, start_month = existing_servers.get_dates() # replace start date with API value
        elif new_servers.exist():
            target_size = new_servers.get_size()
            start_month = 0
            print(new_servers)
        else:
            target_size = 0
            start_month = 0

        commitments = Commitments(length=contract_length, target_size=target_size, start_date=start_date,
                                  start_month=start_month, non_replace=non_replace, limits=limits, deal=contract_deal)

        technology = Technology(new_servers=new_servers, existing_servers=existing_servers, roadmap=roadmap, site_code=site_code, multiplier=multiplier)
        tweaks = Tweaks(repair=repair, redeploy=redeploy, best=best, early_deploy=early_deploy, eoc_deploy=eoc_deploy)

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
        Some of this can be replaced with a web-based output UI.
        '''
        inputs, site_performance, cost_tables, fru_power, fru_efficiency, transactions, cash_flow = simulation.get_results()
    
        # set up output
        excelerator = Excelerator(path=None, filename='bpm_results_{}_{}'.format(project.name, scenario.name), extension='xlsx')
    
        # assemble output
        data, print_index, formats, charts = ExcelePaint.get_paints(scenario.windowed, scenario.commitments.limits, inputs,
                                                                    site_performance, cost_tables, fru_power, fru_efficiency, transactions, cash_flow)
        excelerator.store_data(data, print_index)
        excelerator.store_formats(formats)
        excelerator.store_charts(charts)
        excelerator.to_excel(start=open_results)

    # main code
    def run_model():
        '''
        This function excutes the functions above.
        '''
        project, excel_int = ServiceModel.get_project()
        sql_db, thresholds = ServiceModel.get_structure(structure_db)
        details = ServiceModel.get_details(excel_int)
        apc = APC(sql_db)
        apc.add_to_db()

        ServiceModel.run_scenarios(project, excel_int, details, sql_db, thresholds, apc)


if __name__ == '__main__':
    print('Bloom Service Cost Model')
    ServiceModel.run_model()