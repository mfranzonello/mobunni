# main script to read inputs, set up structure, run simulation and print results

from structure import Project, SQLDB, ExcelInt, Excelerator
from groups import Details, Commitments, Technology, Tweaks
from simulate import Scenario, Simulation

from debugging import StopWatch

# inputs
structure_db = 'bpm.db'

# ask for project
def get_project():
    project = Project()
    project.ask_project()
    excel_int = ExcelInt(project.path)
    return project, excel_int

# read structure
def get_structure(structure_db):
    print('Reading structure database')
    sql_db = SQLDB(structure_db)
    return sql_db

# build details
def get_details(excel_int):
    print ('Getting project details')
    n_sites, n_years, n_runs = excel_int.get_details()
    n_scenarios = excel_int.count_scenarios()
    details = Details(n_sites, n_years, n_runs, n_scenarios)

    return details

# build scenario
def get_scenario(excel_int, scenario_number):
    print('Getting scenario {} details'.format(scenario_number+1))
    scenario_name, limits, target_size, start_date, contract_length, start_month, \
        non_replace, repair, junk_level, best, \
        new_servers, existing_servers, allowed_fru_models = excel_int.get_scenario(scenario_number)

    commitments = Commitments(length=contract_length, target_size=target_size, start_date=start_date,
                              start_month=start_month, non_replace=non_replace, limits=limits)
    technology = Technology(new_servers=new_servers, existing_servers=existing_servers, allowed_fru_models=allowed_fru_models)
    tweaks = Tweaks(repair=repair, junk_level=junk_level, best=best)

    scenario = Scenario(scenario_number, scenario_name,
                        commitments=commitments, technology=technology, tweaks=tweaks)
    return scenario

# run simulation
def run_simulation(details, scenario, sql_db):
    simulation = Simulation(details, scenario, sql_db)
    simulation.run_scenario()
    return simulation

# output results
def save_results(project, scenario, simulation):
    inputs, site_performance, residuals, costs, fru_power, fru_efficiency, transactions = simulation.get_results()
    excelerator = Excelerator(path=None, filename='bpm_results_{}_{}'.format(project.name, scenario.name), extension='xlsx')  
    excelerator.add_sheets({'Inputs': inputs,
                            'Power+Eff (avg)': site_performance['mean'],
                            'Power+Eff (max)': site_performance['max'],
                            'Power+Eff (min)': site_performance['min'],
                            'Residual': residuals, 'Costs': costs,
                            'Power': fru_power, 'Efficiency': fru_efficiency, 'Transactions': transactions},
                           index=False)
    excelerator.to_excel(start=False)

# run scenarios
def run_scenarios(project, excel_int, details, sql_db):
    for scenario_number in range(details.n_scenarios):
        scenario = get_scenario(excel_int, scenario_number)
        
        # run simulation
        simulation = run_simulation(details, scenario, sql_db)
        save_results(project, scenario, simulation)

# main code
project, excel_int = get_project()
sql_db = get_structure(structure_db)
details = get_details(excel_int)
run_scenarios(project, excel_int, details, sql_db)
StopWatch.show_results()