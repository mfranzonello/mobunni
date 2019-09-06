# main script to read inputs, set up structure, run simulation and print results

from math import floor

from structure import Project, SQLDB
from xl_inputs import ExcelInt
from xl_outputs import Excelerator, ExcelePaint
from groups import Details, Commitments, Technology, Tweaks, Thresholds
from simulate import Scenario, Simulation

from debugging import StopWatch, open_results

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
    thresholds = Thresholds(sql_db.get_thresholds())
    return sql_db, thresholds

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
        non_replace, repair, junk_level, best, early_deploy, \
        new_servers, existing_servers, allowed_fru_models = excel_int.get_scenario(scenario_number)

    commitments = Commitments(length=contract_length, target_size=target_size, start_date=start_date,
                              start_month=start_month, non_replace=non_replace, limits=limits)

    technology = Technology(new_servers=new_servers, existing_servers=existing_servers, allowed_fru_models=allowed_fru_models)
    tweaks = Tweaks(repair=repair, junk_level=junk_level, best=best, early_deploy=early_deploy)

    scenario = Scenario(scenario_number, scenario_name,
                        commitments=commitments, technology=technology, tweaks=tweaks)
    return scenario

# run simulation
def run_simulation(details, scenario, sql_db, thresholds):
    simulation = Simulation(details, scenario, sql_db, thresholds)
    simulation.run_scenario()
    return simulation

# output results
def save_results(project, scenario, simulation):
    inputs, site_performance, costs, fru_power, fru_efficiency, transactions = simulation.get_results()
    
    # set up output
    excelerator = Excelerator(path=None, filename='bpm_results_{}_{}'.format(project.name, scenario.name), extension='xlsx')
    
    # assemble output
    data, formats, charts = ExcelePaint.get_paints(scenario.windowed, scenario.commitments.limits, inputs,
                                                   site_performance, costs, fru_power, fru_efficiency, transactions)
    excelerator.store_data(data)
    excelerator.store_formats(formats)
    excelerator.store_charts(charts)
    excelerator.to_excel(start=open_results)

# run scenarios
def run_scenarios(project, excel_int, details, sql_db, thresholds):
    for scenario_number in range(details.n_scenarios):
        scenario = get_scenario(excel_int, scenario_number)
        
        # run simulation
        simulation = run_simulation(details, scenario, sql_db, thresholds)
        save_results(project, scenario, simulation)

# main code
project, excel_int = get_project()
sql_db, thresholds = get_structure(structure_db)
details = get_details(excel_int)
run_scenarios(project, excel_int, details, sql_db, thresholds)
StopWatch.show_results()