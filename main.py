# main script to read inputs, set up structure, run simulation and print results

from math import floor

from structure import Project, SQLDB
from layout import APC, ExistingServers, NewServers
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
def get_scenario(excel_int, scenario_number, apc):
    print('Getting scenario {} details'.format(scenario_number+1))
    scenario_name, limits, start_date, contract_length, \
        non_replace, repair, junk_level, best, early_deploy, \
        site_code, servers, allowed_fru_models = excel_int.get_scenario(scenario_number) ##target_size, start_month,

    existing_servers = ExistingServers(apc.get_site_performance(site_code))
    new_servers = NewServers(servers)

    if existing_servers.exist():
        target_size = existing_servers.get_size()
        start_date, start_month = existing_servers.get_dates() # replace start date with API value
    elif new_servers.exist():
        target_size = new_servers.get_size()
        start_month = 0

    commitments = Commitments(length=contract_length, target_size=target_size, start_date=start_date,
                              start_month=start_month, non_replace=non_replace, limits=limits)

    technology = Technology(new_servers=new_servers, existing_servers=existing_servers, allowed_fru_models=allowed_fru_models, site_code=site_code)
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

# run scenarios
def run_scenarios(project, excel_int, details, sql_db, thresholds, apc):
    for scenario_number in range(details.n_scenarios):
        scenario = get_scenario(excel_int, scenario_number, apc)
        
        # run simulation
        simulation = run_simulation(details, scenario, sql_db, thresholds)
        save_results(project, scenario, simulation)

# main code
project, excel_int = get_project()
sql_db, thresholds = get_structure(structure_db)
details = get_details(excel_int)
apc = APC()

run_scenarios(project, excel_int, details, sql_db, thresholds, apc)
StopWatch.show_results()