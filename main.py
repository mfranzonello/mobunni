# main script to read inputs, set up structure, run simulation and print results

from math import floor

from structure import Project, SQLDB, ExcelInt, Excelerator
from groups import Details, Commitments, Technology, Tweaks
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
    inputs, site_performance, costs, fru_power, fru_efficiency, transactions = simulation.get_results()
    excelerator = Excelerator(path=None, filename='bpm_results_{}_{}'.format(project.name, scenario.name), extension='xlsx')

    # store data
    excelerator.store_sheets({'Inputs': inputs,
                              'Power+Eff': site_performance, 'Costs': costs,
                              'Power': fru_power, 'Efficiency': fru_efficiency, 'Transactions': transactions})

    percent_values = ['TMO', 'eff']
    comma_values = ['power', 'fuel', 'ceiling loss']
    date_values = ['date']

    ranges = {'C': '#2E86C1', 'W': '#E74C3C', 'P': '#28B463'}
    if not scenario.windowed:
        ranges.pop('W')
    bounds = {'_max': 'dash', '': 'solid', '_min': 'dash'}

    percent_columns = ['{}{}{}'.format(r, v, b) for v in percent_values for r in ranges for b in bounds]
    comma_columns = ['{}{}'.format(v, b) for v in comma_values for b in bounds]
    date_columns = date_values

    styles = {'0.00%': percent_columns,
              '#,##0': comma_columns,
              'mm/yyyy': date_columns}
  
    # store formatting
    format_sheet = 'Power+Eff'
    formats = [{'sheetname': format_sheet, 'columns': cols, 'style': style} for (cols, style) in zip(styles.values(), styles.keys())]
    excelerator.store_formats(formats)

    # store charts
    chart_sheet = 'Power+Eff'
    chart_columns = percent_columns
    chart_colors = [ranges[r] for v in percent_values for r in ranges for b in bounds]
    chart_dashes = [bounds[b] for v in percent_values for r in ranges for b in bounds]
    chart_y_axis = {'max': 1.0, 'min': floor(10*site_performance[chart_columns].min().min())/10}
    charts = [{'sheetname': chart_sheet, 'columns': chart_columns, 'colors': chart_colors, 'dashes': chart_dashes, 'chart sheet name': 'Graph', 'y-axis': chart_y_axis}]
    excelerator.store_charts(charts)
    excelerator.to_excel(start=open_results)

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