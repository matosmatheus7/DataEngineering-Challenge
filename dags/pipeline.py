from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import json

# Define a dictionary to map the month names and their English translation
months_dict = {
    'Jan':'Jan',
    'Fev':'Feb',
    'Mar':'Mar',
    'Abr':'Apr',
    'Mai':'May',
    'Jun':'Jun', 
    'Jul':'Jul',
    'Ago':'Aug',
    'Set':'Sep',
    'Out':'Oct',
    'Nov':'Nov',
    'Dez':'Dec'
}

# Define a dictionary to map state names to their abbreviated form
uf_dict = {
	'ACRE':'AC',
	'ALAGOAS':'AL',
	'AMAZONAS':'AM',
	'AMAPÁ':'AP',
	'BAHIA':'BA',
	'CEARÁ':'CE',
	'DISTRITO FEDERAL':'DF',
	'ESPÍRITO SANTO':'ES',	
	'GOIÁS':'GO',	
	'MARANHÃO':'MA',	
	'MINAS GERAIS':'MG',	
	'MATO GROSSO DO SUL':'MS',	
	'MATO GROSSO':'MT',	
	'PARÁ':'PA',	
	'PARAÍBA':'PB',	
	'PERNAMBUCO':'PE',	
	'PIAUÍ':'PI',	
	'PARANÁ':'PR',	
	'RIO DE JANEIRO':'RJ',	
	'RIO GRANDE DO NORTE':'RN',	
	'RONDÔNIA':'RO',	
	'RORAIMA':'RR',	
	'RIO GRANDE DO SUL':'RS',	
	'SANTA CATARINA':'SC',	
	'SERGIPE':'SE',	
	'SÃO PAULO':'SP',	
	'TOCANTINS':'TO'	
}

# FUNCTIONS 
def concatenate_year_and_month_columns(df):
    """
    This function concatenates the 'ANO' and 'month' columns into a new column named 'year_month'. 
    The 'ANO' column is then deleted from the dataframe. The 'year_month' column is then formatted 
    to be accepted in a SQL INSERT statement.
    Args:
    - df: pandas.DataFrame object containing the data to be processed.
    Returns:
    - pandas.DataFrame object with a new column: 'year_month'.
    """
    from pandas.tseries.offsets import MonthEnd
    df['ANO'] = df['ANO'].astype(str)
    df['month'] = df['ANO'] + '-' + df['month']
    df = df.rename(columns={'month':'year_month'}).drop(columns=['ANO'])
    df['year_month'] = pd.to_datetime(df['year_month'], format='%Y-%b') + MonthEnd(1)
    return df

def uf_column_transformation(df):
    """
    This function renames the 'ESTADO' column to 'uf', replaces the values in the 'uf' column according 'uf_dict'.
    Then in order to organize the dataframe to the same order requested on the exercise it selects a subset of columns 
    and returns the resulting dataframe.
    Args:
    - df: a pandas.DataFrame object containing the data to be transformed.
    Returns:
    - A pandas.DataFrame object with a subset of columns: year_month, uf, product, unit, and volume.
    """
    df = df.rename(columns={'ESTADO':'uf'})
    df['uf'] = df['uf'].replace(uf_dict)
    df = df[['year_month','uf','product','unit','volume']]
    return df

def add_product_and_unit_columns(df):
    """
    This function splits the 'COMBUSTÍVEL' column into two columns: 'product' and 'columa'. 
    The 'COMBUSTÍVEL' column is then deleted from the dataframe, and the 'columa' as well.
    Column 'UNIDADE' is replaced by 'unit'.
    Args:
    - df: pandas.DataFrame object containing the data to be processed.
    Returns:
    - pandas.DataFrame object with two new columns: 'product' and 'unit'.
    """
    df = df.rename(columns={'UNIDADE':'unit'})
    df[['product', 'columa']] = df['COMBUSTÍVEL'].str.split(r' \(|\)',expand=True)[[0,1]].rename(columns={0:'product',1:'columa'})
    df = df.drop(columns=['COMBUSTÍVEL'])
    df = df.drop(columns=['columa'])
    return df

def unpivot_df(df):
    """
    This function performs an unpivot operation on the input dataframe 'df' by melting it down into a long format.
    Specifically, it creates two new columns 'month' and 'volume' by unpivoting the table based on the columns 
    ANO', 'ESTADO', 'COMBUSTÍVEL', 'UNIDADE', and the keys of a given dictionary 'months_dict'.
    It also replaces the values in the 'month' column according to the same 'months_dict'.
    Args:
    - df: a pandas.DataFrame object containing the data to be unpivoted.
    Returns:
    - A pandas.DataFrame object with the columns: 'ANO', 'ESTADO', 'COMBUSTÍVEL', 'month', and 'volume'.
    """

    df = df.rename(columns=months_dict)
    df = pd.melt(df, id_vars = ['ANO', 'ESTADO', 'COMBUSTÍVEL', 'UNIDADE'], value_vars = months_dict.values(), var_name = 'month', value_name = 'volume')
    return df

def check_total_volum_sales(df_ingest, df_unpivot):
    """
    It iterates over each row of the df_ingest dataframe and compares the 'TOTAL' column with the sum of the 'volume'
    column in the df_unpivot dataframe. If the two values don't match, it prints a message with the fuel, year, and state
    values for which the mismatch occurred and returns False. Otherwise, it returns True.
    Args:
    - df_ingest: pandas.DataFrame object with a 'TOTAL' column representing the total volume sales for each row.
    - df_unpivot: pandas.DataFrame object with a 'volume' column representing the volume sales for each (fuel, year, state) combination.
    Returns:
    - True if the total volume sales match for all (fuel, year, state) combinations, False otherwise.
    """
    import numpy as np
    mismatch = 0

    for i, row in df_ingest.iterrows():
        fuel = row['COMBUSTÍVEL']
        year = row['ANO']
        state = row['ESTADO']
        df_unpivot_total_sum = df_unpivot.loc[(df_unpivot['COMBUSTÍVEL'] == fuel) &
                                   (df_unpivot['ANO'] == year) &
                                   (df_unpivot['ESTADO'] == state), 'volume'].sum()
        df_ingest_total_sum = row['TOTAL']
        if np.isnan(df_ingest_total_sum):
            df_ingest_total_sum = 0
        if not np.isclose(df_unpivot_total_sum, df_ingest_total_sum, rtol=1e-9, atol=1e-9):
            print('Values not matching: FUEL:{} YEAR:{} STATE:{} TOTAL OF THE TRANSFORMED DATAFRAME:{} TOTAL OF THE EXTRACTED DATAFRAME:{}'.format(row['COMBUSTÍVEL'], row['ANO'], row['ESTADO'], df_ingest_total_sum, df_ingest_total_sum))
            mismatch += 1

    if mismatch == 0:
        return True
    else: 
        print('Total incompatible records')
        return False


def create__table_and_insert_data(df_dfuels_sales, df_diesel_sales):
    """
    Creates a table in a PostgreSQL database and inserts the given dataframes into it.
    I choose to create a table first, because the column created_at will have the exatcly time when the data was inserted.
    Args:
        df_dfuels_sales (pandas.DataFrame): The dataframe containing sales data for dfuels.
        df_diesel_sales (pandas.DataFrame): The dataframe containing sales data for diesel.
    Returns:
        None
    """
    from sqlalchemy import create_engine
    conn_string = 'postgresql://airflow:airflow@postgres:5432/airflow'
    engine = create_engine(conn_string)
    connection = engine.connect()

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS anp_sales (
            year_month DATE NOT NULL,
            uf VARCHAR(2) NOT NULL,
            product VARCHAR(50) NOT NULL,
            unit VARCHAR(2) NOT NULL,
            volume DOUBLE PRECISION NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """

    try:
        connection.execute(create_table_sql)
    except Exception as e:
        print("Error inserting data: ", e)

    try:
        df_dfuels_sales.to_sql('anp_sales', con=engine, if_exists='append', index=False)
        df_diesel_sales.to_sql('anp_sales', con=engine, if_exists='append', index=False)
    except Exception as e:
        print(f"An error occurred while inserting dataframes into the database: {e}")

    connection.close()

def get_pivot_cache(anp_file, pivotname, output):
    """
    This creates a Excel file with the pivot cache data from the given file and pivot name.

    Args:
        anp_file: a string indicating the path to the input Excel file containing the pivot table data.
        pivotname: a string indicating the name of the pivot table to be extracted.
        output: a string indicating the path to the output Excel file to be generated.

    Returns:
        This function does not return any value, but generates an Excel file with the pivot table data.
    """
    import openpyxl
    from openpyxl import load_workbook
    from openpyxl.pivot.fields import Missing
    import numpy as np

    workbook = load_workbook(anp_file)
    worksheet = workbook['Plan1']

    pivot_name = pivotname

    pivot_table = [p for p in worksheet._pivots if p.name == pivot_name][0]

    fields_map = {}
    for field in pivot_table.cache.cacheFields:
        if field.sharedItems.count > 0:
            l = []
            for f in field.sharedItems._fields:
                try:
                    l += [f.v]
                except AttributeError:
                    l += [""]
            fields_map[field.name] = l

    column_names = [field.name for field in pivot_table.cache.cacheFields]
    rows = []
    for record in pivot_table.cache.records.r:
        record_values = [
            field.v if not isinstance(field, Missing) else np.nan for field in record._fields
        ]

        row_dict = {k: v for k, v in zip(column_names, record_values)}

        for key in fields_map:
            row_dict[key] = fields_map[key][row_dict[key]]

        rows.append(row_dict)

    df = pd.DataFrame.from_dict(rows)
    df.to_excel(output, index=False)

# AIRFLOW TASKs
def download_anp_data(): 
    import os
    import urllib.request
    import datetime
    # Seting threshold to 1 month
    threshold_days=30
    # Path and name of the downloaded Excel file
    anp_file = "/opt/airflow/staging/vendas-combustiveis-m3.xlsx"
    if os.path.exists(anp_file):
        # Get the last modified date of the file
        file_date = datetime.datetime.fromtimestamp(os.path.getmtime(anp_file))
        # Get the difference between the current date and the file date in days
        days_diff = (datetime.datetime.now() - file_date).days
        if days_diff < threshold_days:
            print(f"File already exists and is up to date (downloaded {days_diff} days ago). Skipping download.")
            return 'ingest'
        else:
            print(f"File exists but is out of date (downloaded {days_diff} days ago). Downloading new file.")

    # URL where ANP data is located
    url = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/vdpb/vendas-combustiveis-m3.xls'
    # Download the ANP data and save it as an Excel file
    urllib.request.urlretrieve(url, anp_file)

    # Extract pivot table 1 from the Excel file and save it as a separate Excel file
    get_pivot_cache(anp_file, 'Tabela dinâmica1', '/opt/airflow/staging/vendas-dfuels-m3.xlsx')
    # Extract pivot table 3 from the Excel file and save it as a separate Excel file
    get_pivot_cache(anp_file, 'Tabela dinâmica3', '/opt/airflow/staging/vendas-diesel-m3.xlsx')
    return 'ingest'

def ingest_data(ti): 
    # specify the file path for the data source
    df_dfuels_path = '/opt/airflow/staging/vendas-dfuels-m3.xlsx'
    df_diesel_path = '/opt/airflow/staging/vendas-diesel-m3.xlsx'

    # read the data from the Excel file, using pandas.read_excel() function
    # and assign them to two separate data frames
    df_dfuels_sales = pd.read_excel(df_dfuels_path)
    df_diesel_sales = pd.read_excel(df_diesel_path)
    df_diesel_sales['TOTAL'] = df_diesel_sales.iloc[:,range(4,17)].sum(axis=1)

    # push the data frames as JSON strings to Airflow's XCom system, so that
    # they can be accessed by downstream tasks
    ti.xcom_push('dfuels_sales', df_dfuels_sales.to_json())
    ti.xcom_push('diesel_sales', df_diesel_sales.to_json())

def unpivot_data(ti):
    # retrieve data from previous task
    ingest_dfuels_sales = ti.xcom_pull(key='dfuels_sales', task_ids='ingest')
    ingest_diesel_sales = ti.xcom_pull(key='diesel_sales', task_ids='ingest')

    # read the data from Json, using pandas.DataFramel() function
    # and assign them to two separate data frames
    df_dfuels_sales = pd.DataFrame(json.loads(ingest_dfuels_sales))
    df_diesel_sales = pd.DataFrame(json.loads(ingest_diesel_sales))

    # call function unpivot_df(), for each data frame and store the result dataframe
    unpivot_dfuels = unpivot_df(df_dfuels_sales)
    unpivot_diesel = unpivot_df(df_diesel_sales)

    # push the data frames as JSON strings to Airflow's XCom system, so that
    # they can be accessed by downstream tasks
    ti.xcom_push('unpivoted_dfuels_sales', unpivot_dfuels.to_json())
    ti.xcom_push('unpivoted_disel_sales', unpivot_diesel.to_json())

def validate_data(ti): 
    # retrieve data from previous task
    unpivoted_dfuels_sales = ti.xcom_pull(key='unpivoted_dfuels_sales', task_ids='unpivot')
    unpivoted_disel_sales = ti.xcom_pull(key='unpivoted_disel_sales', task_ids='unpivot')
    ingest_dfuels_sales = ti.xcom_pull(key='dfuels_sales', task_ids='ingest')
    ingest_diesel_sales = ti.xcom_pull(key='diesel_sales', task_ids='ingest')

    # read the data from Json, using pandas.DataFramel() function
    # and assign them to two separate data frames
    df_dfuels_sales = pd.DataFrame(json.loads(unpivoted_dfuels_sales))
    df_diesel_sales = pd.DataFrame(json.loads(unpivoted_disel_sales))
    extract_dfuels = pd.DataFrame(json.loads(ingest_dfuels_sales))
    extract_diesel = pd.DataFrame(json.loads(ingest_diesel_sales))

    # call function check_total_volum_sales(), for each data frame
    if check_total_volum_sales(extract_dfuels, df_dfuels_sales) and check_total_volum_sales(extract_diesel, df_diesel_sales):
        return 'load' # if values match, go to 'load'task
    return 'report' # if values do not match, go to 'report'task

def load_data(ti):
    # retrieve data from previous task
    unpivoted_dfuels_sales = ti.xcom_pull(key='unpivoted_dfuels_sales', task_ids='unpivot')
    unpivoted_disel_sales = ti.xcom_pull(key='unpivoted_disel_sales', task_ids='unpivot')    

    # read the data from Json, using pandas.DataFramel() function
    # and assign them to two separate data frames
    df_dfuels_sales = pd.DataFrame(json.loads(unpivoted_dfuels_sales))
    df_diesel_sales = pd.DataFrame(json.loads(unpivoted_disel_sales)) 

    # call function add_product_and_unit_columns(), for each data frame
    df_dfuels_sales =  add_product_and_unit_columns(df_dfuels_sales) 
    df_diesel_sales =  add_product_and_unit_columns(df_diesel_sales) 

    # call function concatenate_year_and_month_columns(), for each data frame
    df_dfuels_sales = concatenate_year_and_month_columns (df_dfuels_sales)
    df_diesel_sales = concatenate_year_and_month_columns (df_diesel_sales)

    # call function uf_column_transformation(), for each data frame
    df_dfuels_sales = uf_column_transformation(df_dfuels_sales)
    df_diesel_sales = uf_column_transformation(df_diesel_sales)

    # call function create__table_and_insert_data()
    create__table_and_insert_data(df_dfuels_sales, df_diesel_sales)

def report_pipeline():
    # just send a print on the task log
    print('Pipeline ends. Check the logs to see if any error occured in the process.')

#Airflow DAG definition and it has several tasks defined
with DAG('anp_fuel_sales_pipeline', start_date = datetime(2023,3,21),
        schedule_interval= '0 23 1 * * ', catchup= False  ) as dag: 
    
    download = PythonOperator(
        task_id = 'download',
        python_callable = download_anp_data
    )    

    ingest = PythonOperator(
        task_id = 'ingest',
        python_callable = ingest_data
    )

    unpivot = PythonOperator(
        task_id = 'unpivot',
        python_callable = unpivot_data
    )

    validate = PythonOperator(
        task_id = 'validate',
        python_callable = validate_data
    )

    load = PythonOperator(
        task_id = 'load',
        python_callable = load_data
    )

    report = PythonOperator(
        task_id = 'report',
        python_callable = report_pipeline
    )

    download >> ingest >> unpivot >> validate >> load >> report
    validate >> report