# Code for ETL operations on Country-GDP data

# Importing the required libraries

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)

    # Find the table under the "By market capitalization" heading
    heading = data.find('span', {'id': 'By_market_capitalization'})
    table = heading.find_next('table')

    # Extract table data
    data = []
    for row in table.find_all('tr'):
        cols = row.find_all('td')
        if len(cols) > 0:
            name = cols[1].text.strip()
            market_cap = float(cols[2].text.replace('\n', '').replace('$', '').replace(',', ''))
            data.append([name, market_cap])

    # Create a Pandas DataFrame
    df = pd.DataFrame(data, columns=['Name', 'MC_USD_Billion'])
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    MC_list = df["MC_USD_Billion"].tolist()
    df['MC_GBP_Billion'] = [np.round(x*0.8,2) for x in MC_list]
    df['MC_EUR_Billion'] = [np.round(x*0.93,2) for x in MC_list]
    df['MC_INR_Billion'] = [np.round(x*82.95,2) for x in MC_list]
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ' : ' + message + '\n')    

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

query_statement1 = f"SELECT * FROM Largest_banks"
query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query_statement3 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement1, sql_connection)
run_query(query_statement2, sql_connection)
run_query(query_statement3, sql_connection)

log_progress('Process Complete.')

sql_connection.close()

log_progress('Server Connection closed')