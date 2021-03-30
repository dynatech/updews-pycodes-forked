#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
# Web scraping of Earthquake data through the Phivolcs Website
# Specs:
#       Should run every minute
# =============================================================================

from bs4 import BeautifulSoup
from datetime import timedelta
import os
import pandas as pd
import requests
import sys

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import dynadb.db as db
import gsm.gsmserver_dewsl3.sms_data as sms
# =============================================================================
# Header texts - <p class=auto-style96> or <th class auto-style95>
# Date -Time (PHT) - <soan class = "auto-style99> r
# Lat Long Depth, Mag - <td class =  'auto-style56'>
# Location = <span class = auto-style78> 
# Get the contents of the 3rd tbody tag and inspect by element
# =============================================================================


#Get HTML from response object
def get_eq(URL):

    response = requests.get(URL,verify=False)
    soup= BeautifulSoup(response.content, 'html.parser')
    eq_table = soup.find_all('tbody')[2]
    
    return(eq_table)

# =============================================================================
# Gives string of a table header <th> 
# for debugging if changes are made to the html in the future
# =============================================================================
def get_eqtbl_headers(eq_table):
    tbl_headers = []
    for headers in eq_table.find_all('th'):
        tmp = ""
        for string in headers.stripped_strings:
            tmp += string
        tbl_headers.append(tmp)
    return(tbl_headers)

# =============================================================================
# There are times when entries in the PHIVOLCS EQ webpage have 2 entries under
# a single tr tag, this separates 2 entries under 1 html <tr> tag
# =============================================================================
def split_list(a_list):
    half = len(a_list)//2
    return a_list[:half], a_list[half:]

# =============================================================================
# Extracts the municipality and province from the location string per row
# =============================================================================
def get_province(location_string):
    province = location_string.split('(')[-1].strip(')')
    
    return(province)

def get_municipality(location_string):

    exact_loc = location_string.split('(')[0]
    municipality = exact_loc.split('of ')[1].rstrip()

    return(municipality)

def get_exact_loc(location_string):

    exact_loc = location_string.split('(')[0]

    return(exact_loc)

# =============================================================================
# Reads each row of data in the first page of the EQ Events page
# Returns dataframe of eq_values
# =============================================================================

def read_tbl(eq_table, tbl_headers):
    eq_data_rows = []
    for tr in eq_table.find_all('tr'):
        row_val = []
        for td in tr.find_all('td'):
            tmp = ""
            for cell in td.stripped_strings:
                tmp += cell
            if not tmp:
                print("No value")
            else:
                row_val.append(tmp)
        if(len(row_val) ==0):
            continue
        elif(len(row_val) != len(tbl_headers)):
            d1,d2 = split_list(row_val)
            eq_data_rows.append(d1)
            eq_data_rows.append(d2)
        else:
            eq_data_rows.append(row_val)
    
    eq_data = pd.DataFrame(data=eq_data_rows, columns=tbl_headers)
    eq_data.columns=['ts', 'latitude', 'longitude', 'depth', 'magnitude', 'location']
    eq_data.loc[:, 'province'] = eq_data.location.apply(get_province)
    eq_data.loc[:, 'municipality'] = eq_data.location.apply(get_municipality)
    eq_data.loc[:, 'exact_loc'] = eq_data.location.apply(get_exact_loc)
    eq_data.ts = pd.to_datetime(eq_data.ts)
    
    return(eq_data)


def main():
    URL = "https://earthquake.phivolcs.dost.gov.ph/"
    eq_table = get_eq(URL)
    eq_data = read_tbl(eq_table,get_eqtbl_headers(eq_table))
    
    query = "SELECT * FROM earthquake_events ORDER BY ts DESC LIMIT 1"
    start = pd.to_datetime(db.df_read(query, connection='analysis').ts.values[0]) - timedelta(1)
    eq_data = eq_data.loc[eq_data.ts >= start, ['ts', 'latitude', 'longitude', 'depth', 'magnitude', 'province']]
    eq_data.loc['issuer'] = 'PHIV'
    data_table = sms.DataTable('earthquake_events', eq_data)
    db.df_write(data_table, connection='analysis')


if __name__ == "__main__":
    
    main()