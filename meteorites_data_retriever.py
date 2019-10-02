#!/usr/bin/env python
# coding: utf-8

# # Meteorites Landing Data Retriever
# 
# 1. Retrive the data by calling NASA api
# 2. Add Country Code, Country Name and City Name
# 3. Save the Meteorites Landing Data to fine "meteorites_landing_raw_data.csv" file for the cleaning process

# In[1]:


#Dependencies
import requests
import pandas as pd
import json
import numpy as np
from sodapy import Socrata
from citipy import citipy
import time


# In[2]:


def fetch_meteorite_landings_limited_dataset():
    #https://data.nasa.gov/resource/y77d-th95.json 
    query_url = "https://data.nasa.gov/resource/gh4g-9sfh.json"
    # Get response from nasa rest call
    response = requests.get(query_url).json()
    meteorite_landings_data = pd.DataFrame(response)
    return meteorite_landings_data


# In[3]:


def fetch_meteorite_landings_full_dataset():
    # Unauthenticated client only works with public data sets. Note 'None'
    # in place of application token, and no username or password:
    client = Socrata("data.nasa.gov", None)

    # Example authenticated client (needed for non-public datasets):
    # client = Socrata(data.nasa.gov,
    #                  MyAppToken,
    #                  userame="user@example.com",
    #                  password="AFakePassword")

    # First 2000 results, returned as JSON from API / converted to Python list of
    # dictionaries by sodapy.
    results = client.get("gh4g-9sfh", limit=50000)

    # Convert to pandas DataFrame
    meteorite_landings_data = pd.DataFrame.from_records(results)
    return meteorite_landings_data


# In[4]:


def meteorites_data_retriever():
    #Fecth the meteorite landings data
    meteorite_landings_data = fetch_meteorite_landings_full_dataset()
    meteorite_data_df = meteorite_landings_data.copy()
    #Delete the columns
    meteorite_data_df = meteorite_data_df.drop(columns=":@computed_region_cbhk_fwbd")
    meteorite_data_df = meteorite_data_df.drop(columns=":@computed_region_nnqa_25f4")

    #Use np.nan instaed of . "" as empty
    meteorite_data_df["country_code"] = np.nan
    meteorite_data_df["country_name"] = np.nan
    meteorite_data_df["city_name"] = np.nan
    meteorite_data_df["continent_name"] = np.nan
    
    #find the country name from country code and update to data Frame
    country_mapping_df = pd.read_csv("./Resources/country_continent_mapping.csv",encoding ='latin1')

    # Loop through the meteorite_data_df and find city name and coutry code for a lat/long
    start_time = time.time()

    for row in meteorite_data_df.itertuples():
        try:
                latitude = float(row.reclat)
                longitude = float(row.reclong)
   
                city = citipy.nearest_city(latitude, longitude)
                city_name = city.city_name.capitalize()
                country_code = city.country_code.upper() 
                meteorite_data_df["city_name"][row.Index]  = city_name
                meteorite_data_df["country_code"][row.Index] = country_code
        except IndexError:
            print(f"Oops!  That was no valid number.  latitude:{latitude}, longitude:{longitude},city_name:{city_name} ,country_code:{country_code} ,  ")

    print("Process completed : It took --- %s minutes ---" % ((time.time() - start_time)/60))
    meteorite_data_df.to_csv('./Resources/meteorites_landing_raw_data.csv')
    return meteorite_landings_data


# In[5]:


#meteorites_data_retriever()

