#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
#%config Completer.use_jedi = False


# In[2]:


def meteorites_data_cleaner():
    # Function that reads a csv generated from NASA api and cleans it
    df_meterorite_data = pd.read_csv("./Resources/meteorites_landing_raw_data.csv")
    # split 'recclass' on comma in prep for cleaning out remainder
    df_meterorite_data[['material_column', 'misc_maerial']] = df_meterorite_data['recclass'].str.split(',', expand=True)
    # date cleanup #1 - remove stuff like timestamp and AM/PM
    # split impact year on space to eliminate things like timestamp and AM/PM
    df_meterorite_data[['cal_year', 'time']] = df_meterorite_data['year'].str.split("T", n = 1, expand = True)
    df_meterorite_data.drop(['misc_maerial'], axis=1, inplace=True)
    df_meterorite_data.drop(['recclass'], axis=1, inplace=True)
    df_meterorite_data.drop(['time'], axis=1, inplace=True)
    df_meterorite_data.drop(['year'], axis=1, inplace=True)
    df_meterorite_data.drop(['nametype'], axis=1, inplace=True)
    df_meterorite_data.dropna(how="any", inplace=True)
    # create a filter to strip out coordinates (0,0)
    real_coords = (df_meterorite_data['reclat'] != 0) & (df_meterorite_data['reclat'] != 0)
    # Create new dataframe to 
    df_met_data = pd.DataFrame()
    df_met_data = df_meterorite_data[real_coords]
    # To enable time sequence calculation need to change type 'object' to 'datetime64[ns]'
    # There are still bad dates, such as date over 500 years old. Pandas has difficulties with this
    # without extra coding. So change to date. 'error=coerce' will set bad dates to NaT
    bad_dates = pd.to_datetime(df_met_data['cal_year'],errors="coerce",infer_datetime_format=True)
    # was a challenge adding the new column of bad dates, so add the series as a column
    # with the 'assign' method
    df_met_data = df_met_data.assign(years=bad_dates)
    # we don't need the original 'cal_year' column. So drop it
    df_met_data.drop(['cal_year'], axis=1, inplace=True)
    df_met_data.dropna(how="any", inplace=True)
    # need to reset the index because of all the data deletions
    df_met_data.reset_index(drop=True, inplace=True)
    # save new data file
    df_met_data.to_csv('./Resources/meteorites_cleaned_data.csv')
    


# In[ ]:


meteorites_data_cleaner()


# In[ ]:




