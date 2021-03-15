# -*- coding: utf-8 -*-
"""
Created on Thu Mar 11 12:08:48 2021

@author: PauloAlves
"""

import numpy as np
import pandas as pd
import json
import urllib.request, json 

       
import re


def find_all_key_sitesearch(df, index,var):
    """
    This function will recursivelly find all the dictionary keys from the webscrapped json.
    This function will work best when you are using "/sites/MLA/search?q="
    """
    
    def recursive_items(dictionary,keys):
        k = []
        if type(dictionary) != dict:
            return [keys]
        for key, value in dictionary.items():
            if type(value) is dict:
     
                y = (recursive_items(value, keys+'||'+key ))
                k.extend(y)
            else:
                y = (recursive_items(value, keys+'||'+key ))
                k.append(y)
        return k

    aux_d = {}
    if type(df.loc[index,var]) == dict:
        for t in recursive_items(dictionary = df.loc[index,var], keys =  var):
            L = [m.start() for m in re.finditer('\|\|', t[0])]
            L_aux = L + [len(t[0])]
            for i in range(len(L)):
                key = t[0][L_aux[i]+2:(L_aux[i+1])]
    
                if i == 0:
                    name = var + '__'+ key
                    aux = df.loc[index,var].get(key)
                else:
                    name = name + '__'+ key
                    aux = aux.get(key)
    
            if len(L) == 0:
                pass
            elif type(aux) == list:
                aux_d[name] = [aux]
            else:
                aux_d[name] = aux
 
    return aux_d


def find_all_key_items(dicti):
    """
    This function will recursivelly find all the dictionary keys from the webscrapped json.
    This function will work best when you are using "/items/"
    """
    def recursive_items(dictionary,keys):
        k = []
        if type(dictionary) != dict:
            return [keys]
        for key, value in dictionary.items():
            if type(value) is dict:
     
                y = (recursive_items(value, keys+'||'+key ))
                k.extend(y)
            else:
                y = (recursive_items(value, keys+'||'+key ))
                k.append(y)
        return k

    aux_d = {}
    if type(dicti) == dict:
        for t in recursive_items(dictionary = dicti, keys =  ''):
            L = [m.start() for m in re.finditer('\|\|', t[0])]
            L_aux = L + [len(t[0])]
            for i in range(len(L)):
                key = t[0][L_aux[i]+2:(L_aux[i+1])]
    
                if i == 0:
                    name = '' + '__'+ key
                    aux = dicti.get(key)
                else:
                    name = name + '__'+ key
                    aux = aux.get(key)
    
            if len(L) == 0:
                pass
            elif type(aux) == list:
                aux_d[name] = [aux]
            else:
                aux_d[name] = aux
 
    return aux_d




def expand_df(df):
    """
    This function will use the function find_all_key_sitesearch to find all possible keys inside all pandas columns.
    It will create a structured version of any dictionary or list found.

    """
    
    df2 = df.copy()
    for col in df.columns:
        # Process if column containing dictionary data
        if (type(df.loc[0,col]) == dict):
            
            # transforming dictionary columns into a sparse DataFrame
            aux = pd.concat([pd.DataFrame(find_all_key_sitesearch(df, i,col), index = [i])  for i in df.index], axis= 0)
            df2[aux.columns] = aux
            
            # Dropping original columns
            df2.drop(col, axis = 1, inplace = True)
        
        # Process if column containing list data with at least 1 dictionary inside
        elif (type(df.loc[0,col]) == list) and len(df.loc[0,col]) > 0:
            if type(df.loc[0,col][0]) == dict:
                k = 0
                # transforming all dictionaries in sparse DataFrames
                while k >= 0:
                    try:
                        df2[col+"_"+str(k)] = df.loc[:,col].apply(lambda x: x[k]  if len(x) > k else None)
                        aux = pd.concat([pd.DataFrame(find_all_key_sitesearch(df2,i,col+"_"+str(k)), index = [i])  for i in df.index], axis= 0)
                        df2[aux.columns] = aux
                        if df2[col+"_"+str(k)].isnull().sum() == df2.shape[0]:
                            k = -1
                        else:
                            k+=1
                        df2.drop(col+"_"+str(k-1), axis = 1, inplace = True)
                    except:

                        k = -1
                df2.drop(col, axis = 1, inplace = True)  
    return df2

def create_product_table(product):
    
    """
    This function will webscrap the mercadolibre API looking for results of a given product. 
    The returned jsons will be transformed into a pandas Dataframe.
    
    Inputs:
        - product (str):
            Name of the product to search

    Outputs:
        The output is a tuple with two elements:
        0 - Pandas Dataframe containing the contents of the webscrapped jsons
        1 - List contaning the IDs that had errors during the process
    
    """
    
    errors = []
    data_df = pd.DataFrame()
    for i in range(0,1000,50):
        with urllib.request.urlopen("https://api.mercadolibre.com/sites/MLA/search?q="+product+"&offset="+str(i)) as url:
            try:
                data = json.loads(url.read().decode())
                data_df = data_df.append(pd.DataFrame(data['results']))
                update_progress( np.round(i/1000,3),  "Product Extraction"  )

            except:
                update_progress( np.round(i/1000,3),  "Product Extraction"  )
                errors.append(i)
                
    data_df = data_df.reset_index(drop = True)
    update_progress( 1,  "Product Extraction"  )
    return expand_df(data_df), errors


def create_id_table(df, id_name, filter_prod):
    """
    This function will webscrap the mercadolibre API looking for items ID of a given pandas column. 
    The returned jsons will be transformed into a pandas Dataframe.
    
    Inputs:
        - df (pandas DataFrame):
            Pandas Dataframe containing a column with items ID
        - id_name (str):
            Name of the column containing the items ID
        - filter_prod (str):
            Name of the domain_id of the items you want to webscrap

    Outputs:
        The output is a tuple with two elements:
        0 - Pandas Dataframe containing the contents of the webscrapped jsons
        1 - List contaning the IDs that had errors during the process
    
    """
    all_ids = df[id_name].unique()
    aux = pd.DataFrame()
    errors = []
    counter = 0
    for i in all_ids:
        try:
            with urllib.request.urlopen("https://api.mercadolibre.com/items/"+str(i)) as url:
                try:
                    # readning inital json
                    data = json.loads(url.read().decode())
                    
                    # Creating a pandas dataframe
                    data2 = expand_df(pd.DataFrame(find_all_key_items(data)))
                    
                    # Deleting attributes columns
                    cols = [c for c in data2.columns if (c.find("__attributes_") == -1) and (c.find("__pictures_") == -1)]
                    data2 = data2.loc[:,data2.columns[data2.columns.isin(cols)]]
                    
                    # Generating a more specific attributes columns
                    att = pd.DataFrame(data['attributes'])
                    att_dict = pd.DataFrame({att.loc[ind,'id']:att.loc[ind,'value_name'] for ind in att.index}, index = data2.index)
                    data2[att_dict.columns] = att_dict
                    
                    # Number of pictures
                    data2['pictures'] = len(data['pictures'])
                    
                    # Filtering only the required domain_id
                    if data2['__domain_id'].values[0] == filter_prod:
                        aux = aux.append(pd.DataFrame(data2))
                    update_progress( np.round(counter/len(all_ids),3),  "Id Extraction"  )
                    counter+=1
                except:
                    errors.append(i)
                    update_progress( np.round(counter/len(all_ids),3),  "Id Extraction"  )
                    counter+=1
        except:
            errors.append(i)
            update_progress( np.round(counter/len(all_ids),3),  "Id Extraction"  )
            counter+=1
           
    aux = aux.reset_index(drop = True)
    update_progress( 1,  "Id Extraction"  )
    return expand_df(aux.copy()), errors

def update_progress(progress,process):
        barLength = 10 # Modify this to change the length of the progress bar
        status = "            "
        if isinstance(progress, int):
            progress = float(progress)
        if not isinstance(progress, float):
            progress = 0
            status = "error: progress var must be float\n"
        if progress < 0:
            progress = 0
            status = "Halt...\n"
        if progress >= 1:
            progress = 1
            status = "Done...\n"
        block = int(round(barLength*progress))
        text = "\r"+process+": [{0}] {1}% {2}".format( "#"*block + "-"*(barLength-block), np.round(progress*100,1), status)
        print(text, end='', flush = True)
        
        

def list_to_dataframe(df, var_name, prefix):
    df_c = df.copy()
    f_l = list(map(lambda x:  pd.DataFrame({ v: [1]  for v in x.replace('[',"").replace("]","").replace("'","").replace(",","").split()}, index = [0]), df_c.loc[:, var_name] ))
    aux = pd.concat(f_l, axis = 0).fillna(0)
    aux.columns = [prefix+'_'+c for c in aux.columns]
    aux.index = df_c.index
    df_c[aux.columns] = aux
    df_c.drop(var_name, axis = 1, inplace = True)
    return df_c


def str_to_dataframe(df, var_name, prefix):
    df_c = df.copy()
    f_l = list(map(lambda x:  pd.DataFrame({ v: [1]  for v in x}, index = [0], columns = [x]), df_c.loc[:, var_name].str.split('\,')))
    aux = pd.concat(f_l, axis = 0).fillna(0)
    aux.columns = [prefix+'_'+str(c[0]) for c in aux.columns]
    aux.index = df_c.index
    df_c[aux.columns] = aux
    df_c.drop(var_name, axis = 1, inplace = True)
    return df_c
