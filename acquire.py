import pandas as pd
import requests
import os

###################### REST API Functions ######################
def base_url():
    '''
    Returns base url to acquire H-E-B data.
    
    Parameters
    ----------
    None
    
    Returns
    -------
    string url to acquire H-E-B data.
    '''
    return  "https://python.zach.lol"


def response_endpoint(endpoint):
    '''
    Accepts endpoint to a specified H-E-B dataset.
    
    Returns 
    Parameters
    ----------
    endpoint : str
        A possible path of "https://python.zach.lol"
        
        Example
        -------
        endpoint = "/documentation"
        base_url = "https://python.zach.lol"
        
    Returns
    -------
    requests.models.Response object
    '''
    get_request = requests.get(base_url() + endpoint)
    return get_request


def max_page(url):
    '''
    Accepts a requests.models.Response object
    
    Return the maximum page for a specific endpoint

    Parameters
    ----------
    url : requests.models.Response
        A response from an endpoint using REST API
    
    Returns
    -------
    integer value
    '''
    return url.json()['payload']['max_page']


def page_iterator(data, data_path, stop_page):
    '''
    Accepts an endpoint name, path to endpoint, and number of pages to acquire.
    
    Return a specific H-E-B dataset as a pandas DataFrame.

    Parameters
    ----------
    data : str
        The name of an endpoint to retrieve H-E-B data
    
    data_path : str
        The path to the specified endpoint
    
    stop_page : int
        The page number to stop on - inclusive.
    
    Returns
    -------
    pandas DataFrame
    '''
    df = pd.DataFrame()
    
    for page in range(1, stop_page+1):
        response = requests.get(base_url() + data_path + '?page=' + str(page))
        df = df.append(response.json()['payload'][f'{data}'])

    return df


###################### Acquire H-E-B data ######################
def acquire_heb_data(dataset='stores'):
    '''
    Acquires H-E-B store data from https://python.zach.lol using REST API
    
    Return dataset of H-E-B store data as a pandas Dataframe.
    
    Datasets
    --------
    'stores'
    A dataset of 10 unique H-E-B stores in San Antonio, TX
        shape : (10, 5)
        feature_names : 'store_address', 'store_city', 'store_id', 'store_state', 'store_zipcode'

    'items'
    A dataset of 50 unique grocery items at H-E-B.
        shape : (50, 6)
        feature_names : 'item_brand', 'item_id', 'item_name', 'item_price', 'item_upc12', 'item_upc14'
        
    'sales'
    A dataset of daily sales for 10 H-E-B stores in San Antonio, TX
        shape : (913000, 5)
        feature_names : 'item', 'sale_amount', 'sale_date', 'sale_id', 'store'

    Parameters
    ----------
    dataset : str, default 'stores'
        'stores': acquires stores dataset
        'items' : acquires items dataset
        'sales' : acquires sales dataset
    
    Returns
    -------
    pandas DataFrame
    '''
    path = f'/api/v1/{dataset}'
    endpoint = response_endpoint(path)
    max_page_num = max_page(endpoint)
    
    df = page_iterator(data=f'{dataset}',
                       data_path=path,
                       stop_page=max_page_num
                       )
    
    df.reset_index(drop=True, inplace=True)
    return df

def check_local_cache(data):
    if os.path.isfile(f'{data}.csv'):
        df = pd.read(f'{data}.csv', in)

###################### Load Main HEB Dataset ######################
def load_heb_data():
    '''
    Return a cached dataset of H-E-B data: stores, items, and sales
    
    Parameters
    ----------
    None
    
    Returns
    -------
    pandas Dataframe
    '''
    df = pd.read_csv('store_sales_data.csv')
    return df

###################### Load Germany's Open Power Systems Data  ######################
def acquire_open_power_systems():
    """
    Open Power Systems Data for Germany - 2006-2017
    Data set of electricity consumption, wind power production, and solar power production.

    Parameters
    ----------
    None
    
    Returns
    -------
    pandas DataFrame
    """
    opsd_url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
    df = pd.read_csv(opsd_url)
    return df