import pandas as pd
import requests


def base_url():
    '''
    
    '''
    return  "https://python.zach.lol"

def max_page(url):
    '''
    
    '''
    return url.json()['payload']['max_page']


def acquire_stores():
    '''
    
    '''
    stores_endpoint = '/api/v1/stores'
    max_page_num = max_page(requests.get(base_url() + stores_endpoint))
    stores = pd.DataFrame()
    
    for page in range(1, max_page_num+1):
        stores_response = requests.get(base_url() + stores_endpoint + '?page=' + str(page))
        stores = stores.append(stores_response.json()['payload']['stores'])
    
    stores.reset_index(drop=True, inplace=True)
    return stores
    
def acquire_items():
    '''
    
    '''
    items_endpoint = '/api/v1/items'
    max_page_num = max_page(requests.get(base_url() + items_endpoint))
    items = pd.DataFrame()
    
    for page in range(1, max_page_num+1):
        items_response = requests.get(base_url() + items_endpoint + '?page=' + str(page))
        items = items.append(items_response.json()['payload']['items'])

    return items
  
def acquire_sales():
    '''
    
    '''
    sales_endpoint = '/api/v1/sales'
    max_page_num = max_page(requests.get(base_url() + sales_endpoint))
    sales = pd.DataFrame()
    
    for page in range(1, max_page_num+1):
        sales_response = requests.get(base_url() + sales_endpoint + '?page=' + str(page))
        sales = sales.append(sales_response.json()['payload']['sales'])
    
    return sales

def load_store_data():
    '''
    
    '''
    df = pd.read_csv('store_sales_data.csv')
    return df