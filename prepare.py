import pandas as pd


############################ Prepare HEB dataset #################################
def prepare_store_data(df):
    '''
    Accepts the raw HEB store dataset
    
    Returns prepared HEB store data as a pandas DataFrame
    
    parameters
    ----------
    df : pandas.DataFrame
        Accepts the HEB dataset. Merged dataframe of : stores, items, and sales
        
    returns
    -------
    df : pandas.DataFrame
        Prepared HEB dataset  
    '''
    # Format the date of the column
    df.sale_date = pd.to_datetime(df.sale_date, format=('%a, %d %b %Y %H:%M:%S %Z'))
    df = df.set_index('sale_date').sort_index()
    
    # Create new columns: month, day of week, and sales total
    df = df.assign(
        month = df.index.month,
        day_of_week = df.index.day_name(),
        sales_total = df.sale_amount * df.item_price
    )
    
    return df


############################ Prepare Open Power Systems dataset #################################
def prepare_ops_data(df):
    '''
    This function prepares Germany's Open Power Systems data
    
    parameters
    ----------
    df : pandas DataFrame
        raw open power systems data
    
    returns
    -------
    df: pandas DataFrame
        Prepared open power systems data
    '''
    df.Date = pd.to_datetime(df.Date)
    df = df.set_index('Date').sort_index()
    
    df = df.assign(
    month=df.index.month,
    year=df.index.year
    )
    
    df = df.fillna(0)
    return df