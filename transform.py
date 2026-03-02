import pandas as pd
import logging
from datetime import datetime
from config import config
import urllib.parse

logger = logging.getLogger("voter_data_pipeline")


def map_party_affiliation(df):
    """
    Map party affiliation codes to full names using config mapping.
    Treat blank/empty values as Unaffiliated.
    
    Args:
        df: DataFrame containing PARTY_AFFILIATION column
        
    Returns:
        DataFrame: DataFrame with mapped party affiliation values
    """
    if 'PARTY_AFFILIATION' in df.columns:
        party_map = config['party_affiliation_map']
        df['PARTY_AFFILIATION'] = df['PARTY_AFFILIATION'].map(party_map).fillna(df['PARTY_AFFILIATION'])
        # Treat blank/empty values as Unaffiliated
        df['PARTY_AFFILIATION'] = df['PARTY_AFFILIATION'].replace('', 'Unaffiliated').fillna('Unaffiliated')
        logger.info('Party affiliation codes mapped to full names. Blank values set to Unaffiliated.')
    
    return df

def get_year_of_birth(df):
    """
    Extract YEAR_OF_BIRTH from DATE_OF_BIRTH column.
    
    Args:
        df: DataFrame containing DATE_OF_BIRTH column   
    Returns:
        DataFrame: DataFrame with YEAR_OF_BIRTH column added    
    """    
    if 'DATE_OF_BIRTH' in df.columns:
        df['DATE_OF_BIRTH'] = pd.to_datetime(df['DATE_OF_BIRTH'], errors='coerce').dt.year
        logger.info('YEAR_OF_BIRTH extracted from DATE_OF_BIRTH.')
    
    return df

def add_load_date(df):
    """
    Add LOAD_DATE column with current timestamp.
    
    Args:
        df: DataFrame to add load date to
        
    Returns:
        DataFrame: DataFrame with LOAD_DATE column added
    """
    df['LOAD_DATE'] = datetime.now()
    logger.info('Load date added to dataframe.')
    return df

def add_additional_columns(df):
    """
    Add any additional columns required for processing.
    
    Args:
        df: DataFrame to add columns to
        
    Returns:
        DataFrame: DataFrame with additional columns added
    """
    
    df['EMAIL'] = config.get('email', '')
    df['CELL_PHONE'] = config.get('cell_phone', '')
    df['HOME_PHONE'] = config.get('home_phone', '')      
    df['FACEBOOK_PROFILE'] = config.get('facebook_profile', '')
    df['PARTICIPATION'] = config.get('participation', '')
    df['DO_NOT_CALL'] = config.get('do_not_call', '')  
    df['DONATE'] = config.get('donate', '')  
    df['FB_SEARCH_LINK'] = df.apply(generate_fb_search_link, axis=1)
   
    logger.info('Additional columns added to dataframe.')
    return df


def sort_data(df):
    """
    Sort data by party affiliation, then by precinct.
    
    Args:
        df: DataFrame to sort
        
    Returns:
        DataFrame: Sorted dataframe
    """
    df = df.sort_values(by=['PARTY_AFFILIATION', 'PRECINCT_NAME'], na_position='last')
    logger.info('Data sorted by party affiliation and precinct.')
    return df


def generate_fb_search_link(df):
    # Standardize data to strings for the URL
    first = str(df['FIRST_NAME']).strip()
    last = str(df['LAST_NAME']).strip()
    city = str(df['RESIDENTIAL_CITY']).strip()
    
    # We use a broad search query: Name + City + State
    query = f"{first} {last} {city} Ohio"
    encoded_query = urllib.parse.quote(query)
    return f"https://www.facebook.com/search/people/?q={encoded_query}"

def transform_data(df):
    """
    Apply all transformations to the dataframe.
    
    Args:
        df: DataFrame to transform
        
    Returns:
        DataFrame: Transformed dataframe
    """
    logger.info('Transforming data...')
    
    # Apply transformations
    df = map_party_affiliation(df)
    df = add_load_date(df)
    df = get_year_of_birth(df)
    df = add_additional_columns(df) 
    df = sort_data(df)
    
    logger.info(f'Data transformation complete. {len(df)} rows processed.')
    return df
