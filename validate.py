import pandas as pd
import logging
from config import config

logger = logging.getLogger("voter_data_pipeline")


def validate_required_columns(df):
    """
    Check that all required columns exist in the dataframe.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        list: Missing column names
    """
    output_cols = config['output_columns']
    missing_cols = [col for col in output_cols if col not in df.columns]
    
    if missing_cols:
        logger.warning(f'Missing columns in input file: {missing_cols}')
        raise ValueError(f'Critical column {missing_cols} missing')
    return missing_cols


def keep_required_columns(df):
    """
    Keep only the columns specified in config.
    
    Args:
        df: DataFrame to filter
        
    Returns:
        DataFrame: Filtered dataframe with only required columns
    """
    output_cols = config['output_columns']
    return df[[col for col in output_cols if col in df.columns]]


def validate_and_convert_dtypes(df, layout):
    """
    Validate and convert data types according to layout specification.
    
    Args:
        df: DataFrame to validate
        layout: Dictionary mapping column names to expected data types
        
    Returns:
        DataFrame: DataFrame with converted data types
    """
    logger.info('Validating data types...')
    
    for column in df.columns:
        if column in layout:
            dtype = layout[column]
            try:
                if dtype == 'datetime64[ns]':
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                elif dtype == int:
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                elif dtype == str:
                    df[column] = df[column].astype(str).str.strip()
                    # Replace NaN values with empty strings for string columns
                    df[column] = df[column].replace('nan', '').fillna('')
            except Exception as e:
                logger.warning(f'Non-critical column {column} conversion failed to {dtype}: {e}')
    
    logger.info('Data type validation complete.')
    return df
