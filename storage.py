
"""
====================================================================================================
FILE: storage.py
PURPOSE: Data Storage Module - Parquet File Writer with Partitioning
====================================================================================================
DESCRIPTION:
It supports local file system storage for testing and AWS S3 for production deployment. 
The partitioning strategy organizes data by load date.
The main function, `save_parquet`, takes a pandas DataFrame containing transformed voter data 
and writes it to the specified storage location (local or S3) in a Hive-style partitioned directory structure. 

VERSION HISTORY:
    - v1.0: Initial version created on 2026-01-20 by Lora Covrett
  
====================================================================================================
"""

import os
from config import config   
import logging

logger = logging.getLogger("voter_data_pipeline")

def save_parquet(df, processed_dir=config['processed_dir']) -> bool:
    """
    Save the transformed DataFrame to Parquet format with date-based partitioning.
    Parameters:
    - df: pandas.DataFrame  

    - processed_dir: str
        Base directory for processed data storage.
        Default is "data/processed" for local testing.
    Returns:    
    - None
    """
    if df is None or df.empty:
        logger.error("DataFrame is None or empty. Nothing to save.")
        return False
    
    if 'LOAD_DATE' not in df.columns:
       logger.error("'LOAD_DATE' column is missing from DataFrame.")
       return False

    # --- Date extraction ---
    # LOAD_DATE may not be datetime dtype if transformation failed or was skipped
    try:
        # Extract the load date from first row to use as partition key
        # 
        # Assumptions:
        # - All rows in DataFrame have the same load_date (batch processing)
        # - DataFrame is not empty (validated in transform step)
        #
        # Format: Convert datetime.date to string "YYYY-MM-DD"
        # Example: datetime.date(2025, 1, 12) → "2025-01-12"
        date = df['LOAD_DATE'].dt.date.iloc[0].strftime('%Y-%m-%d')
    except AttributeError as e:
        logger.error(
            f"'LOAD_DATE' is not datetime dtype. Got {df['LOAD_DATE'].dtype}. "
            "Ensure transformation step converts it before saving."
        )
        return False
    except Exception as e:
        logger.error(f"Failed to extract date from 'LOAD_DATE': {e}")
        return False
  
    # Build Hive-style partition directory path
    # Format: {base_dir}/load_date={YYYY-MM-DD}
    # Example paths:
    # Local:  data/processed/load_date=2025-01-12
    path = os.path.join(processed_dir, f"load_date={date}")
    try:
        os.makedirs(path, exist_ok=True)
    except PermissionError as e:
        logger.error(f"Permission denied when creating directory: {path}")
        return False
    except OSError as e:
        logger.error(f"Failed to create directory '{path}': {e}")
        return False

    # Write DataFrame to Parquet format
    # 
    # Parameters:
    # - index=False: Exclude DataFrame index from Parquet file
    #   (index is just row numbers, not meaningful data) 
    # --- Parquet write ---
    # Can fail due to disk space, bad data types, or corrupt state
    output_path = os.path.join(path, "voter_data.parquet")
    try:
        df.to_parquet(output_path, index=False)
        logger.info(f"Successfully saved {len(df)} rows to {output_path}")
        return True
    except OSError as e:
        logger.error(
            f"Failed to write parquet to '{output_path}'. "
            f"Check disk space and write permissions. Error: {e}"
        )
        return False
    except Exception as e:
        logger.error(f"Unexpected error writing parquet file: {e}")
        return False
