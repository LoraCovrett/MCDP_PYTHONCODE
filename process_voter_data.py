import pandas as pd
import logging
from datetime import datetime
import os
from pathlib import Path
from config import config
import glob
from logging_config import setup_logging
from storage import save_parquet
from validate import validate_required_columns, keep_required_columns, validate_and_convert_dtypes
from transform import transform_data
from export import export_excel, export_csv
import uuid

# Generate unique run identifier for tracing records through pipeline
run_id = str(uuid.uuid4())[:8]

# Initialize structured logging with run context
logger = setup_logging(name="voter_data_pipeline", run_id=run_id)  
logger.info(f'Starting weekly data processing run: {run_id}')    
   
# Define input and output directories
input_dir = config['data_dir']   
output_dir = config['processed_dir']

# Find the most recent file in the input directory
def get_latest_input_file(input_dir, file_pattern='*.txt'):
    """Find the most recently modified file matching the pattern in the directory"""
    logger.info(f'Searching for most recent file in {input_dir}')
    files = glob.glob(os.path.join(input_dir, file_pattern))
    
    if not files:
        raise FileNotFoundError(f'No files matching pattern {file_pattern} found in {input_dir}')
    
    # Sort by modification time and get the most recent
    latest_file = max(files, key=os.path.getmtime)
    logger.info(f'Found most recent file: {latest_file}')
    return latest_file

# Get the most recent input file
input_file = get_latest_input_file(input_dir)

# Define the expected layout
layout = {
    'SOS_VOTERID': str,
    'COUNTY_NUMBER': int,
    'LAST_NAME': str,
    'FIRST_NAME': str,
    'MIDDLE_NAME': str,
    'DATE_OF_BIRTH': str,
    'PARTY_AFFILIATION': str,
    'VOTER_STATUS': str,
    'RESIDENTIAL_ADDRESS1': str,
    'RESIDENTIAL_SECONDARY_ADDR': str,
    'RESIDENTIAL_CITY': str,
    'RESIDENTIAL_STATE': str,
    'RESIDENTIAL_ZIP': str,
    'MAILING_ADDRESS1': str,
    'MAILING_SECONDARY_ADDRESS': str,
    'MAILING_CITY': str,
    'MAILING_STATE': str,
    'MAILING_ZIP': str,
    'PRECINCT_NAME': str,
    'PRECINCT_CODE': str,
    'TOWNSHIP': str,
    'VILLAGE': str,
    'WARD': str
}

# Function to validate and transform data
def validate_and_transform_data(df):
    logger.info('Validating and transforming data...')
    
    # Check for required columns
    validate_required_columns(df)
    
    # Keep only the columns specified in config
    df = keep_required_columns(df)
    
    # Validate and transform data types
    df = validate_and_convert_dtypes(df, layout)
    
    # Apply all transformations
    df = transform_data(df)
    
    return df

# Read the input file
try:
    df = pd.read_csv(input_file)  # CSV format with comma delimiter
    logger.info(f'Input file read successfully from {input_file}. {len(df)} rows found.')
except Exception as e:
    logger.error(f'Error reading input file: {e}')
    raise

# Validate and transform the data
transformed_df = validate_and_transform_data(df)

# Save the transformed data as a parquet file
try:
    save_parquet(transformed_df, output_dir)
    logger.info(f'Transformed data saved successfully to {output_dir}.')
except Exception as e:
    logger.error(f'Error saving parquet file: {e}')
    raise

# Export Excel file for committee members
try:
    excel_file = export_excel(transformed_df, output_dir)
    logger.info(f'Excel file exported successfully: {excel_file}')
except Exception as e:
    logger.error(f'Error exporting Excel file: {e}')
    raise

# Export CSV file as backup
try:
    csv_file = export_csv(transformed_df, output_dir)
    logger.info(f'CSV file exported successfully: {csv_file}')
except Exception as e:
    logger.error(f'Error exporting CSV file: {e}')
    raise


logger.info('Weekly data processing completed successfully.')
