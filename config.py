# Configuration for voter data processing
import os
"""
====================================================================================================
FILE: config.py
PURPOSE: Configuration Module - Pipeline Settings and Constants
====================================================================================================

Defines API endpoints, data directory paths, and other 
constants used throughout the pipeline.

VERSION HISTORY:
- v1.0: Initial version created on 2026-01-20 by Lora Covrett
====================================================================================================
"""
import os

# Configuration settings for the data processing pipeline
DATA_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
MASTER_FILE_PATH = "data/master/master_voters.csv"
PRECINCT_OUTPUT_DIR = "data/processed/precincts"

# Ensure data directories exist
os.makedirs(DATA_DIR, exist_ok=True)        
os.makedirs(PROCESSED_DIR, exist_ok=True)
# Ensure precinct output directory exists as well
os.makedirs(PRECINCT_OUTPUT_DIR, exist_ok=True)


# Party affiliation mapping
party_affiliation_map = {
    'D': 'Democrat',
    'R': 'Republican',
    'C': 'Constitution',
    'E': 'Reform',
    'G': 'Green',
    'L': 'Libertarian',
    'S': 'Socialist'
}

config = {
    'data_dir': DATA_DIR,
    'processed_dir': PROCESSED_DIR,
    'master_file_path': MASTER_FILE_PATH,
    'precinct_dir': PRECINCT_OUTPUT_DIR,                         
    'party_affiliation_map': party_affiliation_map,
    'output_columns': [
        'SOS_VOTERID',
        'LAST_NAME',
        'FIRST_NAME',
        'MIDDLE_NAME',
        'DATE_OF_BIRTH',
        'PARTY_AFFILIATION',
        'VOTER_STATUS',
        'RESIDENTIAL_ADDRESS1',
        'RESIDENTIAL_SECONDARY_ADDR',
        'RESIDENTIAL_CITY',
        'RESIDENTIAL_STATE',
        'RESIDENTIAL_ZIP',
        'MAILING_ADDRESS1',
        'MAILING_SECONDARY_ADDRESS',
        'MAILING_CITY',
        'MAILING_STATE',
        'MAILING_ZIP',
        'PRECINCT_NAME',
        'PRECINCT_CODE',
        'TOWNSHIP',
        'VILLAGE',
        'WARD'
    ]
}
