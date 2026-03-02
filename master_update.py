"""
====================================================================================================
FILE: master_update.py
PURPOSE: Master Data Update - Merge weekly data while preserving manual edits and tracking changes
====================================================================================================

DESCRIPTION:
    This module handles the weekly update of the master voter file. It:
    1. Preserves manually entered data (email, phone, etc.) from the master file
    2. Updates voter information from the new weekly data
    3. Tracks removed voters (voters who disappeared from this week's data)
    4. Tracks party affiliation changes
    
WORKFLOW:
    1. Load master file (contains manual edits)
    2. Load new weekly data
    3. Merge data, preserving manual columns
    4. Identify removed voters
    5. Identify party changes
    6. Save updated master
    7. Save change reports

====================================================================================================
"""

import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional

logger = logging.getLogger("voter_data_pipeline")


# Columns that contain manual data entry (preserve these from master)
MANUAL_ENTRY_COLUMNS = [
    'EMAIL',
    'CELL_PHONE',
    'HOME_PHONE',
    'FACEBOOK_PROFILE',
    'PARTICIPATION',
    'DONATE',
    'DO_NOT_CALL'
]

# Primary key to identify unique voters
VOTER_ID_COLUMN = 'SOS_VOTERID'


def load_master_file(master_file_path: str) -> Optional[pd.DataFrame]:
    """
    Load the master voter file.
    
    Args:
        master_file_path: Path to master CSV file
        
    Returns:
        DataFrame or None if file doesn't exist
    """
    try:
        if not Path(master_file_path).exists():
            logger.warning(f"Master file not found: {master_file_path}. This will be treated as first run.")
            return None
        
        master_df = pd.read_csv(master_file_path)
        logger.info(f"Loaded master file with {len(master_df)} voters")
        return master_df
    
    except Exception as e:
        logger.error(f"Error loading master file: {e}")
        raise


def identify_removed_voters(master_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify voters who were in the master file but are NOT in the new data.
    
    Args:
        master_df: Master voter DataFrame
        new_df: New weekly voter DataFrame
        
    Returns:
        DataFrame containing removed voters
    """
    # Get voter IDs from both datasets
    master_ids = set(master_df[VOTER_ID_COLUMN])
    new_ids = set(new_df[VOTER_ID_COLUMN])
    
    # Find IDs that were in master but not in new data
    removed_ids = master_ids - new_ids
    
    if removed_ids:
        logger.warning(f"Found {len(removed_ids)} voters removed from this week's data")
        removed_voters = master_df[master_df[VOTER_ID_COLUMN].isin(removed_ids)].copy()
        removed_voters['REMOVAL_DATE'] = datetime.now()
        removed_voters['REMOVAL_REASON'] = 'Not in weekly data'
        return removed_voters
    else:
        logger.info("No removed voters found")
        return pd.DataFrame()


def identify_party_changes(master_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify voters whose party affiliation changed.
    
    Args:
        master_df: Master voter DataFrame
        new_df: New weekly voter DataFrame
        
    Returns:
        DataFrame containing voters with party changes
    """
    # Merge on voter ID to compare party affiliations
    comparison = master_df[[VOTER_ID_COLUMN, 'PARTY_AFFILIATION', 'LAST_NAME', 'FIRST_NAME']].merge(
        new_df[[VOTER_ID_COLUMN, 'PARTY_AFFILIATION']],
        on=VOTER_ID_COLUMN,
        how='inner',
        suffixes=('_OLD', '_NEW')
    )
    
    # Find where party changed
    party_changes = comparison[
        comparison['PARTY_AFFILIATION_OLD'] != comparison['PARTY_AFFILIATION_NEW']
    ].copy()
    
    if len(party_changes) > 0:
        logger.warning(f"Found {len(party_changes)} voters with party affiliation changes")
        party_changes['CHANGE_DATE'] = datetime.now()
        party_changes['CHANGE_TYPE'] = 'Party Affiliation'
    else:
        logger.info("No party affiliation changes found")
    
    return party_changes


def merge_with_master(master_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge new data with master, preserving manual entry columns.
    
    Args:
        master_df: Master voter DataFrame (with manual edits)
        new_df: New weekly voter DataFrame
        
    Returns:
        Updated DataFrame with preserved manual entries
    """
    logger.info("Merging new data with master file...")
    
    # Get manual entry data from master
    manual_columns = [VOTER_ID_COLUMN] + [col for col in MANUAL_ENTRY_COLUMNS if col in master_df.columns]
    manual_data = master_df[manual_columns].copy()
    
    # Start with new data (this has all the updated voter information)
    merged_df = new_df.copy()
    
    # For each manual entry column, preserve values from master
    for col in MANUAL_ENTRY_COLUMNS:
        if col in manual_data.columns:
            # Create a mapping of voter_id -> manual_value
            manual_values = manual_data.set_index(VOTER_ID_COLUMN)[col].to_dict()
            
            # Update the merged dataframe with manual values where they exist
            # Only update if the voter exists in both (inner merge logic)
            merged_df[col] = merged_df[VOTER_ID_COLUMN].map(manual_values).fillna(merged_df[col])
            
            logger.info(f"Preserved manual entries for column: {col}")
    
    logger.info(f"Merge complete. Final dataset has {len(merged_df)} voters")
    return merged_df


def update_master_file(
    master_file_path: str,
    new_data_df: pd.DataFrame,
    output_dir: str
) -> Tuple[str, str, str]:
    """
    Update the master voter file with new weekly data.
    
    This function:
    1. Loads existing master file
    2. Preserves manual entry columns
    3. Identifies removed voters
    4. Identifies party changes
    5. Saves updated master
    6. Saves change reports
    
    Args:
        master_file_path: Path to master CSV file
        new_data_df: New weekly voter data
        output_dir: Directory for output files
        
    Returns:
        Tuple of (updated_master_path, removed_voters_path, party_changes_path)
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Load master file
    master_df = load_master_file(master_file_path)
    
    # If no master file exists (first run), use new data as master
    if master_df is None:
        logger.info("No master file found. Creating initial master file from weekly data.")
        updated_master = new_data_df.copy()
        removed_voters = pd.DataFrame()
        party_changes = pd.DataFrame()
    else:
        # Identify changes BEFORE merging
        removed_voters = identify_removed_voters(master_df, new_data_df)
        party_changes = identify_party_changes(master_df, new_data_df)
        
        # Merge new data with master, preserving manual entries
        updated_master = merge_with_master(master_df, new_data_df)
    
    # Save updated master file (overwrite existing)
    master_backup_path = output_path / f"master_backup_{timestamp}.csv"
    if master_df is not None:
        master_df.to_csv(master_backup_path, index=False)
        logger.info(f"Master file backup saved: {master_backup_path}")
    
    updated_master.to_csv(master_file_path, index=False)
    logger.info(f"Updated master file saved: {master_file_path}")
    
    # Save removed voters report
    removed_path = None
    if len(removed_voters) > 0:
        removed_path = output_path / f"removed_voters_{timestamp}.csv"
        removed_voters.to_csv(removed_path, index=False)
        logger.info(f"Removed voters report saved: {removed_path}")
    else:
        logger.info("No removed voters to report")
    
    # Save party changes report
    changes_path = None
    if len(party_changes) > 0:
        changes_path = output_path / f"party_changes_{timestamp}.csv"
        party_changes.to_csv(changes_path, index=False)
        logger.info(f"Party changes report saved: {changes_path}")
    else:
        logger.info("No party changes to report")
    
    # Generate summary
    summary = {
        'Update Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'Total Voters (Latest File)': len(new_data_df),
        'Total Voters (Master File)': len(master_df) if master_df is not None else 0,
        'Total Voters (Merged)': len(updated_master),
        'Removed Voters': len(removed_voters),
        'Party Changes': len(party_changes),
        'New Voters Added': len(updated_master) - (len(master_df) if master_df is not None else 0)
    }
    
    summary_df = pd.DataFrame([summary])
    summary_path = output_path / f"update_summary_{timestamp}.csv"
    summary_df.to_csv(summary_path, index=False)
    logger.info(f"Update summary saved: {summary_path}")
    
    # Log summary
    logger.info("="*60)
    logger.info("MASTER FILE UPDATE SUMMARY")
    logger.info("="*60)
    for key, value in summary.items():
        logger.info(f"{key}: {value}")
    logger.info("="*60)

    return str(master_file_path), str(removed_path) if removed_path else "", str(changes_path) if changes_path else ""

def initialize_master_file(initial_data_df: pd.DataFrame, master_file_path: str) -> str:
    """
    Initialize a new master file from scratch.
    Use this for the very first run.
    
    Args:
        initial_data_df: Initial voter data
        master_file_path: Path where master file should be created
        
    Returns:
        Path to created master file
    """
    logger.info("Initializing new master file...")
    
    master_df = initial_data_df.copy()
    master_df.to_csv(master_file_path, index=False)
    
    logger.info(f"Master file initialized with {len(master_df)} voters at: {master_file_path}")
    return master_file_path


def get_master_file_stats(master_file_path: str) -> dict:
    """
    Get statistics about the master file.
    
    Args:
        master_file_path: Path to master CSV file
        
    Returns:
        Dictionary of statistics
    """
    try:
        df = pd.read_csv(master_file_path)
        
        stats = {
            'total_voters': len(df),
            'file_path': master_file_path,
            'last_modified': datetime.fromtimestamp(Path(master_file_path).stat().st_mtime),
            'columns': list(df.columns),
            'party_breakdown': df['PARTY_AFFILIATION'].value_counts().to_dict() if 'PARTY_AFFILIATION' in df.columns else {}
        }
        
        # Count how many manual entries exist
        for col in MANUAL_ENTRY_COLUMNS:
            if col in df.columns:
                non_default = df[col] != 'unknown'
                stats[f'{col.lower()}_filled'] = non_default.sum()
        
        return stats
    
    except Exception as e:
        logger.error(f"Error getting master file stats: {e}")
        return {}