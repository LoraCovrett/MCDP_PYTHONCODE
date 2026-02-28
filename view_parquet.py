"""
Usage: python view_parquet.py [date]
Example: python view_parquet.py 2025-01-12
"""

import pandas as pd
import sys
import os
import glob

def find_latest_parquet():
    """Find the most recent parquet file."""
    # Look for all parquet files
    pattern = "data/processed/load_date=*/voter_data.parquet"
    files = glob.glob(pattern)
    
    if not files:
        print("No parquet files found in data/processed/")
        return None
    
    # Sort by date (newest first)
    files.sort(reverse=True)
    return files[0]


def view_parquet(file_path, rows=10):
    """
    Load and display parquet file with summary statistics.
    
    Parameters:
    - file_path: Path to parquet file
    - rows: Number of rows to display (default 10)
    """
    print(f"Loading: {file_path}")
    print("=" * 80)
    
    # Load parquet file
    df = pd.read_parquet(file_path)
    
    # Basic info
    print(f"\n📊 Dataset Overview:")
    print(f"   Total Registered Voters: {len(df):,}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Load Date: {df['LOAD_DATE'].iloc[0]}")
    
    # Column info
    print(f"\n📋 Columns:")
    for col in df.columns:
        dtype = df[col].dtype
        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df)) * 100
        print(f"   {col:25} {str(dtype):15} ({null_count} nulls, {null_pct:.1f}%)")
    
    # Summary statistics for key fields
    print(f"\n📈 Key Metrics:")
    print(f"   Active Voters: {(df['VOTER_STATUS'] == 'ACTIVE').sum():,} ({(df['VOTER_STATUS'] == 'ACTIVE').mean()*100:.1f}%)")
    
    # Party affiliation distribution
    print(f"\n🎯 Party Affiliation Distribution:")
    for party, count in df['PARTY_AFFILIATION'].value_counts().items():
        pct = (count / len(df)) * 100
        print(f"   {party:20} {count:5,} ({pct:5.1f}%)")
    
    # Voter status distribution
    print(f"\n📊 Voter Status Distribution:")
    for status, count in df['VOTER_STATUS'].value_counts().items():
        pct = (count / len(df)) * 100
        print(f"   {status:15} {count:5,} ({pct:5.1f}%)")
    
    # Top precincts by voter count
    print(f"\n🏘️  Top 10 Precincts by Voter Count:")
    top_precincts = df['PRECINCT_NAME'].value_counts().head(10)
    for precinct, count in top_precincts.items():
        print(f"   {precinct:30} {count:4,}")
    
    # Sample records
    print(f"\n📄 First {rows} Records:")
    print("=" * 80)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 30)
    print(df.head(rows))
    
    # Interactive mode option
    print(f"\n" + "=" * 80)
    print("💡 Tips:")
    print("   - Access DataFrame in Python: df = pd.read_parquet('path/to/file.parquet')")
    print("   - Export to CSV: df.to_csv('output.csv', index=False)")
    print("   - Filter data: df[df['PARTY_AFFILIATION'] == 'Democrat']")
    print("   - Group by: df.groupby('PARTY_AFFILIATION')['PRECINCT_NAME'].count()")
    
    return df


if __name__ == "__main__":
    # Get file path from command line or find latest
    if len(sys.argv) > 1:
        date = sys.argv[1]
        file_path = f"data/processed/load_date={date}/firehydrants.parquet"
        
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            print("Looking for latest file instead...")
            file_path = find_latest_parquet()
    else:
        file_path = find_latest_parquet()
    
    if file_path:
        df = view_parquet(file_path)
        df.to_csv('output_voter_data.csv', index=False)
        # Optional: Open interactive Python session with df loaded
        # Uncomment to enable:
        # import code
        # code.interact(local=locals())
    else:
        print("\nUsage: python view_parquet.py [YYYY-MM-DD]")
        print("Example: python view_parquet.py 2025-01-12")