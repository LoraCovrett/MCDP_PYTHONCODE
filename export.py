import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("voter_data_pipeline")


def export_excel(df, output_dir):
    """
    Export voter data to Excel file with multiple sheets and formatting.
    
    Args:
        df: DataFrame containing voter data
        output_dir: Directory to save Excel file
        
    Returns:
        str: Path to exported Excel file
    """
    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = output_path / f"voter_data_{timestamp}.xlsx"
        
        # Export to Excel with formatting
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Sheet 1: Full voter data
            df.to_excel(writer, sheet_name='Voter Data', index=False)
            
            # Sheet 2: Summary by party
            if 'PARTY_AFFILIATION' in df.columns:
                party_summary = df['PARTY_AFFILIATION'].value_counts().reset_index()
                party_summary.columns = ['Party', 'Count']
                party_summary.to_excel(writer, sheet_name='Party Summary', index=False)
            
            # Sheet 3: Summary by precinct
            if 'PRECINCT_NAME' in df.columns:
                precinct_summary = df['PRECINCT_NAME'].value_counts().reset_index()
                precinct_summary.columns = ['Precinct', 'Count']
                precinct_summary.to_excel(writer, sheet_name='Precinct Summary', index=False)
            
            # Sheet 4: Summary statistics
            summary_stats = {
                'Metric': ['Total Voters', 'Total Columns', 'Export Date', 'Active Voters', 'Inactive Voters'],
                'Value': [
                    len(df),
                    len(df.columns),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    len(df[df['VOTER_STATUS'] == 'ACTIVE']) if 'VOTER_STATUS' in df.columns else 'N/A',
                    len(df[df['VOTER_STATUS'] == 'INACTIVE']) if 'VOTER_STATUS' in df.columns else 'N/A'
                ]
            }
            stats_df = pd.DataFrame(summary_stats)
            stats_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Format worksheets
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f'Voter data exported to Excel: {excel_file}')
        return str(excel_file)
    
    except Exception as e:
        logger.error(f'Error exporting to Excel: {e}')
        raise


def export_csv(df, output_dir):
    """
    Export voter data to CSV file.
    
    Args:
        df: DataFrame containing voter data
        output_dir: Directory to save CSV file
        
    Returns:
        str: Path to exported CSV file
    """
    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = output_path / f"voter_data_{timestamp}.csv"
        
        df.to_csv(csv_file, index=False)
        
        logger.info(f'Voter data exported to CSV: {csv_file}')
        return str(csv_file)
    
    except PermissionError as e:
      logger.error(f'Permission denied writing {csv_file}: {e}')
      raise PermissionError(f'Access denied: {csv_file}') from e
    except IOError as e:
      logger.error(f'Failed to write Excel file {csv_file}: {e}')
      raise IOError(f'Cannot write to file: {csv_file}') from e
    except Exception as e:
      logger.error(f'Unexpected error exporting to Excel: {e}')
      raise
