import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger("voter_data_pipeline")


def separate_by_precinct(
    master_file_path: str,
    output_dir: str,
) -> None:
    """Split the master voter file into one CSV per precinct, limited to Democrats.

    This routine reads the current master voter file, filters the
    records to Democrats only, and writes a CSV for each precinct into
    ``output_dir``. Precinct files are updated only when their
    contents change; unchanged files are left untouched.

    Args:
        master_file_path: path to the master CSV file
        output_dir: directory where precinct files should be stored
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    master_path = Path(master_file_path)
    if not master_path.exists():
        logger.warning(f"Master file not found, skipping precinct separation: {master_file_path}")
        return

    # read master data; ensure strings for voterid/precinct so grouping
    # doesn't convert to numeric types
    df = pd.read_csv(master_path, dtype={
        'SOS_VOTERID': str,
        'PRECINCT_NAME': str
    })

    # keep only Democrats
    df = df[df['PARTY_AFFILIATION'] == 'Democrat']

    logger.info(f"Separating master file into precincts (Democrats only) - {len(df)} rows to process")

    seen_precincts = set()
    for precinct, group in df.groupby('PRECINCT_NAME'):
        clean_name = str(precinct).replace("/", "-").replace(".", "").strip()
        if not clean_name:
            continue
        seen_precincts.add(clean_name)
        precinct_file = output_path / f"{clean_name}.csv"

        write_file = True
        if precinct_file.exists():
            try:
                existing = pd.read_csv(precinct_file, dtype={
                    'SOS_VOTERID': str,
                    'PRECINCT_NAME': str
                })
                # compare DataFrames ignoring index
                if existing.equals(group):
                    write_file = False
                    logger.info(f"No changes for precinct '{precinct}', leaving file in place")
            except Exception as e:
                logger.warning(f"Failed to compare existing file {precinct_file}: {e}")

        if write_file:
            group.to_csv(precinct_file, index=False)
            logger.info(f"Written precinct file: {precinct_file}")

    # Optionally we could cleanup files for precincts that no longer exist.
    # For now we simply log them.
    for existing_file in output_path.glob("*.csv"):
        name = existing_file.stem
        if name not in seen_precincts:
            logger.info(f"Existing precinct file '{existing_file}' has no matching voters in master; leaving unchanged")
