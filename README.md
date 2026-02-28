# MCDP_PYTHONCODE

This repository contains scripts for processing voter data.

## Structure

- `config.py` - configuration settings
- `export.py` - data export logic
- `process_voter_data.py` - main processing script
- `validate.py` - data validation utilities
- `transform.py` - transformation logic
- `view_parquet.py` - tools for viewing parquet files
- `storage.py` - storage interaction
- `logging_config.py` - logging configuration

## Data layout

- `data/raw/` - raw input files
- `data/processed/` - processed output files with load dates

## Usage

1. Configure settings in `config.py`.
2. Run `process_voter_data.py` to process raw files.
3. Use other scripts for validation, exporting, or viewing results.

## Requirements

See `requirements.txt` for Python dependencies. Use a virtual environment and install with:

```bash
pip install -r requirements.txt
```

## License

Specify project license here.
