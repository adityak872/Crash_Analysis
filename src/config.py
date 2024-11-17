from pathlib import Path

# root directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent

# data directories path
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

# csv file paths for analysis
FILES = {
    "Charges_use": INPUT_DIR / "Charges_use.csv",
    "Damages_use": INPUT_DIR / "Damages_use.csv",
    "Endorse_use": INPUT_DIR / "Endorse_use.csv",
    "Primary_Person_use": INPUT_DIR / "Primary_Person_use.csv",
    "Restrict_use": INPUT_DIR / "Restrict_use.csv",
    "Units_use": INPUT_DIR / "Units_use.csv",
}

# Ensure the output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ANALYSIS_CONFIG = {
    "two_wheeler_keywords": ["MOTORCYCLE", "POLICE MOTORCYCLE"],

    "driver_keywords":["DRIVER","DRIVER OF MOTORCYCLE TYPE VEHICLE"],

    "valid_license_categories": ["COMMERCIAL DRIVER LICENSE","DRIVER LICENSE","OCCUPATIONAL"],

    "invalid_body_style":["UNKNOWN","NOT REPORTED","NA","OTHER (EXPLAIN IN NARRATIVE)"],

    "invalid_ethnicity_id":["NA","UNKNOWN"],

    "valid_insurance":["CERTIFICATE OF SELF-INSURANCE","LIABILITY INSURANCE POLICY","PROOF OF LIABILITY INSURANCE"],
}
