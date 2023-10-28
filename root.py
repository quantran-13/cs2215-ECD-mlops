from pathlib import Path

ROOT_DIR = Path(__file__).parent

CONFIG_DIR = ROOT_DIR / "configs"

DATA_DIR = ROOT_DIR / "data"
BACKUP_DIR = DATA_DIR / "backup"
CACHED_DIR = DATA_DIR / "cached"
CACHED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SRC_DIR = ROOT_DIR / "src"
MODEL_DIR = ROOT_DIR / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = ROOT_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
