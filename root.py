from pathlib import Path

ROOT_DIR = Path(__file__).parent
CONFIG_DIR = ROOT_DIR / "configs"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

SRC_DIR = ROOT_DIR / "src"
SRC_DIR.mkdir(parents=True, exist_ok=True)

MODEL_DIR = ROOT_DIR / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
