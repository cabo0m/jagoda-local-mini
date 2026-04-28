from pathlib import Path
import os

ROOT = Path(r"C:\jagoda-memory-api").resolve()
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "jagoda_memory.db"
ALLOWED_ROOTS = [
    ROOT,
    Path(r"C:\Users\micha\Documents").resolve(),
    Path(r"D:\Finexto").resolve(),
    Path(r"D:\MorenaTech").resolve(),
]


