import pandas as pd
import regex as re
##### UTIL #####

def is_cell_empty(cell): 
    return pd.isna(cell) or str(cell).strip() == ""

def is_row_empty(row):
    return all(pd.isna(cell) or str(cell).strip() == "" for cell in row)

def compact_row(row):
    """Remove empty cells, preserve order"""
    return [str(cell).strip() for cell in row if not pd.isna(cell) and str(cell).strip() != ""]

def looks_like_datetime(value):
    DATE_REGEXES = [
        # 7/3/2025 9:40:01 AM
        r"\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s*(AM|PM)",
        # 9:40:01 AM
        r"\d{1,2}:\d{2}:\d{2}\s*(AM|PM)",
        # 07-09-2025
        r"\d{1,2}-\d{1,2}-\d{4}",
        # 2025-07-09
        r"\d{4}-\d{1,2}-\d{1,2}",
        # 7/3/2025
        r"\d{1,2}/\d{1,2}/\d{4}",
    ]

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False

    # Excel serial date
    if isinstance(value, (int, float)):
        return 20000 < value < 60000  # sane Excel date range

    s = str(value).strip()

    if not s:
        return False

    # Regex-based fast path
    for rx in DATE_REGEXES:
        if re.fullmatch(rx, s, re.IGNORECASE):
            return True

    # Pandas fallback (strict-ish)
    try:
        pd.to_datetime(s, errors="raise")
        return True
    except Exception:
        return False

def load_excel_raw(path, sheet_name=0):
    df = pd.read_excel(path, sheet_name=sheet_name, header=None)
    # Drop completely empty columns
    df = df.dropna(axis=1, how="all")
    return df

def normalize(s):
    return (
        s.lower()
        .replace(" ", "")
        .replace("/", "")
        .replace(":", "")
        .strip()
    )