import re
import pandas as pd
import numpy as np
from datetime import datetime

EXPECTED_FIELDS = [
    "Name",
    "Door Name",
    "Message Type",
    "Message Text",
    "Message Date/Time",
]

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

##### PROCESSING PIPELINE #####

def split_into_chunks(df):

    def is_end_of_chunk_data(row):
        compacted_row = compact_row(row)

        if not compacted_row:
            return False

        first = compacted_row[0]

        if looks_like_datetime(first):
            return True

        if isinstance(first, str) and "relko" in first.strip().lower():
            return True

        return False

    def normalize(s):
        return (
            s.lower()
            .replace(" ", "")
            .replace("/", "")
            .replace(":", "")
            .strip()
        )

    # normalized *view* of EXPECTED_FIELDS (EXPECTED_FIELDS itself untouched)
    normalized_expected = {normalize(f) for f in EXPECTED_FIELDS}

    def is_start_of_chunk(row):
        found = set()

        for cell in compact_row(row):
            key = normalize(cell)
            if key in normalized_expected:
                found.add(key)

        return found == normalized_expected

    chunks = []
    current_chunk = []
    chunk_started = False

    for _, row in df.iterrows():

        if is_start_of_chunk(row):
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
            chunk_started = True
            continue  # header row is control, not data

        if chunk_started:
            if is_end_of_chunk_data(row):
                chunks.append(current_chunk)
                current_chunk = []
                chunk_started = False
            else:
                current_chunk.append(row.tolist())

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


# def process_chunk(chunk_rows):
#     """
#     Convert one chunk into normalized rows
#     """

#     def extract_out_name_data(chunk_rows):
#         left_most_non_empty_cell = len(chunk_rows[0])

#         for row in chunk_rows:
#             for idx, cell in enumerate(row):
#                 if not is_cell_empty(cell):
#                     left_most_non_empty_cell = min(left_most_non_empty_cell, idx)
#                     break

#         parts = []
#         for row in chunk_rows:
#             if left_most_non_empty_cell < len(row):
#                 cell = row[left_most_non_empty_cell]
#                 if not is_cell_empty(cell):
#                     parts.append(str(cell).strip())

#         # remove the column which has the name from all rows
#         for i, row in enumerate(chunk_rows):
#             if left_most_non_empty_cell < len(row):
#                 chunk_rows[i] = row[:left_most_non_empty_cell ] + row[left_most_non_empty_cell + 1:]
#         return " ".join(parts)

#     name = extract_out_name_data(chunk_rows)

#     for i, row in enumerate(chunk_rows):
#         row = compact_row(row)
#         chunk_rows[i] = [name] + row

#     # iterate over the rows again and concatenate all the contents in a column BETWEEN TWO DATE/TIME cols (some will be empty)
#     # use the is_cell_empty(cell) function to check

#     return chunk_rows
def process_chunk(chunk_rows):
    """
    Convert one chunk into normalized rows
    """

    def extract_out_name_data(chunk_rows):
        left_most_non_empty_cell = len(chunk_rows[0])

        for row in chunk_rows:
            for idx, cell in enumerate(row):
                if not is_cell_empty(cell):
                    left_most_non_empty_cell = min(left_most_non_empty_cell, idx)
                    break

        parts = []
        for row in chunk_rows:
            if left_most_non_empty_cell < len(row):
                cell = row[left_most_non_empty_cell]
                if not is_cell_empty(cell):
                    parts.append(str(cell).strip())

        for i, row in enumerate(chunk_rows):
            if left_most_non_empty_cell < len(row):
                chunk_rows[i] = (
                    row[:left_most_non_empty_cell] +
                    row[left_most_non_empty_cell + 1:]
                )

        return " ".join(parts)
    
    def concatenate_related_rows(chunk_rows):
        results = []
        current = None

        DATE_COL_IDX = EXPECTED_FIELDS.index("Message Date/Time")

        for row in chunk_rows:
            if DATE_COL_IDX >= len(row):
                continue

            dt_cell = row[DATE_COL_IDX]

            # start of a new logical row
            if not is_cell_empty(dt_cell):
                if current:
                    results.append(current)
                    current = row
                    continue
                else: 
                    current = row

            for idx, field in enumerate(EXPECTED_FIELDS):
                if field in("Name, Message Date/Time"):
                    continue
                
                field = EXPECTED_FIELDS.index(field)
                cell = row[idx]
                if not is_cell_empty(cell):
                    current[field] += (
                        " " + str(cell).strip()
                        if current[field]
                        else str(cell).strip()
                    )

        if current:
            results.append(current)

        return results

    # ---- name extraction ----
    name = extract_out_name_data(chunk_rows)

    # prepend name and compact
    for i, row in enumerate(chunk_rows):
        row = compact_row(row)
        chunk_rows[i] = [name] + row
    
    return concatenate_related_rows(chunk_rows)


def convert_schema(df):
    """
    df: pandas DataFrame with columns:
        Name, Door Name, Message Type, Message Text, Message Date/Time
    """

    out_rows = []

    for _, row in df.iterrows():
        dt = pd.to_datetime(row["Message Date/Time"], errors="coerce")

        out_rows.append({
            "Date": dt.strftime("%Y-%m-%d") if not pd.isna(dt) else "",
            "Time": dt.strftime("%H:%M:%S") if not pd.isna(dt) else "",
            "Device": row.get("Door Name", ""),
            "Event": row.get("Message Type", ""),
            "Badge": "",
            "Name": row.get("Name", ""),
            "Location": "Miluwakee",
            "Comments": row.get("Message Text", ""),
        })

    return pd.DataFrame(out_rows)


def main(path = "Relko Door Report July GD - Milwaukee.xls", output_path = "output.xlsx"):
    df = load_excel_raw(path)
    chunks = split_into_chunks(df)

    all_rows = []
    for chunk in chunks:
        all_rows.extend(process_chunk(chunk))

    normalized_df = pd.DataFrame(all_rows, columns=EXPECTED_FIELDS)
    final_output = convert_schema(normalized_df)
    final_output.to_excel(output_path, index=False)
    return normalized_df

main()