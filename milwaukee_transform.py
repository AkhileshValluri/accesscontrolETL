import pandas as pd
from transform import Transform
import utils as Utils


class MilwaukeeTransform(Transform): 
    @property
    def EXPECTED_FIELDS(self) -> list[str]:
        return [
            "Name",
            "Door Name",
            "Message Type",
            "Message Text",
            "Message Date/Time",
        ]

    # normalized *view* of EXPECTED_FIELDS (EXPECTED_FIELDS itself untouched)
    @property
    def normalized_expected(self): 
        return {Utils.normalize(f) for f in self.EXPECTED_FIELDS}

    def is_start_of_chunk(self, row, idx, df):
        found = set()

        for cell in Utils.compact_row(row):
            key = Utils.normalize(cell)
            if key in self.normalized_expected:
                found.add(key)

        return found == self.normalized_expected


    def is_end_of_chunk(self, row, idx, df):
        compacted_row = Utils.compact_row(row)

        if not compacted_row:
            return False

        first = compacted_row[0]

        if Utils.looks_like_datetime(first):
            return True

        if isinstance(first, str) and "relko" in first.strip().lower():
            return True

        return False

    def find_datetime_idx(self, chunk_rows): 
        # right most (visually) cell
        right_most_non_empty_cell = 0
        for row in chunk_rows:
            for idx, cell in enumerate(row): 
                if Utils.is_cell_empty(cell):
                    continue
                right_most_non_empty_cell = max(right_most_non_empty_cell, idx)
        return right_most_non_empty_cell

    def concatenate_related_rows(self, chunk_rows):
        results = []
        current = None

        date_col_idx = self.find_datetime_idx(chunk_rows)

        for row in chunk_rows:
            if date_col_idx >= len(row):
                continue

            dt_cell = row[date_col_idx]

            # entry of date is start of new logical row
            if not Utils.is_cell_empty(dt_cell):
                if current:
                    results.append(current)
                    current = row
                    continue
                else: 
                    current = row
                    continue
            
            if not current: 
                continue

            for idx, cell in enumerate(row):
                if idx == date_col_idx:
                    break
                if not Utils.is_cell_empty(cell):
                    current[idx] = str(current[idx]) + (
                        " " + str(cell).strip()
                        if current[idx]
                        else str(cell).strip()
                    )

        if current:
            results.append(current)

        return results


    def extract_out_name_data(self, chunk_rows):
        left_most_non_empty_cell = len(chunk_rows[0])
        number_of_data_points = 0

        for row in chunk_rows:
            for idx, cell in enumerate(row):
                if not Utils.is_cell_empty(cell):
                    if left_most_non_empty_cell == idx:
                        number_of_data_points += 1
                    elif left_most_non_empty_cell > idx:
                        number_of_data_points= 0
                        left_most_non_empty_cell = idx
                    break

        # all chunks may not have name entries, don't remove actual left column data
        if number_of_data_points * 2 > len(chunk_rows):
            # then the "left most" data is not name 
            return 

        parts = []
        for row in chunk_rows:
            if left_most_non_empty_cell < len(row):
                cell = row[left_most_non_empty_cell]
                if not Utils.is_cell_empty(cell):
                    parts.append(str(cell).strip())

        for i, row in enumerate(chunk_rows):
            if left_most_non_empty_cell < len(row):
                chunk_rows[i] = (
                    row[:left_most_non_empty_cell] +
                    row[left_most_non_empty_cell + 1:]
                )

        self.chunk_name = " ".join(parts)

    def extract_id(self, chunk_rows):
        id_end_delimiter = ')'
        id_start_delimiter = '('
        message_text_idx = self.EXPECTED_FIELDS.index("Message Text")

        for row in chunk_rows: 
            if len(row) <= message_text_idx: 
                continue
            message_text:str = str(row[message_text_idx])
            if Utils.is_cell_empty(message_text):
                continue
            id_start_idx = message_text.find(id_start_delimiter)
            id_end_idx = message_text.find(id_end_delimiter)
            if id_start_idx == -1 or id_end_idx == -1: 
                continue
            return message_text[id_start_idx: id_end_idx + 1]
        return ""

    def process_chunk(self, chunk_rows):
        """
        Convert one chunk into normalized rows
        """
        chunk_rows = self.concatenate_related_rows(chunk_rows)

        self.extract_out_name_data(chunk_rows)
        for i, row in enumerate(chunk_rows):
            chunk_rows[i] = Utils.compact_row(row)
            chunk_rows[i] = [self.chunk_name] + chunk_rows[i]

        id = self.extract_id(chunk_rows)
        for i, row in enumerate(chunk_rows):
            chunk_rows[i][self.EXPECTED_FIELDS.index("Message Text")] = id

        return chunk_rows

    def convert_schema(self, df):
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
                "Name": row.get("Name", ""),
                "Location": "Miluwakee",
                "Badge": row.get("Message Text", ""),
            })

        return pd.DataFrame(out_rows)