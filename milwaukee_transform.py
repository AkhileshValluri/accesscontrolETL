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

    def is_start_of_chunk(self, row):
        found = set()

        for cell in Utils.compact_row(row):
            key = Utils.normalize(cell)
            if key in self.normalized_expected:
                found.add(key)

        return found == self.normalized_expected


    def is_end_of_chunk_data(self, row):
        compacted_row = Utils.compact_row(row)

        if not compacted_row:
            return False

        first = compacted_row[0]

        if Utils.looks_like_datetime(first):
            return True

        if isinstance(first, str) and "relko" in first.strip().lower():
            return True

        return False

    def split_into_chunks(self, df):
        chunks = []
        current_chunk = []
        chunk_started = False

        for _, row in df.iterrows():

            if self.is_start_of_chunk(row):
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = []
                chunk_started = True
                continue  # header row is control, not data

            if chunk_started:
                if self.is_end_of_chunk_data(row):
                    chunks.append(current_chunk)
                    current_chunk = []
                    chunk_started = False
                else:
                    current_chunk.append(row.tolist())

        if current_chunk:
            chunks.append(current_chunk)

        return chunks



    def concatenate_related_rows(self, chunk_rows):
        results = []
        current = None

        DATE_COL_IDX = self.EXPECTED_FIELDS.index("Message Date/Time")

        for row in chunk_rows:
            if DATE_COL_IDX >= len(row):
                continue

            dt_cell = row[DATE_COL_IDX]

            # start of a new logical row
            if not Utils.is_cell_empty(dt_cell):
                if current:
                    results.append(current)
                    current = row
                    continue
                else: 
                    current = row

            for idx, field in enumerate(self.EXPECTED_FIELDS):
                if field in("Name, Message Date/Time"):
                    continue
                
                field = self.EXPECTED_FIELDS.index(field)
                cell = row[idx]
                if not Utils.is_cell_empty(cell):
                    current[field] += (
                        " " + str(cell).strip()
                        if current[field]
                        else str(cell).strip()
                    )

        if current:
            results.append(current)

        return results


    def extract_out_name_data(self, chunk_rows):
        left_most_non_empty_cell = len(chunk_rows[0])

        for row in chunk_rows:
            for idx, cell in enumerate(row):
                if not Utils.is_cell_empty(cell):
                    left_most_non_empty_cell = min(left_most_non_empty_cell, idx)
                    break

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

        return " ".join(parts)

    def process_chunk(self, chunk_rows):
        """
        Convert one chunk into normalized rows
        """
        # ---- name extraction ----
        name = self.extract_out_name_data(chunk_rows)

        # prepend name and compact
        for i, row in enumerate(chunk_rows):
            row = Utils.compact_row(row)
            chunk_rows[i] = [name] + row
        
        return self.concatenate_related_rows(chunk_rows)


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
                "Badge": "",
                "Name": row.get("Name", ""),
                "Location": "Miluwakee",
                "Comments": row.get("Message Text", ""),
            })

        return pd.DataFrame(out_rows)