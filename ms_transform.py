import pandas as pd
import re
from transform import Transform
from utils import (
    is_cell_empty,
    is_row_empty,
    compact_row,
    looks_like_datetime,
    normalize,
)

class MSTransform(Transform):

    LOCATION = "MS"
    chunk_metadata = []
    chunk_metadata_idx = 0

    @property
    def EXPECTED_FIELDS(self):
        return [
            "Date/Time",
            "Device",
            "Event",
            "Badge",
            "Cardholder Name",
        ]

    # ---------------- chunk detection ----------------

    def is_start_of_chunk(self, row, idx, df):
        """
        A chunk starts when a row has multiple filled columns
        and resembles a stable header row.
        """
        cells = compact_row(row.tolist())
        if len(cells) != 5:
            return False
        if looks_like_datetime(cells[0]): 
            cell = self._extract_date_metadata(df, idx)
            if cell:
                self.chunk_metadata.append({"date" : cell})
                return True
        return False


    def is_end_of_chunk(self, row, idx, df: pd.DataFrame):
        """
        Chunk ends on empty row or when a new date metadata row appears.
        """
        if len(compact_row(row)) > 1: 
            # in the middle of the chunk, don't check for metadata
            return False

        cell = df.iloc[idx, 0]
        if looks_like_datetime(cell):
            return True

    # ---------------- helpers ----------------

    def _extract_date_metadata(self, df, start_idx):
        """
        Walk upwards from chunk start to find date metadata
        in first column.
        """
        for i in range(start_idx - 1, -1, -1):
            row = df.iloc[i]
            if len(compact_row(row)) > 1: 
                # in the middle of the chunk, don't check for metadata
                return

            cell = df.iloc[i, 0]
            if looks_like_datetime(cell):
                return cell

    # ---------------- chunk processing ----------------

    def process_chunk(self, chunk):
        """
        Process one MS chunk into normalized rows
        """
        rows = []
        metadata = self.chunk_metadata[self.chunk_metadata_idx]
        current_date = metadata['date']

        for row in chunk:
            compacted = compact_row(row)

            # Expect: Date/Time, Device, Event, Badge, Name
            if len(compacted) < 5 or not compacted:
                continue

            compacted[0] = f"{current_date} {compacted[0]}"
            rows.append(compacted)
        
        self.chunk_metadata_idx += 1
        return rows

    # ---------------- schema conversion ----------------

    def convert_schema(self, normalized_df: pd.DataFrame) -> pd.DataFrame:
        rows = []

        for _, row in normalized_df.iterrows():
            dt = pd.to_datetime(row[0], errors="coerce")
            if pd.isna(dt):
                continue

            rows.append([
                dt.date().isoformat(),
                dt.time().strftime("%H:%M:%S"),
                row[1],   # Device
                row[2],   # Event
                row[3],   # Badge
                row[4],   # Name
                self.LOCATION,
            ])

        return pd.DataFrame(rows, columns=["Date" ,"Time" ,"Device" ,"Event" ,"Badge" ,"Name" ,"Location"])
