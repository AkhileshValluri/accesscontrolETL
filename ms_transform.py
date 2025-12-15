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
    chunk_dates = []

    @property
    def EXPECTED_FIELDS(self):
        return [
            "Date",
            "Time",
            "Device",
            "Event",
            "Badge",
            "Name",
            "Location",
        ]

    # ---------------- chunk detection ----------------

    def is_start_of_chunk(self, row, idx, df):
        """
        A chunk starts when a row has multiple filled columns
        and resembles a stable header row.
        """
        cells = compact_row(row.tolist())
        if len(cells) < 4:
            return False

        normalized = {normalize(c) for c in cells}

        expected = {
            "datetime",
            "device",
            "event",
            "badge",
            "cardholdername",
        }

        return expected.issubset(normalized)

    def is_end_of_chunk(self, row, idx, df):
        """
        Chunk ends on empty row or when a new date metadata row appears.
        """
        if is_row_empty(row):
            return True

        first_cell = row.iloc[0]
        return looks_like_datetime(first_cell)

    # ---------------- helpers ----------------

    def _extract_date_metadata(self, df, start_idx):
        """
        Walk upwards from chunk start to find date metadata
        in first column.
        """
        for i in range(start_idx - 1, -1, -1):
            cell = df.iloc[i, 0]
            if looks_like_datetime(cell):
                dt = pd.to_datetime(cell)
                return dt.date().isoformat()
        return ""

    # ---------------- chunk processing ----------------

    def process_chunk(self, chunk):
        """
        Process one MS chunk into normalized rows
        """
        rows = []
        current_date = None

        for row in chunk:
            compacted = compact_row(row)

            # Expect: Date/Time, Device, Event, Badge, Name
            if len(compacted) < 5:
                continue

            if not compacted:
                continue
            rows.append(compacted)

        return rows

    # ---------------- schema conversion ----------------

    def convert_schema(self, normalized_df: pd.DataFrame) -> pd.DataFrame:
        """
        MS is already normalized at this point.
        """
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

        return pd.DataFrame(rows)
