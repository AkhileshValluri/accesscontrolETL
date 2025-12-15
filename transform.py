from utils import * 

class Transform:
    def __init__(self, input_file, output_file):
        self.input_file = input_file
        self.output_file = output_file

    def split_into_chunks(self): 
        raise NotImplementedError("Subclasses should implement this")
    
    def process_chunk(self, chunk): 
        raise NotImplementedError("Subclasses should implement this")
    
    @property
    def EXPECTED_FIELDS(self):
        raise NotImplementedError("Subclasses should implement this")

    def convert_schema(self, normalized_df: pd.DataFrame) -> pd.DataFrame: 
        raise NotImplementedError("Subclasses should implement this")

    def is_start_of_chunk(self, row, idx, df):
        raise NotImplementedError("Subclasses should implement this")

    def is_end_of_chunk(self, row, idx, df): 
        raise NotImplementedError("Subclasses should implement this")

    def split_into_chunks(self, df):
        chunks = []
        current_chunk = []
        chunk_started = False

        for _, row in df.iterrows():

            if self.is_start_of_chunk(row, _, df):
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = []
                chunk_started = True
                continue  # header row is control, not data

            if chunk_started:
                if self.is_end_of_chunk(row, _, df):
                    chunks.append(current_chunk)
                    current_chunk = []
                    chunk_started = False
                else:
                    current_chunk.append(row.tolist())

        if current_chunk:
            chunks.append(current_chunk)

        return chunks

            

    def main(self) :
        df = load_excel_raw(self.input_file)
        chunks = self.split_into_chunks(df)

        all_rows = []
        for chunk in chunks:
            all_rows.extend(self.process_chunk(chunk))

        normalized_df = pd.DataFrame(all_rows, columns=self.EXPECTED_FIELDS)
        final_output = self.convert_schema(normalized_df)
        final_output.to_excel(self.output_file, index=False)
        return normalized_df