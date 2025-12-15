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