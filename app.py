import argparse
import os
import re
import sys
from typing import List, Type

from milwaukee_transform import MilwaukeeTransform
from moselgk_transform import MoselGKTransform
from ms_transform import MSTransform
from transform import Transform


# =========================
# Colorized Logger
# =========================
class Log:
    RESET = "\033[0m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"

    @classmethod
    def info(cls, msg: str):
        print(f"{cls.BLUE}[INFO]{cls.RESET} {msg}")

    @classmethod
    def success(cls, msg: str):
        print(f"{cls.GREEN}[SUCCESS]{cls.RESET} {msg}")

    @classmethod
    def warn(cls, msg: str):
        print(f"{cls.YELLOW}[WARN]{cls.RESET} {msg}")

    @classmethod
    def error(cls, msg: str):
        print(f"{cls.RED}[ERROR]{cls.RESET} {msg}")


# =========================
# Main Application
# =========================
class ExcelSchemaConverterApp:
    SUPPORTED_SCHEMAS = {"Milwaukee", "MoselGK", "MS"}

    SCHEMA_TO_TRANSFORMER: dict[str, Type[Transform]] = {
        "milwaukee": MilwaukeeTransform,
        "moselgk": MoselGKTransform,
        "ms": MSTransform,
    }

    def __init__(self):
        self.args = self._parse_args()

    # ---------------- CLI ----------------
    def _parse_args(self):
        parser = argparse.ArgumentParser(
            description="Convert dirty access-control Excel reports into a normalized schema"
        )

        parser.add_argument(
            "--batch",
            help="Regex to match multiple Excel files in current directory",
        )

        parser.add_argument(
            "--input",
            help="Single input Excel file (ignored if --batch is provided)",
        )

        parser.add_argument(
            "--output",
            help="Output Excel file (only valid for single input mode)",
        )

        parser.add_argument(
            "--schema",
            choices=self.SUPPORTED_SCHEMAS,
            help="Input schema. If omitted, inferred from filename.",
        )

        return parser.parse_args()

    # ---------------- orchestration ----------------
    def run(self):
        try:
            files = self._resolve_input_files()
        except Exception as e:
            Log.error(str(e))
            sys.exit(1)

        Log.info(f"Found {len(files)} file(s) to process")

        for input_file in files:
            try:
                schema = self._resolve_schema(input_file)
                output_file = self._resolve_output_file(input_file)

                Log.info(f"Processing file: {input_file}")
                Log.info(f"Schema detected: {schema}")
                Log.info(f"Output file: {output_file}")

                self._process_single_file(input_file, output_file, schema)
                Log.success(f"Completed: {output_file}")

            except Exception as e:
                Log.error(f"Failed processing {input_file}: {e.with_traceback()}")

    # ---------------- helpers ----------------
    def _resolve_input_files(self) -> List[str]:
        if self.args.batch:
            pattern = re.compile(self.args.batch)
            files = [
                f for f in os.listdir(".")
                if pattern.search(f) and f.lower().endswith((".xls", ".xlsx"))
            ]

            if not files:
                raise ValueError("Batch regex matched no Excel files")

            return files

        if not self.args.input:
            raise ValueError("Either --input or --batch must be provided")

        if not os.path.exists(self.args.input):
            raise FileNotFoundError(f"Input file not found: {self.args.input}")

        return [self.args.input]

    def _resolve_schema(self, input_file: str) -> str:
        if self.args.schema:
            return self.args.schema

        fname = input_file.lower()
        for schema in self.SUPPORTED_SCHEMAS:
            if schema.lower() in fname:
                Log.warn(f"Inferred schema '{schema}' from filename")
                return schema

        raise ValueError(
            f"Could not infer schema from filename: {input_file}. "
            f"Please specify --schema."
        )

    def _resolve_output_file(self, input_file: str) -> str:
        if self.args.output:
            if self.args.batch:
                Log.warn("--output ignored in batch mode")
            else:
                if self.args.output.endswith("xls"):
                    Log.warn("--output has invalid format: 'xls'. Converting to 'xlsx'")
                    self.args.output += 'x'
                return self.args.output

        base, _ = os.path.splitext(input_file)
        return f"{base}_Converted.xlsx"

    # ---------------- core pipeline call ----------------
    def _process_single_file(self, input_file: str, output_file: str, schema: str):
        transformer_cls = self.SCHEMA_TO_TRANSFORMER.get(schema.lower())
        if not transformer_cls:
            raise ValueError(f"Unsupported schema: {schema}")

        transformer = transformer_cls(input_file, output_file)

        Log.info(f"Running transformer: {transformer_cls.__name__}")
        transformer.main()


if __name__ == "__main__": 
    ExcelSchemaConverterApp().run()