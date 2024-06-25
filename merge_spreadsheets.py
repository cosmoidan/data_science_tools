"""
Author: Dan Bright, cosmoid@tutu.io
License: GPLv3.0
Version: 1.0
First published: 17 June 2024
Description: 
    - A script to merge multiple excel spreadsheets into 
    into a single primary spreadsheet.
    - Values in primary sheet are overwritten with values 
    from additional spreadsheets (in the order they were loaded)
    - Rows are associated by the index column (INDEX_COL_NAME)
    - All spreadsheets need to share the same index column name
      (for associating rows)
    - Either all columns, or only those existing in the primary 
    sheet care merged, depending on the configuration.
Usage:
    1) Define config parameters in main()
        - If 'COPY_ALL' is configured to True, all columns in 
        additional spreadsheets are copied. If False, only those 
        columns present in the primary sheet are copied.
    2) Drop all spreadsheets into the directory at SHEET_DIR_PATH
    3) Run script: python merge_spreadsheets.py
"""

import pandas as pd
from pathlib import Path
from pprint import pp


class MergeSpreadsheets:

    def __init__(self,
                 sheets_dir_path: str = '',
                 primary_sheet_name: str = '',
                 output_dir: str = '',
                 output_fn: str = '',
                 primary_index_col_name: str = '',
                 merge_index_col_name: str = '',
                 copy_all: bool = False) -> None:
        self.sheets_dir_path = sheets_dir_path
        self.primary_sheet_name: str = primary_sheet_name
        self.output_dir: str = output_dir
        self.output_fn: str = output_fn
        self.primary_index_col_name: str = primary_index_col_name
        self.merge_index_col_name: str = merge_index_col_name
        self.dfs: list[dict] = []
        self.merged: pd.DataFrame = None
        self.primary_df: pd.DataFrame = None
        self.copy_all: bool = copy_all

    def _read_sheets(self) -> None:
        df: pd.DataFrame = pd.DataFrame()
        files: list[Path] = list(Path(self.sheets_dir_path).glob("*.xlsx"))
        if not files:
            raise Exception("No files found in the specified directory.")
        for file in files:
            df = pd.read_excel(file, engine="openpyxl")
            self.dfs.append({file.name: df})
        if not self.dfs:
            raise Exception("No files successfully read.")
        return self.dfs

    def _update_values(self):
        dfs: list = []
        for idx, sheet in enumerate(self.dfs):
            if self.primary_sheet_name in sheet.keys():
                self.primary_df = self.dfs.pop(idx)[self.primary_sheet_name]
        for idx, sheet in enumerate(self.dfs):
            for df in sheet.values():
                dfs.append(df)
        for update_df in dfs:
            update_dict: dict = update_df.set_index(
                self.merge_index_col_name).to_dict('index')
            for index, updates in update_dict.items():
                for col, val in updates.items():
                    if col in self.primary_df.columns or self.copy_all:
                        self.primary_df.loc[self.primary_df[self.primary_index_col_name]
                                            == index, col] = val
        self.primary_df.sort_values(
            by=self.primary_index_col_name, inplace=True)

    def _write_output(self) -> None:
        self.primary_df.to_excel(
            Path(self.output_dir) / (self.output_fn), index=False)

    def exe(self) -> None:
        try:
            self._read_sheets()
            self._update_values()
            self._write_output()
            print('Done!')
        except Exception as e:
            print(f'Merge failed: {e}')


def main() -> None:
    PRIMARY_SHEET_NAME = 'WIP_VERSON_3d_DB_rand_range(53-17286)_DB_COMPLETE.xlsx'
    SHEETS_DIR_PATH = '/Users/dan/Dev/scu/InformationExtraction/data'
    OUTPUT_DIR = '/Users/dan/Dev/scu/InformationExtraction/output/merged'
    OUTPUT_FN = 'alt_standard_validation_data.xlsx'
    PRIMARY_INDEX_COL_NAME = 'RecNum'
    MERGE_INDEX_COL_NAME = 'RecNum'
    COPY_ALL = False

    merge = MergeSpreadsheets(sheets_dir_path=SHEETS_DIR_PATH, output_dir=OUTPUT_DIR,
                              output_fn=OUTPUT_FN, primary_sheet_name=PRIMARY_SHEET_NAME, primary_index_col_name=PRIMARY_INDEX_COL_NAME, merge_index_col_name=MERGE_INDEX_COL_NAME, copy_all=COPY_ALL)
    merge.exe()


if __name__ == "__main__":
    main()
