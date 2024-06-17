import pandas as pd
from pathlib import Path
from pprint import pp


class MergeSpreadsheets:

    def __init__(self,
                 sheet_dir_path: str = '',
                 primary_sheet_name: str = '',
                 output_dir: str = '',
                 output_fn: str = '',
                 index_col_name: str = '',) -> None:
        self.sheet_dir_path = sheet_dir_path
        self.primary_sheet_name: str = primary_sheet_name
        self.output_dir: str = output_dir
        self.output_fn: str = output_fn
        self.index_col_name: str = index_col_name
        self.dfs: list[dict] = []
        self.merged: pd.DataFrame = None
        self.primary_df: pd.DataFrame = None

    def _read_sheets(self) -> None:
        df: pd.DataFrame = pd.DataFrame()
        files: list[Path] = list(Path(self.sheet_dir_path).glob("*.xlsx"))
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
                self.index_col_name).to_dict('index')
            for index, updates in update_dict.items():
                for col, val in updates.items():
                    if col in self.primary_df.columns:
                        self.primary_df.loc[self.primary_df[self.index_col_name]
                                            == index, col] = val
        self.primary_df.sort_values(by=self.index_col_name, inplace=True)

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
    SHEET_DIR_PATH = '/Users/dan/Dev/scu/InformationExtraction/data'
    OUTPUT_DIR = '/Users/dan/Dev/scu/InformationExtraction/output/merged'
    PRIMARY_SHEET_NAME = 'WIP_VERSION_3d_DB.xlsx'
    OUTPUT_FN = PRIMARY_SHEET_NAME
    INDEX_COL_NAME = 'RecNum'

    merge = MergeSpreadsheets(sheet_dir_path=SHEET_DIR_PATH, output_dir=OUTPUT_DIR,
                              output_fn=OUTPUT_FN, primary_sheet_name=PRIMARY_SHEET_NAME, index_col_name=INDEX_COL_NAME)
    merge.exe()


if __name__ == "__main__":
    main()
