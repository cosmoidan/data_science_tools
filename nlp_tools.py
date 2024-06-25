"""
Author: Dan Bright, cosmoid@tutu.io
License: GPLv3.0
Version: 1.0
First published: 20 May 2024
Description: 
    - A script to perform the following utility functions:
        * Extract substrings from a spreadsheet column
        * Validate extracted substrings against a hand-curated validation set
        * Count the number of non-empty & empty values in a spreadsheet column
        * Extract a random sample of user-defined size from a spreadsheet (excluding 
        ids listed in CSV file if required)
Usage:
    1) Define config parameters in main()
    2) Performed function is defined in the 'mode' config variable (in main())
    3) Run script: python nlp_tools.py
"""

import pandas as pd
from pandas._libs.missing import NAType
from pathlib import Path
from pprint import pp
import numpy as np


class NLPTools:

    def __init__(self,
                 data_files_dir: Path = None,
                 substrings: dict = dict(),
                 only_all: bool = False,
                 extraction_column: str = '',
                 id_column: str = '',
                 count_column: str = '',
                 output_found_column: str = '',
                 validation_column_name: str = '',
                 output_dir: str = 'output',
                 output_filename: str = '',
                 output_format: str = 'xlsx',
                 output_validation_filename: str = '',
                 output_validation_format: str = 'xlsx',
                 output_sample_filename: str = '',
                 output_sample_format: str = 'xlsx',
                 mode: str = 'extract',
                 sample_size: int = 0,
                 sample_exclude_file: str = '',
                 ) -> None:
        self.data_files_dir: Path = data_files_dir
        self.substrings: dict = substrings
        self.only_all: bool = only_all
        self.extraction_column: str = extraction_column
        self.id_column: str = id_column
        self.count_column: str = count_column
        self.validation_column_name: str = validation_column_name
        self.data: pd.DataFrame = pd.DataFrame()
        self.output_found_column: str = output_found_column
        self.output_dir: str = output_dir
        self.output_filename: str = output_filename
        self.output_format: str = output_format
        self.output_validation_filename: str = output_validation_filename
        self.output_validation_format: str = output_validation_format
        self.output_sample_filename: str = output_sample_filename
        self.output_sample_format: str = output_sample_format
        self.sample_size: int = sample_size
        self.extracted: pd.DataFrame = None
        self.validated: dict = dict()
        self.sample: dict = dict()
        self.sample_exclude_file: Path = Path(
            self.output_dir) / ('sample_exclude_file.csv') if not sample_exclude_file else Path(self.output_dir) / (sample_exclude_file + '.csv')
        self.sample_exclusions: pd.Series = None
        self.resp_format_unsupported = 'Output format currently unsupported!'
        self.resp_mode_unsupported = 'Selected mode currently unsupported!'
        self.resp_sample_size_error = 'There is an error in the sample size!'
        self.mode = mode

    def execute(self) -> None:
        if self.mode not in ['extract', 'validate', 'sample', 'count']:
            print(self.resp_mode_unsupported)
            return None
        else:
            self._get_data()
        if self.mode in ['extract', 'validate']:
            self._extract_strings()
        elif self.mode == 'sample':
            if type(self.sample_size) is int and self.sample_size > 0:
                self._generate_sample()
            else:
                print(self.resp_sample_size_error)
                return None
        elif self.mode == 'count':
            self.scrub_data = False
            self._count_col_values()
        self._write_output_file()

    def _count_col_values(self) -> None:
        df = self.data
        cc = self.count_column
        total_rows = df[self.id_column].count()
        non_empty = df[cc][df[cc].notna() & (df[cc] != '')].count()
        print(f'''Total rows: {total_rows}\nNon empty in {cc} column: {
              non_empty}\nEmpty in {cc} column: {total_rows - non_empty}''')

    def _clean_data(self) -> None:
        # method to clean the data
        def _clean_id_col() -> None:
            # Remove rows with non-finite values in id col
            self.data = self.data[np.isfinite(self.data[self.id_column])]
            # Ensure there are no NaN values in ID column
            self.data = self.data.dropna(subset=[self.id_column])
            # Convert ID column to integers
            self.data[self.id_column] = self.data[self.id_column].astype(int)
        _clean_id_col()

    def _get_data(self) -> pd.DataFrame:
        # load the xlsx files from the directory
        try:
            files = list(self.data_files_dir.glob("*.xlsx"))
            if not files:
                print("No files found in the specified directory.")
            dfs = []
            for file in files:
                try:
                    df = pd.read_excel(file)
                    df.dropna(how='all', inplace=True)
                except Exception as e:
                    print(f"Error reading file {file}. {e}. Skipping.")
                dfs.append(df)
            self.data = pd.concat(dfs, axis=0).reset_index(drop=True)
            self.data.drop_duplicates(subset=self.id_column, keep=False)
            self._clean_data()
        except Exception as e:
            print('Could not load data file!')

    def _pad_dict_lists(self, data: dict[list]):
        # pad the lists that are 'data' dict's values with 'None' to make all lists the same length
        len_of_longest_list = max(len(i) for i in data.values())
        for val in data.values():
            if len(val) < len_of_longest_list:
                val += [None] * (len_of_longest_list - len(val))

    def _map_synonyms(self, found: dict[list]):
        # map input search terms to any specified synonyms for output
        to_map = [(token, details['synonym_mapping'])
                  for token, details in self.substrings.items() if details['synonym_mapping']]
        for token_map_tuple in to_map:
            found[token_map_tuple[1]] += found[token_map_tuple[0]]
            found.pop(token_map_tuple[0])
            found = {k: v.sort() for k, v in found.items()}
            del self.substrings[token_map_tuple[0]]

    def _extract_strings(self) -> pd.DataFrame:
        # extract the substring tokens
        if not self.data.empty:
            found: dict = dict()
            data: pd.DataFrame = self.data.dropna(subset=[self.extraction_column])[
                [self.id_column, self.extraction_column]]
            for token, details in self.substrings.items():
                filtered: pd.DataFrame = data[
                    data[self.extraction_column].str.contains(details['regex'], regex=True)]
                indexes: list = filtered[self.id_column].to_list()
                found[token] = indexes
            self._map_synonyms(found=found)
            self._pad_dict_lists(data=found)
            self.extracted = pd.DataFrame.from_dict(
                found, orient='index').transpose()
            for token in self.extracted.columns:
                self.extracted[token] = self.extracted[token].astype(
                    pd.Int64Dtype())

    def _generate_sample(self) -> None:
        # generate a random sample
        df = self.data
        try:
            old_ids: pd.Series = pd.read_csv(
                self.sample_exclude_file, header=None, dtype=int)
            df = df[~df[self.id_column].isin(old_ids[0])]
        except FileNotFoundError:
            old_ids: pd.Series = pd.Series()
        sample: pd.DataFrame = df.sample(n=self.sample_size)
        if sample[self.id_column].isna().any():
            self._generate_sample()
        sample.sort_values(by=self.id_column, inplace=True)
        ids = [int(n)
               for n in sample[self.id_column].sort_values().to_list()]
        self.sample = {'ids': ids, 'records': sample}
        # update sample exclusions
        new_ids = pd.Series(self.sample['ids'], dtype='int')
        if not old_ids.empty:
            self.sample_exclusions: pd.Series = pd.concat(
                [old_ids, new_ids], sort=True).astype(int)
            self.sample_exclusions.sort_values(0, inplace=True)
        else:
            self.sample_exclusions: pd.Series = new_ids.astype(int)

    def _validate(self, print_to_console=False):
        #  validate the results against validation column
        total_hits = 0
        total_misses = 0
        total_manual = 0
        self.validated['true_pos'] = dict()
        self.validated['false_pos'] = dict()
        notes: pd.DataFrame = self.data[[
            self.validation_column_name, self.id_column]].dropna(inplace=False)
        noted_values: dict = dict()
        for token in self.substrings.keys():
            noted_values[token] = notes[notes[self.validation_column_name].str.contains(
                fr'\b{token}\b', regex=True)][self.id_column].sort_values().to_list()
            total_manual += len(noted_values[token])
        self._pad_dict_lists(data=noted_values)
        manually_recorded = pd.DataFrame.from_dict(noted_values)
        for token in manually_recorded.columns:
            manually_recorded[token] = manually_recorded[token].astype(
                pd.Int64Dtype())
        for token in self.substrings.keys():
            hit: pd.DataFrame = pd.merge(
                self.extracted, manually_recorded, on=[token])
            hit.replace({pd.NA: None}, inplace=True)
            self.validated['true_pos'][token] = hit[token].to_list()
            miss: pd.DataFrame = pd.merge(
                self.extracted, manually_recorded, on=[token], how='outer', indicator=True)
            miss.replace({pd.NA: None}, inplace=True)
            miss: pd.DataFrame = miss[miss['_merge'] != 'both']
            miss['_merge'] = miss['_merge'].cat.rename_categories(
                {'left_only': 'manual', 'right_only': 'script'})
            miss.rename(columns={'_merge': 'NOT_IN'}, inplace=True)
            miss_records: dict = miss[[token, 'NOT_IN']].to_dict(
                orient='records')
            self.validated['false_pos'][token] = [
                (r[token], r['NOT_IN']) for r in miss_records]
            total_hits += len([v for v in self.validated['true_pos']
                              [token] if v])
            total_misses += len([v for v in self.validated['false_pos']
                                [token] if v])
        if print_to_console:
            print(f'Total manually extracted values: {total_manual}')
            print(f'Total hits: {total_hits}')
            print(f'Total misses: {total_misses}')
            print(f'Total accuracy: {
                  round(total_hits / total_manual, 2) * 100}%')

    def _write_output_file(self) -> None:
        # write results output file
        if self.mode == 'sample':
            sample_range: str = f'{
                str(self.sample['ids'][0])}-{str(self.sample['ids'][-1])}'
            if self.output_sample_format == 'xlsx':
                output_location: Path = Path(
                    self.output_dir) / (self.output_sample_filename + 'rand_range[' + sample_range + '].xlsx')
                self.sample['records'].to_excel(
                    output_location, index=False, engine='openpyxl')
                self.sample_exclusions.to_csv(
                    self.sample_exclude_file, index=False, header=False)
            else:
                print(self.resp_format_unsupported)
        elif self.mode in ['extract', 'validate']:
            if not self.data.empty:
                df: pd.DataFrame = self.extracted
                rec_nums_list: list = [set(df[ss].dropna().tolist())
                                       for ss in self.substrings]
                rec_nums_set: set = set()
                for rn in rec_nums_list:
                    rec_nums_set.update(rn)
                result_df: pd.DataFrame = pd.DataFrame(
                    columns=[self.id_column, ', '.join([ss for ss in self.substrings])])
                records_with_substrings: list = list()
                for ss in self.substrings:
                    records_with_substrings.append(
                        {'substring': ss, 'recnums': df[ss].dropna().tolist()})
                result_data = []
                for rec_num in rec_nums_set:
                    if rec_num not in {r[self.id_column] for r in result_data}:
                        result_data.append({self.id_column: rec_num,
                                            self.output_found_column: ', '.join(s['substring'] for s in records_with_substrings if rec_num in s['recnums'])})
                    else:
                        for idx, d in enumerate(result_data):
                            if rec_num == d[self.id_column]:
                                result_data[idx][self.output_found_column] = ', '.join(
                                    r['substring'] for r in records_with_substrings if rec_num in r['recnums'])
                result_df = pd.DataFrame(result_data)
                result_df.sort_values(by=self.id_column, inplace=True)
            if self.mode == 'validate':
                if self.output_validation_format == 'xlsx':
                    output_location: Path = Path(
                        self.output_dir) / (self.output_validation_filename + '.xlsx')
                    self._validate(print_to_console=True)
                    validated_dict: dict = dict()
                    all_ids: list = self.data[[self.id_column, self.extraction_column]].dropna(
                        how='any')[self.id_column].sort_values().to_list()
                    for id in all_ids:
                        validated_dict[id] = dict()
                        for ss in self.substrings:
                            if id in self.validated['true_pos'][ss]:
                                validated_dict[id].update({ss: True})
                            if self.validated['false_pos'][ss] and id in [v[0] for v in self.validated['false_pos'][ss]]:
                                validated_dict[id].update({ss: False, f'{ss}_error_in': [
                                    v[1] for v in self.validated['false_pos'][ss] if v[0] == id][0]})
                    val_df = pd.DataFrame.from_dict(
                        validated_dict, orient='index')
                    val_df.rename_axis(self.id_column, inplace=True)
                    val_df.sort_values(by=self.id_column, inplace=True)
                    val_df.to_excel(output_location, index=True,
                                    na_rep='None', engine='openpyxl')
                else:
                    print(self.resp_format_unsupported)
            elif self.mode == 'extract':
                if self.output_format == 'xlsx':
                    output_location: Path = Path(
                        self.output_dir) / (self.output_filename + '.xlsx')
                    result_df.to_excel(output_location, index=False)
                else:
                    print(self.resp_format_unsupported)


def main() -> None:
    # configure and run the script
    data_files_dir: Path = Path(
        '/Users/dan/Dev/scu/InformationExtraction/data')
    output_dir: str = '/Users/dan/Dev/scu/InformationExtraction/output/substring_extractor'
    output_filename: str = 'alt_stand_validation'
    output_format: str = 'xlsx'
    output_validation_filename: str = 'alt_stand_validation'
    output_validation_format = 'xlsx'
    output_sample_filename: str = 'WIP_VERSION_3d_DB_'
    output_sample_format: str = 'xlsx'
    substrings: dict = {'MSL': {'regex': r'(?i)(?<=\b|(?<=\d))(MSL|mean\ssea\slevel)(?=\b|/)', 'synonym_mapping': None},
                        'AGL': {'regex': r'(?i)(?<=\b|(?<=\d))(AGL|above\sground\slevel)(?=\b|/)', 'synonym_mapping': None},
                        'ASL': {'regex': r'(?i)(?<=\b|(?<=\d))(ASL|above\ssea\slevel)(?=\b|/)', 'synonym_mapping': None},
                        'FL': {'regex': r'(?i)(?<=\b)(FL|flight\slevel)\s*\d+', 'synonym_mapping': 'MSL'},
                        }
    extraction_column: str = 'CLEANED Summary'
    id_column: str = 'RecNum'
    count_column: str = 'UAS ALT'
    output_found_substrings_column: str = 'Altitude Standard'
    validation_column_name: str = 'ALT NOTES'
    sample_size: int = 350
    sample_exclude_file: str = ''  #  do not inc. ext. Leave blank for default.
    mode: str = 'validate'  # from 'sample', 'extract', 'validate' & 'count'

    tools = NLPTools(
        data_files_dir=data_files_dir,
        substrings=substrings,
        only_all=False,
        extraction_column=extraction_column,
        id_column=id_column,
        count_column=count_column,
        output_found_column=output_found_substrings_column,
        validation_column_name=validation_column_name,
        output_dir=output_dir,
        output_filename=output_filename,
        output_format=output_format,
        output_validation_filename=output_validation_filename,
        output_validation_format=output_validation_format,
        output_sample_filename=output_sample_filename,
        output_sample_format=output_sample_format,
        sample_size=sample_size,
        sample_exclude_file=sample_exclude_file,
        mode=mode,
    )
    tools.execute()


if __name__ == "__main__":
    main()
