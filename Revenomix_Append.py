"""
Created by: Chakradhar T.

On date: 11/10/2019

"""
import pandas as pd
import datetime
import logging



class IDSAppend:
    def is_not_used(self):
        pass

    def separator(self, df, sep_by=None, datatype=None, filter_value=None):
        self.is_not_used()
        df = pd.DataFrame(df)
        if sep_by is not None:
            df_with = df[df[sep_by].astype(datatype) != filter_value]
            df_without = df[df[sep_by].astype(datatype) == filter_value]
            return df_with, df_without
        else:
            return df, df

    def append(self, hist_df, new_df, key, datatype = None):
        self.is_not_used()
        if datatype is None:
            data_type = new_df[key].dtype
        else:
            data_type = datatype
        new_df = pd.DataFrame(new_df)
        hist_df = pd.DataFrame(hist_df)
        key_diff = set(hist_df[key].astype(data_type)).difference(new_df[key].astype(data_type))
        where_diff = hist_df[key].astype(data_type).isin(key_diff)
        logging.info("Slice Old Data accordingly and append to New Data")
        df_append = new_df.append(hist_df[where_diff], ignore_index=True, sort=True)

        return df_append

    def key_generator(self, df, columns=None, sep='|'):
        self.is_not_used()
        _df = pd.DataFrame(df)
        if columns is not None:
            col_list = list(columns)
        else:
            col_list = list(df.columns)
        _df['key_col'] = ''
        for index, i in enumerate(col_list):
            if index == 0:
                _df['key_col'] = _df[i].astype(str)
            else:
                _df['key_col'] = _df['key_col'].astype(str).str.cat(_df[i].astype(str), sep=sep)
        df = pd.DataFrame(_df)
        return df

