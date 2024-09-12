from datetime import datetime
import pandas as pd
from data_pipeline.preprocessing_functions import pipeline_for_df, get_float_columns, get_object_columns, \
    mean_value_count
from data_pipeline.load_tabels import categories, data_for_regions
from data_pipeline.split_funcitons import get_month_fractions, chanels_frac, chanels_region_frac, chanels_reg_split_mean

print(f'{datetime.now()}: start to split by region')

df_reg = pipeline_for_df(data_for_regions, categories)
# df_to_products = df.copy()

new_categories = sorted(list(df_reg['category'].unique()))
unique_months = tuple(set(map(lambda x: int(x.strftime('%m')), tuple([df_reg.columns[df_reg.dtypes == 'float64']][0]))))
object_columns = get_object_columns(df_reg)
float_columns = get_float_columns(df_reg)

pre_res_reg, mean_months_reg = get_month_fractions(df_reg, object_columns, float_columns, unique_months)

to_out_reg = (pre_res_reg
              .drop(columns=['chanel_region', 'nom_1', 'nom_2', 'cat_3'])
              .groupby(['category', 'chanel'])[list(pre_res_reg.columns[pre_res_reg.dtypes == 'float64'])]
              .sum() #only_numeric сделать
              .reset_index()
              )

chanels_reg = sorted(list(df_reg['chanel'].unique()))

with_region = (pre_res_reg
               .drop(columns=['chanel_region', 'nom_1', 'nom_2', 'cat_3'])
               .groupby(['category', 'chanel', 'region'])
               .sum()
               .reset_index()
               )

ch_r_frac = chanels_region_frac(with_region, new_categories, chanels_reg)

region_frac_toexp = ch_r_frac.copy()

need_mean = True
if need_mean:
    cols_to_mean_than_delete = ch_r_frac.columns[ch_r_frac.dtypes == 'float64']
    ch_r_frac_float = ch_r_frac[ch_r_frac.columns[ch_r_frac.dtypes == 'float64']]
    ch_r_frac = chanels_reg_split_mean(ch_r_frac)
    # ch_r_frac['mean'] = ch_r_frac[ch_r_frac.columns[ch_r_frac.dtypes == 'float64']].mean(axis=1)
    # ch_r_frac_float.fillna(0)
    # for index, row in ch_r_frac_float.iterrows():  # новое
    #    ch_r_frac['mean'].iloc[index] = mean_value_count(row)  # новое
    # ch_r_frac = ch_r_frac.drop(columns=cols_to_mean_than_delete)

print(f'{datetime.now()}: finish to split by region')
