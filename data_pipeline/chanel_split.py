from datetime import datetime
import pandas as pd
from data_pipeline.preprocessing_functions import pipeline_for_df, get_float_columns, get_object_columns
from data_pipeline.load_tabels import categories, data_for_chanels
from data_pipeline.split_funcitons import get_month_fractions, chanels_frac, chanels_region_frac

print(f'{datetime.now()}: start to split by chanel')

data_for_chanels = pipeline_for_df(data_for_chanels, categories)
# df_to_products = df.copy()

new_categories = sorted(list(data_for_chanels['category'].unique()))
unique_months = tuple(set(map(lambda x: int(x.strftime('%m')),
                              tuple([data_for_chanels.columns[data_for_chanels.dtypes == 'float64']][0]))))
object_columns = get_object_columns(data_for_chanels)
float_columns = get_float_columns(data_for_chanels)

pre_res_ch, mean_months_ch = get_month_fractions(data_for_chanels, object_columns, float_columns, unique_months)

to_out_ch = (pre_res_ch
             .drop(columns=['chanel_region', 'nom_1', 'nom_2', 'cat_3'])
             .groupby(['category', 'chanel'])[list(pre_res_ch.columns[pre_res_ch.dtypes == 'float64'])]
             .sum()
             .reset_index()
             )

chanels = sorted(list(data_for_chanels['chanel'].unique()))

chanels_frac_sum = chanels_frac(to_out_ch, new_categories)
chanels_toexp = chanels_frac_sum.copy()

need_mean = True
if need_mean:
    cols_to_mean_than_delete = chanels_frac_sum.columns[chanels_frac_sum.dtypes == 'float64']
    chanels_frac_sum['mean'] = chanels_frac_sum[chanels_frac_sum.columns[chanels_frac_sum.dtypes == 'float64']].mean(
        axis=1)
    chanels_frac_sum = chanels_frac_sum.drop(columns=cols_to_mean_than_delete)

print(f'{datetime.now()}: finish to split by chanel')
