import pandas as pd
from datetime import datetime
from data_pipeline.preprocessing_functions import pipeline_for_df, get_float_columns, get_object_columns
from data_pipeline.load_tabels import categories, df
from data_pipeline.split_funcitons import get_month_fractions, chanels_frac, chanels_region_frac

print(f'{datetime.now()}: start to split by month, hierarchy')

df = pipeline_for_df(df, categories)
#df_to_products = df.copy()

new_categories = sorted(list(df['category'].unique()))
unique_months = tuple(set(map(lambda x: int(x.strftime('%m')), tuple([df.columns[df.dtypes == 'float64']][0]))))
object_columns = get_object_columns(df)
float_columns = get_float_columns(df)

pre_res, mean_months = get_month_fractions(df, object_columns, float_columns, unique_months)

# находим среднюю долю по месяцу относительно всего года
mean_months_frac = {key: value / sum(list(mean_months.values())) for key, value in mean_months.items()}
# делаем табличку для средних долей по месяцу относительно всего года
data_mean_months_frac = pd.DataFrame(data=mean_months_frac, index=[0])

to_out = (pre_res
          .drop(columns=['chanel_region', 'nom_1', 'nom_2', 'cat_3'])
          .groupby(['category', 'chanel'])[list(pre_res.columns[pre_res.dtypes == 'float64'])]
          .sum()
          .reset_index()
          )

third_ier_to_months = to_out.groupby(['category']).sum()

for_template_3hier = third_ier_to_months.copy()

print(f'{datetime.now()}: finish to split by month, hierarchy')

if __name__ == '__main__':
    chanels = sorted(list(df['chanel'].unique()))

    chanels_frac_sum = chanels_frac(to_out, new_categories)
    chanels_toexp = chanels_frac_sum.copy()

    need_mean = True
    if need_mean:
        cols_to_mean_than_delete = chanels_frac_sum.columns[chanels_frac_sum.dtypes == 'float64']
        chanels_frac_sum['mean'] = chanels_frac_sum[chanels_frac_sum.columns[chanels_frac_sum.dtypes == 'float64']].mean(
            axis=1)
        chanels_frac_sum = chanels_frac_sum.drop(columns=cols_to_mean_than_delete)

    with_region = (pre_res
                   .drop(columns=['chanel_region', 'nom_1', 'nom_2', 'cat_3'])
                   .groupby(['category', 'chanel', 'region'])
                   .sum()
                   .reset_index()
                   )

    ch_r_frac = chanels_region_frac(with_region, new_categories, chanels)

    region_frac_toexp = ch_r_frac.copy()

    need_mean = True
    if need_mean:
        cols_to_mean_than_delete = ch_r_frac.columns[ch_r_frac.dtypes == 'float64']
        ch_r_frac['mean'] = ch_r_frac[ch_r_frac.columns[ch_r_frac.dtypes == 'float64']].mean(axis=1)
        ch_r_frac = ch_r_frac.drop(columns=cols_to_mean_than_delete)
