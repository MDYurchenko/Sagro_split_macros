from datetime import datetime

import pandas as pd
from data_pipeline.month_3hier_split import mean_months_frac, third_ier_to_months
from data_pipeline.chanel_split import chanels_frac_sum
from data_pipeline.region_split import ch_r_frac
from data_pipeline.input_values import INITIAL_VOLUME
from data_pipeline.preprocessing_functions import get_float_columns
from data_pipeline.volumes_functions import apply_volume_to_3ier, apply_volumes_to_chanels, apply_volumes_to_regions, \
    app_vols_to_regions
from data_pipeline.load_tabels import categories

print(f'{datetime.now()}: start to applying volumes')

month_volumes = {month: value * INITIAL_VOLUME for month, value in mean_months_frac.items()}
month_data = pd.DataFrame(data=month_volumes, index=[0])

mean_months_volumes = dict(zip(month_data.to_dict(orient='split', index=False)['columns'],
                               month_data.to_dict(orient='split', index=False)['data'][0]))

try:
    del mean_months_volumes['Unnamed: 0']
except:
    pass

floats_for_regs = get_float_columns(chanels_frac_sum)

for col in floats_for_regs:
    if col not in ch_r_frac.columns:
        ch_r_frac[col] = ch_r_frac['mean']

if len(get_float_columns(chanels_frac_sum)) == 1:
    for col in ['1_mean_frac', '2_mean_frac', '3_mean_frac', '4_mean_frac', '5_mean_frac', '6_mean_frac', '7_mean_frac',
                '8_mean_frac', '9_mean_frac', '10_mean_frac', '11_mean_frac', '12_mean_frac']:
        chanels_frac_sum[col] = chanels_frac_sum['mean']

res_volumes_3_ier = apply_volume_to_3ier(third_ier_to_months, mean_months_volumes)
res_volumes_chanel = apply_volumes_to_chanels(res_volumes_3_ier, chanels_frac_sum)
try:
    res_volumes_chanel = res_volumes_chanel.drop(columns='Unnamed: 0')
except:
    pass

ch_r_frac = ch_r_frac.drop(columns=['product_name'])

res_volumes_regions = app_vols_to_regions(res_volumes_chanel, ch_r_frac)

#if len(get_float_columns(ch_r_frac)) == 1:
#    for col in ['1_mean_frac', '2_mean_frac', '3_mean_frac', '4_mean_frac', '5_mean_frac', '6_mean_frac', '7_mean_frac',
#                '8_mean_frac', '9_mean_frac', '10_mean_frac', '11_mean_frac', '12_mean_frac']:
#        ch_r_frac[col] = ch_r_frac['mean']

#res_volumes_regions = apply_volumes_to_regions(res_volumes_chanel, categories, ch_r_frac)

print(f'{datetime.now()}: finish to applying volumes')
