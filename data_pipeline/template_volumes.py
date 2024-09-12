from datetime import datetime
import pandas as pd

import data_pipeline.input_values
# from data_pipeline.input_values import price_coefficient
from data_pipeline.preprocessing_functions import get_float_columns, get_object_columns, change_months_in_templates
from data_pipeline.load_tabels import clients, prices, corr_reg
from data_pipeline.volumes_applying import res_volumes_regions
from data_pipeline.input_values import shift_month

print(f'{datetime.now()}: start create volumes template')

result_table = pd.DataFrame(columns=['ID', 'week', 'chanel', 'country', 'FO', 'region', 'category', 'vol', 'price'])

try:
    regions_for_result = res_volumes_regions.drop(columns=['mean'])
except:
    regions_for_result = res_volumes_regions

for month in get_float_columns(regions_for_result):
    month_nubmer = month.split('_')[0]
    temp_df = regions_for_result[list(get_object_columns(regions_for_result)) + [month]]
    temp_df.columns = list(get_object_columns(regions_for_result)) + ['vol']
    temp_df['week'] = month_nubmer
    result_table = pd.concat([result_table, temp_df])

RUSSIAN_LIST = list(result_table['region'].unique())

FO_foreign = {
    'Киргизия': 'Киргизия',
    'ОАЭ': 'ОАЭ',
    'Монголия': 'Монголия',
    'Вьетнам': 'Вьетнам',
    'Казахстан': 'Казахстан',
    'Нидерланды': 'Нидерланды'
}

for country in FO_foreign:
    try:
        result_table['FO'].loc[result_table['region'].eq(country)] = country
    except:
        pass

for item in FO_foreign:
    try:
        RUSSIAN_LIST.remove(item)
    except:
        pass

russian_regions = {country: 'Россия' for country in RUSSIAN_LIST}

regions_to_country = {**FO_foreign, **russian_regions}

country_to_region = {
    'Россия': RUSSIAN_LIST,
    'Киргизия': 'Киргизия',
    'ОАЭ': 'ОАЭ',
    'Монголия': 'Монголия',
    'Вьетнам': 'Вьетнам',
    'Казахстан': 'Казахстан',
    'Нидерланды': 'Нидерланды'
}

# for item in country_to_region['Россия']:
#    country_to_region[item] = 'Россия'
# country_to_region.pop('Россия')

for region in regions_to_country:
    result_table['country'].loc[result_table['region'].eq(region)] = regions_to_country[region]

for region in RUSSIAN_LIST:
    try:
        result_table['FO'].loc[result_table['region'].eq(region)] = \
        corr_reg['FO'].loc[corr_reg['right'].eq(region)].iloc[0]
    except:
        pass

result_table = change_months_in_templates(result_table, shift_month)

for month in get_float_columns(prices):
    for row, data in prices.iterrows():
        result_table['price'].loc[
            result_table['region'].eq(data['region'])
            & result_table['category'].eq(data['category'])
            & result_table['chanel'].eq(data['chanel'])
            & result_table['week'].eq(month)] = data[month]  # * price_coefficient

for row, data in clients.iterrows():
    result_table['ID'].loc[result_table['region'].eq(data['region']) & result_table['chanel'].eq(data['chanel'])] = row

if result_table[result_table['vol'] == 0]['vol'].count() > 0:
    v = result_table[result_table['vol'] == 0]['vol'].count()
    print(f'THERE ARE EMPTY VOLS: {v}')
result_table = result_table[result_table['vol'] != 0]

if result_table['price'].isna().count() > 0:
    result_table[result_table['price'].isna()].to_excel('OUT/no_price.xlsx')
    v = result_table['price'].isna().value_counts()
    print(f'THERE ARE EMPTY PRICES: {v}')

result_table = result_table.dropna(subset=['price'])

result_table['step'] = data_pipeline.input_values.step

print(f'{datetime.now()}: finish create volumes template')
