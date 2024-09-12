from datetime import datetime

import pandas as pd

from data_pipeline.input_values import shift_month
from data_pipeline.preprocessing_functions import change_months_in_templates
from data_pipeline.product_split import data_for_products
from data_pipeline.load_tabels import corr_reg, clients
from data_pipeline.template_volumes import RUSSIAN_LIST, russian_regions, regions_to_country, country_to_region, \
    FO_foreign

print(f'{datetime.now()}: start to create fractions template')

try:
    df_to_products = data_for_products.reset_index()
except:
    pass
data = df_to_products[['category', 'chanel', 'region', 'product_name', 'sum', 'id']]

result_table2 = pd.DataFrame(
    columns=['ID', 'week', 'chanel', 'country', 'FO', 'region', 'category', 'product_name', 'sum', 'id'])

for month in range(1, 13):
    temp_df = data
    temp_df['week'] = month
    result_table2 = pd.concat([result_table2, temp_df])

for item in ['Киргизия', 'ОАЭ', 'Монголия', 'Вьетнам']:
    try:
        RUSSIAN_LIST.remove(item)
    except:
        pass

for country in FO_foreign:
    try:
        result_table2['FO'].loc[result_table2['region'].eq(country)] = country
    except:
        pass

for item in FO_foreign:
    try:
        RUSSIAN_LIST.remove(item)
    except:
        pass

russian_regions = {country: 'Россия' for country in RUSSIAN_LIST}

regions_to_country = {**FO_foreign, **russian_regions}

for region in regions_to_country:
    result_table2['country'].loc[result_table2['region'].eq(region)] = regions_to_country[region]

for region in RUSSIAN_LIST:
    try:
        result_table2['FO'].loc[result_table2['region'].eq(region)] = \
        corr_reg['FO'].loc[corr_reg['right'].eq(region)].iloc[0]
    except:
        pass

for row, data in clients.iterrows():
    result_table2['ID'].loc[
        result_table2['region'].eq(data['region']) & result_table2['chanel'].eq(data['chanel'])] = row

result_table2 = change_months_in_templates(result_table2, shift_month)

print(f'{datetime.now()}: finish to create fractions template')
