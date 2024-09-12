import pandas as pd
from data_pipeline.input_values import step, categories_file_name, main_file_name, chanel_file_name, region_file_name, \
    prices_file_name

corr_prod = pd.read_excel('static/corrections.xlsx', sheet_name='product')
corr_reg = pd.read_excel('static/corrections.xlsx', sheet_name='region')
sku_table = pd.read_excel('static/sku-3ier.xlsx')
id_products = pd.read_excel('static/id_products.xlsx')

data_for_categories = pd.read_excel(f'IN/{categories_file_name}')
categories = list(data_for_categories['category'].unique())

data_for_chanels = pd.read_excel(f'IN/{chanel_file_name}')
data_for_regions = pd.read_excel(f'IN/{region_file_name}')

df = pd.read_excel(f'IN/{main_file_name}')

prices = pd.read_excel(str(prices_file_name), sheet_name=f'{step}')
clients = pd.read_excel('static/clients.xlsx', index_col='client_id')

print('input tables were loaded')
