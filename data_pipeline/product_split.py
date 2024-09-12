from datetime import datetime

from data_pipeline.preprocessing_functions import get_float_columns, pipeline_for_df
from data_pipeline.load_tabels import data_for_categories, categories, id_products

print(f'{datetime.now()}: start to split by product')

data_for_products = pipeline_for_df(data_for_categories, categories)
data_for_products = data_for_products.drop(columns=['chanel_region', 'nom_1', 'nom_2', 'cat_3'])

region_summary = data_for_products.drop(columns=['product_name']).groupby(['category', 'chanel', 'region']).sum()
region_summary = region_summary[region_summary.sum(axis=1) != 0]
region_summary['sum'] = region_summary.sum(axis=1)
without_index = region_summary.reset_index()

data_for_products = data_for_products.groupby(['category', 'chanel', 'region', 'product_name']).sum()
data_for_products = data_for_products[data_for_products.sum(axis=1) != 0]
data_for_products['sum'] = data_for_products[get_float_columns(data_for_products)].sum(axis=1)

for index in data_for_products.index:
    try:
        data_for_products['sum'].loc[index] = data_for_products['sum'].loc[index] / float(without_index['sum'][
                                                                                              without_index[
                                                                                                  'category'].eq(
                                                                                                  index[0]) &
                                                                                              without_index[
                                                                                                  'chanel'].eq(
                                                                                                  index[1]) &
                                                                                              without_index[
                                                                                                  'region'].eq(
                                                                                                  index[2])])
    except:
        pass
data_for_products = data_for_products.reset_index()

data_for_products = data_for_products.merge(id_products, on='product_name', how='left')

df_to_products_forexp = data_for_products.copy()
data_for_products.to_excel('OUT/products.xlsx')

print(f'{datetime.now()}: finish to split by product')
