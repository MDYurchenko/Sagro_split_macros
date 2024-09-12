import pandas as pd
from datetime import datetime
import subprocess


def main():
    pipeline_df = pd.read_excel('IN/input_dependences.xlsx',
                                sheet_name='input')
    merged_result = None

    for index, row in pipeline_df.iterrows():
        categories_file_name = str(row['categories_file_name'])
        main_file_name = str(row['main_file_name'])
        chanel_file_name = str(row['chanel_file_name'])
        region_file_name = str(row['region_file_name'])
        prices_file_name = str(row['prices_file_name'])
        shift_month = int(row['first_month'])
        only_template = bool(row['only_template'])
        # price_coefficient = float(input_data['price_coeff'].values[0])

        time = datetime.now().strftime('%d.%m.%Y_%H.%M')
        meat_or_obvl = str(row['meat_or_obvl'])
        FO_regions_using = False

        # volumes_parameters
        INITIAL_VOLUME = float(row['INITIAL_VOLUME'])
        step = str(row['step'])

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

        subprocess.run(['ls',
                        'cd .\\data_pipeline\\', 'pyhton .\\month_3hier_split.py',
                        'pyhton .\\chanel_split.py', 'pyhton .\\region_split.py', 'pyhton .\\volumes_applying.py',
                        'pyhton .\\product_split.py', 'pyhton .\\template_volumes.py',
                        'pyhton .\\template_fractions.py',
                        ], shell=True
                       )
        # import data_pipeline.month_3hier_split
        # import data_pipeline.chanel_split
        # import data_pipeline.region_split
        # import data_pipeline.volumes_applying
        # import data_pipeline.product_split
        # import data_pipeline.template_volumes
        # import data_pipeline.template_fractions

        from data_pipeline.template_volumes import result_table
        if merged_result is None:
            merged_result = result_table
        else:
            print('it will be merged')
            merged_result = pd.concat([merged_result, result_table], axis=0)

        from data_pipeline.template_fractions import result_table2

        prefix = f'OUT/{step}_{time}_{meat_or_obvl}'

        result_table2.to_excel(f'{prefix}_product_split_template.xlsx')

    prefix = f'OUT/{time}'

    merged_result.to_excel(f'{prefix}_products_regions_split_template.xlsx')


if __name__ == '__main__':
    main()
