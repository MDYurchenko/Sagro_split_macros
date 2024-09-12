import pandas as pd

prefix = 'OUT/sep2024-aug2025 many_steps/'
excel_list = [
    f'{prefix}+10-2_30.08.2024_12.27_obvl_products_regions_split_template.xlsx',
    f'{prefix}+20-4_30.08.2024_12.31_obvl_products_regions_split_template.xlsx',
    f'{prefix}+30-6_30.08.2024_12.33_obvl_products_regions_split_template.xlsx',
    f'{prefix}+40-8_30.08.2024_12.35_obvl_products_regions_split_template.xlsx',
    f'{prefix}+50-10_30.08.2024_13.34_obvl_products_regions_split_template.xlsx',
    f'{prefix}+80-16_30.08.2024_13.40_obvl_products_regions_split_template.xlsx',
    f'{prefix}-10+2_30.08.2024_13.42_obvl_products_regions_split_template.xlsx',
    f'{prefix}-20+4_30.08.2024_14.14_obvl_products_regions_split_template.xlsx',
    f'{prefix}main_30.08.2024_11.53_obvl_products_regions_split_template.xlsx',
    f'{prefix}main_30.08.2024_09.07_meat_products_regions_split_template.xlsx',
    f'{prefix}+3000-10_30.08.2024_14.23_meat_products_regions_split_template.xlsx',
]

pandas_list = [pd.read_excel(excel) for excel in excel_list]

result = pd.concat(pandas_list)

result.to_excel(f'{prefix}merged.xlsx')
