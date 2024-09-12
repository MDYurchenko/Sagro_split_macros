from datetime import datetime
from data_pipeline.month_3hier_split import data_mean_months_frac, for_template_3hier
from data_pipeline.region_split import region_frac_toexp
from data_pipeline.chanel_split import chanels_toexp
from data_pipeline.volumes_applying import month_data, res_volumes_3_ier, res_volumes_chanel, res_volumes_regions
from data_pipeline.template_volumes import result_table
from data_pipeline.template_fractions import result_table2
from data_pipeline.input_values import only_template
from data_pipeline import step, time, meat_or_obvl
from data_pipeline.product_split import df_to_products_forexp

print(f'{datetime.now()}: start export results')

prefix = f'OUT/{step}_{time}_{meat_or_obvl}'
if not only_template:

    data_mean_months_frac.to_excel(f'{prefix}_split_by_month.xlsx')

    for_template_3hier.to_excel(f'{prefix}_3_ier_fractions.xlsx')

    chanels_toexp.to_excel(f'{prefix}_chanels_fractions.xlsx')

    region_frac_toexp.to_excel(f'{prefix}_chanels_regions_frac.xlsx')

    month_data.to_excel(f'{prefix}_month_volume.xlsx')

    res_volumes_3_ier.to_excel(f'{prefix}_res_cat_volumes.xlsx')

    res_volumes_chanel.to_excel(f'{prefix}_res_chan_volumes.xlsx')

    res_volumes_regions.to_excel(f'{prefix}_res_reg_volumes.xlsx')

    df_to_products_forexp.to_excel(f'{prefix}_product_split.xlsx')

result_table.to_excel(f'{prefix}_products_regions_split_template.xlsx')

result_table2.to_excel(f'{prefix}_product_split_template.xlsx')

print(f'{datetime.now()}: finish export results')
