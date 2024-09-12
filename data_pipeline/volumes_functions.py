from datetime import datetime
from data_pipeline.preprocessing_functions import get_float_columns, get_object_columns
import pandas as pd

if __name__ == '__main__':
    print(f'{datetime.now()}: initialize volume functions')


def apply_volume_to_3ier(data, month_volumes):
    for month, volume in month_volumes.items():
        data[f'{month}_mean_frac'] = data[f'{month}_mean_frac'].apply(lambda x: float(x) * float(volume))
    return data


def apply_volumes_to_chanels(third_vols, chanels_frac):
    rows = list(third_vols.index)
    cols = list(third_vols.columns[third_vols.dtypes == 'float64'])
    res = chanels_frac.copy()

    for row in rows:
        for col in cols:
            previous_values = chanels_frac.loc[chanels_frac['category'] == row][col]
            insert_values = chanels_frac.loc[chanels_frac['category'] == row][col].apply(
                lambda x: x * third_vols.loc[row][col])

            res[col].loc[chanels_frac['category'] == row] = chanels_frac[col].loc[
                chanels_frac['category'] == row].apply(lambda x: x * third_vols.loc[row][col])

    return res


def apply_volumes_to_regions(chanel_volumes, categories, region_frac):
    rows = list(chanel_volumes['chanel'].unique())
    cols = list(chanel_volumes.columns[chanel_volumes.dtypes == 'float64'])

    res = region_frac.copy()

    for col in cols:
        for cat in categories:
            for row in rows:
                previous_values = res[col].loc[region_frac['chanel'].eq(row) & region_frac['category'].eq(cat)]

                insert_values = res[col].loc[region_frac['chanel'].eq(row) & region_frac['category'].eq(cat)].apply(
                    lambda x: x * float(list(
                        chanel_volumes.loc[chanel_volumes['chanel'].eq(row) & chanel_volumes['category'].eq(cat)][col])[
                                            0]))

                res[col].loc[region_frac['chanel'].eq(row) & region_frac['category'].eq(cat)] = insert_values
    return res


def app_vols_to_regions(chanel_volumes: pd.DataFrame, region_frac: pd.DataFrame):
    reg_frac_with_vols: pd.DataFrame = region_frac.merge(chanel_volumes,
                                                         how='inner',
                                                         left_on=['category', 'chanel'],
                                                         right_on=['category', 'chanel']
                                                         ).drop(columns=['mean_y'])
    lst = list(get_float_columns(reg_frac_with_vols))

    lst.remove('mean_x')
    obj_d = reg_frac_with_vols[get_object_columns(reg_frac_with_vols)]
    mul1 = reg_frac_with_vols[['mean_x']].astype(float)
    #mul1.to_excel('mul1.xlsx')
    mul2 = reg_frac_with_vols[lst].astype(float)
    #mul2.to_excel('mul2.xlsx')

    vols = pd.DataFrame(columns=lst)
    for col in mul2:
        vols[col] = mul2[col] * mul1['mean_x']

    vols.to_excel('vols.xlsx')
    return obj_d.join(vols, how='left')
