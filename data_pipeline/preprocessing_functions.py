import pandas as pd
import numpy as np

from .input_values import FO_regions_using
from .load_tabels import corr_reg, corr_prod, sku_table

print('initialize preprocessing functions')


def get_float_columns(df: pd.DataFrame):
    return df.columns[df.dtypes == 'float64']


def get_object_columns(df: pd.DataFrame):
    return df.columns[df.dtypes == 'object']


def data_processing(df: pd.DataFrame) -> pd.DataFrame:
    df['region'] = np.where(df['region'].isna(), df['chanel_region'], df['region'])
    float_columns = df.columns[df.dtypes == 'float64']
    df[df[float_columns] < 0] = 0
    return df


def korus_processing(df: pd.DataFrame, corr_prod: pd.DataFrame, corr_reg: pd.DataFrame) -> pd.DataFrame:
    for wrong_product, right_product in dict(zip(list(corr_prod['wrong']), list(corr_prod['right']))).items():
        df.loc[df['product_name'] == str(wrong_product), 'product_name'] = str(right_product)
    for wrong_region, right_region in dict(zip(list(corr_reg['wrong']), list(corr_reg['right']))).items():
        df.loc[df['region'] == str(wrong_region), 'region'] = str(right_region)
    return df


def FO_processing(df: pd.DataFrame, corr_prod: pd.DataFrame, corr_reg: pd.DataFrame) -> pd.DataFrame:
    for wrong_product, right_product in dict(zip(list(corr_prod['wrong']), list(corr_prod['right']))).items():
        df.loc[df['product_name'] == str(wrong_product), 'product_name'] = str(right_product)
    for wrong_region, right_region in dict(zip(list(corr_reg['wrong']), list(corr_reg['FO']))).items():
        df.loc[df['region'] == str(wrong_region), 'region'] = str(right_region)
    return df


def wrong_to_right(df: pd.DataFrame, dict_to_change: dict) -> pd.DataFrame:
    for third_ierarchy, product in dict_to_change.items():
        for right_name in product:
            df.loc[df['product_name'].str.contains(right_name, regex=False), 'category'] = right_name
    return df


def sku_category_changing(df: pd.DataFrame, changing_table) -> pd.DataFrame:
    for row in changing_table.index:
        df['category'].loc[df['product_name'].eq(changing_table['sku'].iloc[row])] = \
            changing_table['category'].iloc[
                row]
    return df


def mean_value_count(array) -> float:
    '''
    Функция возвращает среднее значение ненулевых значений массива.
    '''

    array = np.array(array)

    length = len(array[array != 0])

    sum = np.sum(array)

    if length == 0:
        mean = 0
    # elif length == 1:
    #    mean = sum
    else:
        mean = sum / length
    return mean


products_to_change = {
    'Шпик': ['Шпик боковой', 'Шпик хребтовой'],
    'Ребра свиные': ['Ребра свиные (ленточные с корейки)'],
    # '':['Набор для бульона']
}
columns_to_compare = ['category', 'product_name']


def pipeline_for_df(df: pd.DataFrame, categories: list):
    df = df.loc[df['category'].isin(categories)]
    # Подменяем регион сбыта на бизнес регион контрагента, если регион сбыта не определен
    df['region'] = np.where(df['region'].isna(), df['chanel_region'], df['region'])

    float_columns = get_float_columns(df)
    object_columns = get_object_columns(df)

    # Определяем колонки с категориальными и числовыми признаками.
    # **Заменяем** значения меньше нуля в объемах на нули.
    df[df[float_columns] < 0] = 0

    # костыль
    # Сейчас подменяем руками. Позже в БД его должны внести.
    df.loc[df['product_name'].isna(), 'product_name'] = 'Карбонад свинной б/к зам  ВУ пакет короб L СИБАГРО Люкс'

    if FO_regions_using:
        df = FO_processing(df, corr_prod, corr_reg)
    else:
        df = korus_processing(df, corr_prod, corr_reg)
    df = wrong_to_right(df, products_to_change)
    df = sku_category_changing(df, sku_table)

    return df


def month_shift(months: list, shift: int) -> dict:
    '''
    Смещает начало нумерации месяцев на указанный
    :param months: список месяцев для расчета
    :param shift: месяц, с которого надо начать нумерацию (то есть июль сделаем 1-ым)
    :return:
    '''
    return dict(
        zip(
            months,
            [months[(index + shift - 1) % len(months)] for index, value in enumerate(months)]
        )
    )


def change_months_in_templates(df: pd.DataFrame, shift) -> pd.DataFrame:
    '''
    Подменяет week в файле, подаваемом в модель в соответствиии со смещением, полученным в month_shift
    :param df:
    :return:
    '''
    months = list(df['week'].unique())
    changes = month_shift(months, shift)
    series = list(df['week'])

    for index, value in enumerate(series):
        series[index] = changes[value]
    df['week'] = series
    return df

def minus_NMK_semipig(main_df: pd.DataFrame, subtrahend_df: pd.DataFrame):
    return ...


print('loaded_preprocessing_funcitons')
