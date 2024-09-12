import numpy as np
import pandas as pd
from data_pipeline.preprocessing_functions import get_float_columns, get_object_columns

print('initialize split functions')


def get_month_fractions(df, objects_columns, float_columns, months):
    # делим табличку на две: со строковыми и числовыми признаками
    obj_data = df[objects_columns]
    num_data = df[float_columns]
    # создаем копию таблички с строковыми признаками, чтобы потом joinить к ней данные об объемах по месяцам
    pre_res = obj_data.copy()
    # создаем словарь для накопления средний значений по месяцам за исследуемый период
    mean_months = {}
    #
    for month in months:

        month_number = month
        # берем все названия колонок, у которых месяц равняется текущему месяцу и соединяем в таблице month_cols
        month_cols = [m for m in float_columns if m.month == month]  # .strftime('%m.%Y')
        # находим сумму по выбранному месяцу в разных годах
        month_sums = {m: num_data[m].sum(axis='rows') for m in float_columns if
                      m.month == month}  # .strftime('%m.%Y')
        # находим среднее значение между конкретным месяцем в разных годах
        mean_months[month] = np.mean(list(month_sums.values()))
        # выбираем из всех месяцев по всем годам только нужный нам месяц
        month_data = num_data[month_cols]

        # КОСТЫЛЬ, чтобы не делить на 0
        # month_data[month_data == 0] = 1

        # создаем шаблон для сохранения будущих долей
        month_fractions = pd.DataFrame(columns=month_cols)

        # рассчитываем в кажом году по данному месяцу доли
        for month, m_sum in month_sums.items():
            month_fractions[month] = month_data[month] / m_sum
        # заменяем nan на нули
        month_fractions = month_fractions.fillna(0)
        # находим среднюю долю по месяцу по всем годам
        month_fractions[f'{month_number}_mean_frac'] = month_fractions.mean(axis='columns')
        # joinим получившуюся среднюю долю конкретной категории по месяцу к итоговой табличке
        pre_res = pre_res.join(month_fractions[f'{month_number}_mean_frac'])

    return pre_res, mean_months


def chanels_frac(dataframe, categories):
    # создаем шаблон для распределения по каналам
    res = pd.DataFrame(columns=dataframe.columns)
    # получаем столбцы с числовыми данными
    float_columns = dataframe.columns[dataframe.dtypes == 'float64']
    # для каждой категории создаем две таблицы: одна с категорией, вторая - с сумммой по категории
    for category in categories:
        pre_df = dataframe.loc[dataframe['category'] == category]

        cat_sum = pre_df.sum()

        # выполняем деление полученных значений по каналу, категории, месяцу на сумму в этой категории в данном месяце
        cat_frac = pre_df[['category', 'chanel']].join(
            pre_df[pre_df.columns[pre_df.dtypes == 'float64']] / cat_sum[
                pre_df.columns[pre_df.dtypes == 'float64']])
        # подтягиваем результат по данной категории к результируеющей табличке со всеми категориями
        res = pd.concat([res, cat_frac], ignore_index=True)
        res[float_columns] = res[float_columns].astype('float64')

    return res


def chanels_region_frac(dataframe: pd.DataFrame, categories, chanels):
    res = pd.DataFrame(columns=dataframe.columns)
    float_columns = dataframe.columns[dataframe.dtypes == 'float64']

    for category in categories:
        for chanel in chanels:
            pre_df = dataframe.loc[dataframe['category'].eq(category) & dataframe['chanel'].eq(chanel)]

            cat_chan_sum = pre_df.sum()

            cat_chan_frac = pre_df[['category', 'chanel', 'region']].join(
                pre_df[pre_df.columns[pre_df.dtypes == 'float64']] / cat_chan_sum[
                    pre_df.columns[pre_df.dtypes == 'float64']])

            res = pd.concat([res, cat_chan_frac], ignore_index=True)
            res[float_columns] = res[float_columns].astype('float64')

    return res


def chanels_reg_split_mean(fracions: pd.DataFrame):
    sums = fracions.groupby(by=['category', 'chanel']).sum().reset_index()
    means = sums[get_object_columns(sums)].join(
        pd.DataFrame(sums[get_float_columns(sums)].sum(axis=1), columns=['sum']))
    cols = get_float_columns(fracions)

    fracions['sum1'] = fracions[cols].sum(axis=1)
    fracions = fracions.drop(columns=cols)

    fracions = fracions.merge(means, on=['category', 'chanel'])
    fl = fracions[get_float_columns(fracions)]

    fl['sum1'] = fl['sum1'] / fracions['sum']
    fracions['sum1'] = fl['sum1']

    fracions = fracions.drop(columns=['region_y', 'product_name_y', 'sum'])
    fracions.columns = ['category', 'chanel', 'region', 'product_name', 'mean']

    return fracions
