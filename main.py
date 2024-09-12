# import os


def main():
    # os.startfile(f'{os.getcwd()}/data_pipeline/input_values.py')
    # os.startfile(f'{os.getcwd()}/data_pipeline/load_tabels.py')
    # os.startfile(f'{os.getcwd()}/data_pipeline/preprocessing_functions.py')
    # exec(open('data_pipeline/input_values.py', "r").read())
    # exec(open('data_pipeline/load_tabels.py', "r").read())
    # exec(open('data_pipeline/split_funcitons.py', "r").read())
    # exec(open('data_pipeline/volumes_functions.py', "r").read())
    # exec(open('data_pipeline/preprocessing_functions.py', "r", encoding='utf-8').read())
    # exec(open('data_pipeline/month_3hier_split.py', "r").read())

    import data_pipeline
    import data_pipeline.month_3hier_split
    import data_pipeline.chanel_split
    import data_pipeline.region_split
    import data_pipeline.volumes_applying
    import data_pipeline.product_split
    import data_pipeline.template_volumes
    import data_pipeline.template_fractions
    import data_pipeline.export_data


if __name__ == '__main__':
    main()
