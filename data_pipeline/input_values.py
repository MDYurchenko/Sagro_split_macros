from datetime import datetime
import pandas as pd

input_data = pd.read_excel('IN/input_dependences.xlsx', sheet_name='input')

categories_file_name = str(input_data['categories_file_name'].values[0])
main_file_name = str(input_data['main_file_name'].values[0])
chanel_file_name = str(input_data['chanel_file_name'].values[0])
region_file_name = str(input_data['region_file_name'].values[0])
prices_file_name = str(input_data['prices_file_name'].values[0])
shift_month = int(input_data['first_month'].values[0])

only_template = bool(input_data['only_template'].values[0])
# price_coefficient = float(input_data['price_coeff'].values[0])

time = datetime.now().strftime('%d.%m.%Y_%H.%M')
meat_or_obvl = str(input_data['meat_or_obvl'].values[0])
FO_regions_using = False

# volumes_parameters
INITIAL_VOLUME = float(input_data['INITIAL_VOLUME'].values[0])
step = str(input_data['step'].values[0])

print('initialize values')
print(f'''production_type = {meat_or_obvl},
INITIAL VOLUME = {INITIAL_VOLUME}
''')
