import os
import pandas as pd
from json import dump


df_map = {}
try:
    excel_reader = pd.ExcelFile('input/well_data.xlsx')
except FileNotFoundError:
    print(
        'Исходный Excel-файл не найден! '
        'Убедитесь, что файл well_data.xlsx находится в папке input'
    )
    exit(1)
for sheet_name in excel_reader.sheet_names:
    df_map[sheet_name] = excel_reader.parse(sheet_name, index_col=None)

out_path = f'{os.path.dirname(__file__)}/output'
if not os.path.exists(out_path):
    os.makedirs(out_path)


def validation(df: pd.DataFrame) -> list:
    # группируем данные по дате и ID скважины, считаем суммы
    new_df = df.groupby(['dt', 'well_id']).sum(
        ['oil_split', 'gas_split', 'water_split']).reset_index()

    # применяем округление для корректной работы операторов сравнения на следующем шаге
    new_df[['oil_split', 'gas_split', 'water_split']] = new_df[
        ['oil_split', 'gas_split', 'water_split']].round(10)

    # фильтруем данные
    filtered_df = new_df[(new_df['oil_split'] < 100) |
                         (new_df['gas_split'] < 100) |
                         (new_df['water_split'] < 100)]
    # в принципе, датафрейм filtered_df может удовлетворять условиям задачи 1,
    # но я на всякий случай решил сделать вывод в консоль
    res = []
    for row in filtered_df.itertuples():
        tmp = f"{row[1]}, well_id: {row[2]}, "
        if round(row[3], 10) < 100:
            tmp += f'oil_split: {round(row[3], 10)}, '
        if round(row[4], 10) < 100:
            tmp += f'gas_split: {round(row[4], 10)}, '
        if round(row[5], 10) < 100:
            tmp += f'water_split: {round(row[5], 10)}, '
        res.append(tmp)
    if len(res) > 0:
        print('Invalid data found:')
    else:
        print('Invalid data not found')
    return res


def allocation():
    # мержим два датафрейма по дате и well_id
    # в результате напротив каждого well_id появятся соотв. коэффициенты из листа rates
    res_df = df_map['splits'].merge(
        df_map['rates'],
        left_on=['dt', 'well_id'],
        right_on=['dt', 'well_id']
    )

    # вычисляем значения по формуле
    res_df['oil_rate'] = (res_df['oil_rate'] * res_df['oil_split'] / 100)
    res_df['gas_rate'] = (res_df['gas_rate'] * res_df['gas_split'] / 100)
    res_df['water_rate'] = (res_df['water_rate'] * res_df['water_split'] / 100)

    # приводим датафрейм к требуемому виду и сохраняем как Excel-файл
    res_df = res_df[['dt', 'well_id', 'layer_id', 'oil_rate', 'gas_rate', 'water_rate']]

    res_df.to_excel(f'{out_path}/results.xlsx', index=False)
    print('Excel file created')

    # выгружаем датафрейм в JSON
    res_json = {
        "allocation": {
            "data": []
        }
    }
    for row in res_df.itertuples():
        res_json["allocation"]["data"].append(
            {
                "wellId": row[2],
                "dt": f"{row[1].isoformat()}",
                "layerId": row[3],
                "oilRate": row[4],
                "gasRate": row[5],
                "waterRate": row[6]
            }
        )
    with open(f'{out_path}/result.json', 'w') as jsf:
        dump(res_json, jsf)
    print('JSON file created')


def main():
    for row in validation(df_map['invalid_splits'][
                              ['dt', 'well_id', 'oil_split', 'gas_split',
                               'water_split']]):
        print(row)
    allocation()


if __name__ == '__main__':
    main()
