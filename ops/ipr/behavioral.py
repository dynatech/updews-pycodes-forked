from collections import Counter
import numpy as np
import os
import pandas as pd

import lib as ipr_lib
import ipr

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//input_output//'))

def get_grades(personnel, behavioral):
    name = personnel.Nickname.values[0]
    lst_grade = []
    for col_name in ['monitoring partner', 'previous mt', 'previous ct', 'backup monitoring personnel', 'backup monitoring personnel.1', 'monitoring partner (2)']:
        col_filt = behavioral.columns.str.contains('rating')
        col_filt = col_filt & behavioral.columns.str.contains(col_name.replace(r'(2)', '').replace('.1', '').strip())
        if '1' in col_name:
            col_filt = col_filt & behavioral.columns.str.contains('1')
        else:
            col_filt = col_filt & ~(behavioral.columns.str.contains('1'))
        if '2' in col_name:
            col_filt = col_filt & behavioral.columns.str.contains('2')
        else:
            col_filt = col_filt & ~(behavioral.columns.str.contains('2'))
        temp_grade = np.array(list(behavioral.loc[behavioral[col_name] == name, col_filt].values.flatten()))
        lst_grade += list(temp_grade[~np.isnan(temp_grade)])
    personnel.loc[:, 'grade'] = np.round(100*np.mean(lst_grade)/5, 2)
    return personnel


def main(start, end):
    key = "1UylXLwDv1W1ukT4YNoUGgHCHF-W8e3F8-pIg1E024ho"
    personnel = ipr_lib.get_sheet(key, "personnel")
    personnel = personnel.loc[:, ['Nickname', 'Fullname']].dropna().set_index('Fullname')
    per_personnel = personnel.groupby('Nickname', as_index=False)
    
    behavioral = pd.read_excel(output_path+'behavioral.xlsx', sheet_name='Monitoring Behavior', dtype=str)
    behavioral.columns = behavioral.columns.str.lower()
    behavioral.loc[:, ['name', 'monitoring partner', 'previous mt', 'previous ct', 'backup monitoring personnel', 'backup monitoring personnel.1', 'monitoring partner (2)']] = behavioral.loc[:, ['name', 'monitoring partner', 'previous mt', 'previous ct', 'backup monitoring personnel', 'backup monitoring personnel.1', 'monitoring partner (2)']].replace(personnel.to_dict()['Nickname'])
    behavioral.loc[:, behavioral.columns[behavioral.columns.str.contains('rating for')]] = behavioral.loc[:, behavioral.columns[behavioral.columns.str.contains('rating for')]].replace({'5 - Outstanding': 5, '4 - Very satisfactory': 4, '4 - Very Satisfactory': 4, '4': 4, '3 - Satisfactory': 3, '3 - Average': 3, '2 - Unsatisfactory': 2, '2': 2, '1 - Poor': 1})
    behavioral = behavioral.loc[(pd.to_datetime(behavioral['date of shift']) >= start) & (pd.to_datetime(behavioral['date of shift']) <= end), :]
    behavioral = behavioral.drop_duplicates(['date of shift', 'name'], keep='last')
    
    personnel_grade = per_personnel.apply(get_grades, behavioral=behavioral).reset_index(drop=True)
    
    month_list = pd.date_range(start=pd.to_datetime(start), end=pd.to_datetime(end), freq='1M')
    month_list = list(map(lambda x: x.strftime('%B %Y'), month_list[month_list>='2021-09-01']))
    shift_sched = pd.DataFrame()
    for sheet_name in month_list:
        shift_sched = shift_sched.append(ipr.get_shift(key, sheet_name))
    total_shifts = pd.DataFrame(Counter(list(shift_sched['IOMP-MT'].values) + list(shift_sched['IOMP-CT'].values)).items(), columns=['name', 'shifts'])
    rated = pd.DataFrame(Counter(behavioral.loc[behavioral['date of shift'] >= '2021-09-01', 'name']).items(), columns=['name', 'rated'])
    
    return personnel_grade
    

if __name__ == '__main__':
    start = '2021-06-01'
    end = '2021-12-01'
    df = main(start, end)
    print(df)