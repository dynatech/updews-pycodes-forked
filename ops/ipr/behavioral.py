import numpy as np
import pandas as pd


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
    personnel = pd.read_excel('input/OOMP form responses.xlsx', sheet_name='email', dtype=str)
    personnel = personnel.loc[:, ['Nickname', 'Name']].dropna().set_index('Name')
    per_personnel = personnel.groupby('Nickname', as_index=False)
    
    behavioral = pd.read_excel('input/OOMP form responses.xlsx', sheet_name='behavioral', dtype=str)
    behavioral.columns = behavioral.columns.str.lower()
    behavioral.loc[:, ['monitoring partner', 'previous mt', 'previous ct', 'backup monitoring personnel', 'backup monitoring personnel.1', 'monitoring partner (2)']] = behavioral.loc[:, ['monitoring partner', 'previous mt', 'previous ct', 'backup monitoring personnel', 'backup monitoring personnel.1', 'monitoring partner (2)']].replace(personnel.to_dict()['Nickname'])
    behavioral.loc[:, behavioral.columns[behavioral.columns.str.contains('rating for')]] = behavioral.loc[:, behavioral.columns[behavioral.columns.str.contains('rating for')]].replace({'5 - Outstanding': 5, '4 - Very satisfactory': 4, '4 - Very Satisfactory': 4, '4': 4, '3 - Satisfactory': 3, '3 - Average': 3, '2 - Unsatisfactory': 2, '2': 2, '1 - Poor': 1})
    behavioral = behavioral.loc[(behavioral.timestamp >= start) & (behavioral.timestamp <= end), :]
    
    personnel = per_personnel.apply(get_grades, behavioral=behavioral).reset_index(drop=True)
    return personnel
    

if __name__ == '__main__':
    start = '2021-01-01'
    end = '2021-06-01'
    df = main(start, end)
    print(df)