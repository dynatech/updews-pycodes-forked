# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 11:08:27 2020

@author: Meryll
"""

import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd


output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..//input_output//'))


#def main(start, end, sched, eval_df, mysql=True):
monitoring_ipr = pd.read_excel(output_path + 'monitoring_ipr.xlsx', sheet_name=None)

PM_grades = []
AM_grades = []

for name in set(monitoring_ipr.keys()) - set(['Summary']):
    indiv_ipr = monitoring_ipr[name]
    indiv_ipr.columns = indiv_ipr.columns.astype(str)
    PM = indiv_ipr.columns[indiv_ipr.columns.str.contains('20:00')]
    AM = indiv_ipr.columns[indiv_ipr.columns.str.contains('8:00')]
    PM_grades += list(indiv_ipr.loc[indiv_ipr.Output1 == 'Total', PM].values[0])
    AM_grades += list(indiv_ipr.loc[indiv_ipr.Output1 == 'Total', AM].values[0])
    
AM_grades = list(map(lambda x: np.round(x*100, 2), AM_grades))
PM_grades = list(map(lambda x: np.round(x*100, 2), PM_grades))

fig = plt.figure()
ax = fig.add_subplot()
bins = np.arange(50,101,5)
PM_n, PM_bins, PM_patches = ax.hist(PM_grades, bins)
AM_n, AM_bins, AM_patches = ax.hist(AM_grades, bins)

fig = plt.figure()
ax = fig.add_subplot()
width = 0.35
AM = ax.bar(np.arange(len(bins)-1)-width/2, AM_n, color='#434348', width=width, label='AM shifts')
PM = ax.bar(np.arange(len(bins)-1)+width/2, PM_n, color='#7CB5EC', width=width, label='PM shifts')

ax.bar_label(AM, labels=list(map(lambda x: int(x) if x!=0 else '', AM_n)), padding=8)
ax.bar_label(PM, labels=list(map(lambda x: int(x) if x!=0 else '', PM_n)), padding=8)
ax.legend(loc='lower right')
plt.xticks(np.arange(len(bins)-1), list(map(lambda x: str(x-5) + '-' + str(x), bins))[1:])
fig.suptitle('AM vs PM grade distribution', fontsize=18)
