# -*- coding: utf-8 -*-
"""
Created on Mon May  6 16:34:23 2019

@author: Data Scientist 1
"""

import numpy as np
import pandas as pd


def corr_event(site_alert, activity):
    score = np.array([1, 2])
    site_code = site_alert['site_code'].values[0]
    movt_event = site_alert[site_alert.trigger_type.isin(['S', 's', 'G',
                                                          'g', 'M', 'm'])]
    movt_event['trigger_type'] = movt_event['trigger_type'].apply(lambda x: x.lower())
    movt_event['trigger_type'] = movt_event['trigger_type'].replace('m', 'g')

    if len(movt_event) == 0:
        activity.loc[activity.site_code == site_code, 'corr_event'] = 0
    elif 's' in movt_event['trigger_type'].values and 'g' in movt_event['trigger_type'].values:
        activity.loc[activity.site_code == site_code, 'corr_event'] = score[1]
    else:
        activity.loc[activity.site_code == site_code, 'corr_event'] = score[0]


def max_event(site_alert, activity):
    score = np.array([3, 4])
    site_code = site_alert['site_code'].values[0]
    if any(x in {'S', 'G', 'M'} for x in set(site_alert.trigger_type)):
        activity.loc[activity.site_code == site_code, 'max_event'] = score[1]
    elif any(x in {'s', 'g', 'm'} for x in set(site_alert.trigger_type)):
        activity.loc[activity.site_code == site_code, 'max_event'] = score[0]
    else:
        activity.loc[activity.site_code == site_code, 'max_event'] = 0
    

def freq_event(site_alert, activity):
    freq = np.array([1, 2, 3, 5])
    score = np.array([1, 2, 3, 4])
    site_code = site_alert['site_code'].values[0]
    movt_event = site_alert[site_alert.trigger_type.isin(['S', 's', 'G',
                                                          'g', 'M', 'm'])]
    movt_freq = len(set(movt_event.event_id))
#    print (site_code, movt_freq)
    try:
        activity.loc[activity.site_code == site_code, 'freq_event'] = score[movt_freq >= freq][-1]
    except:
        activity.loc[activity.site_code == site_code, 'freq_event'] = 0
    

def rain_event(site_alert, activity):
    site_code = site_alert['site_code'].values[0]
    rain_event = site_alert[site_alert.trigger_type == 'R']
    rain_freq = len(set(rain_event.event_id))
    activity.loc[activity.site_code == site_code, 'rain_event'] = rain_freq


def eq_event(site_alert, activity):
    site_code = site_alert['site_code'].values[0]
    rain_event = site_alert[site_alert.trigger_type == 'E']
    rain_freq = len(set(rain_event.event_id))
    activity.loc[activity.site_code == site_code, 'eq_event'] = rain_freq


def trigger_type(event_trigger):
    event_id = event_trigger['event_id'].values[0]
    triggers = ''.join(set(event_trigger.trigger_type))
    set_trigger = pd.DataFrame({'event_id': [event_id], 'set_trigger': [triggers]})
    return set_trigger


def event_counter(site_alert, activity):
    site_code = site_alert['site_code'].values[0]
    movt_event = site_alert[site_alert.trigger_type.isin(['S', 's', 'G',
                                                          'g', 'M', 'm'])]
    A3_event = movt_event[movt_event.trigger_type.isin(['S', 'G', 'M'])]
    A3_event['trigger_type'] = A3_event['trigger_type'].replace('M', 'G')
    print(site_code)
    if site_code == 'tga':
        print(A3_event[['event_id', 'trigger_type']])
    if len(A3_event) == 0:
        activity.loc[activity.site_code == site_code, 'A3_SG'] = 0
        activity.loc[activity.site_code == site_code, 'A3_S/G'] = 0
    else:
        per_A3_event = A3_event.groupby('event_id')
        A3_set_trigger = per_A3_event.apply(trigger_type)
        A3_count = len(A3_set_trigger)
        A3_corr_count = len(A3_set_trigger[list(map(lambda x: len(x) == 2, A3_set_trigger.set_trigger))])
        activity.loc[activity.site_code == site_code, 'A3_SG'] = A3_corr_count
        activity.loc[activity.site_code == site_code, 'A3_S/G'] = A3_count - A3_corr_count

    A2_event = movt_event[~movt_event.event_id.isin(A3_event.event_id)]
    A2_event['trigger_type'] = A2_event['trigger_type'].replace('m', 'g')
    if len(A2_event) == 0:
        activity.loc[activity.site_code == site_code, 'A2_sg'] = 0
        activity.loc[activity.site_code == site_code, 'A2_s/g'] = 0
    else:
        per_A2_event = A2_event.groupby('event_id')
        A2_set_trigger = per_A2_event.apply(trigger_type)
        A2_count = len(A2_set_trigger)
        A2_corr_count = len(A2_set_trigger[list(map(lambda x: len(x) == 2, A2_set_trigger.set_trigger))])
        activity.loc[activity.site_code == site_code, 'A2_sg'] = A2_corr_count
        activity.loc[activity.site_code == site_code, 'A2_s/g'] = A2_count - A2_corr_count
    

def main():
    public_alert = pd.read_csv('public_alert.csv')
    act = pd.DataFrame({'site_code': sorted(set(public_alert.site_code))})
    
    site_alert = public_alert.groupby('site_id')
    # correlation of surficial and subsurface movement
    site_alert.apply(corr_event, activity=act).reset_index(drop=True)   
    # maximum event
    site_alert.apply(max_event, activity=act).reset_index(drop=True)
    # frequency of event with movt
    site_alert.apply(freq_event, activity=act).reset_index(drop=True)
    # frequency of rainfall event
    site_alert.apply(rain_event, activity=act).reset_index(drop=True)
    # frequency of eq event
    site_alert.apply(eq_event, activity=act).reset_index(drop=True)
    # frequency of A2 and event
    site_alert.apply(event_counter, activity=act).reset_index(drop=True)
    
    act = act.fillna(0)
    act['total'] = act['A2_s/g'] + (1.5 * act['A2_sg']) + (8 * act['A3_S/G']) + (12 * act['A3_SG'])
    act['site_code'] = act['site_code'].apply(lambda x: x.upper())
    act = act.sort_values('site_code')#, ascending=False)
    
    return public_alert, act


if __name__ == "__main__":
    
    public_alert, act = main()
    print(act)
    
    act.to_excel('site_activity.xlsx', index=False)