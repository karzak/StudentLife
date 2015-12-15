from __future__ import division
import numpy as np
import pandas as pd
import os, glob, gc, dateutil

#To get an idea of which variables will be used in the model, we will read in each table and compute output variables.
#Shape denotes how many participants are in each outcome variable. 
#Overall average = groupby('uid')
#Daily average = groupby['date', 'uid'] then groupby('uid')
pd.options.mode.chained_assignment = None
#Read activity table and calculate average activity level and average daily activity level
os.chdir(r'/data/final/dataset/tables/activity')
activity_iterator = pd.read_csv(r'/data/final/dataset/tables/activity/activity.csv', index_col = None, header = 0, iterator = True, chunksize = 10000)
#Overall Average
activity_piece = [x.groupby('uid')[' activity inference'].agg(['sum', 'count']) for x in activity_iterator]
activity_agg = pd.concat(activity_piece).groupby(level=0).sum()
activity_agg.reset_index(inplace =True)
activity_agg[' activity inference'] = activity_agg['sum']/activity_agg['count']
activity_agg = activity_agg.drop(['sum', 'count'], axis = 1)
#shape = (49, 2)
os.chdir(r'/data/StudentLife/Model Tables')
#output to table for interactive exploratory analysis with iPythonNotebook
activity_agg.to_csv('activity.csv' ,index = False)
del activity_piece
del activity_iterator

activity_iterator = pd.read_csv(r'/data/final/dataset/tables/activity/activity.csv', index_col = None, header = 0, iterator = True, chunksize = 10000)
#Daily Average
activity_piece = [x.groupby(['date', 'uid'])[' activity inference'].agg(['sum', 'count']) for x in activity_iterator]
activity_daily = pd.concat(activity_piece).groupby(level=[0,1]).sum()
activity_daily.reset_index(inplace =True)
activity_daily['daily_act_inf'] = activity_daily['sum']/activity_daily['count']
activity_daily_ave = activity_daily.groupby('uid').mean()
activity_daily_ave.reset_index(inplace =True)
activity_daily_ave = activity_daily_ave.drop(['sum', 'count'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
activity_daily_ave.to_csv('act_daily_ave.csv', index = False)
del activity_piece
del activity_iterator
gc.collect()

#Calculate average audio level and average daily audio level
os.chdir(r'/data/final/dataset/tables/audio')
audio_iterator = pd.read_csv('/data/final/dataset/tables/audio/audio.csv',
index_col = None, header = 0, iterator = True, chunksize = 10000)
audio_piece = [x.groupby('uid')[' audio inference'].agg(['sum', 'count']) for x in audio_iterator]
audio_agg = pd.concat(audio_piece).groupby(level=0).sum()
audio_agg.reset_index(inplace =True)
audio_agg['audio_inference'] = audio_agg['sum']/audio_agg['count']
audio_agg = audio_agg.drop(['sum', 'count'], axis = 1)
#shape =(49, 2)
del audio_piece
del audio_iterator
gc.collect()
os.chdir(r'/data/StudentLife/Model Tables')
audio_agg.to_csv('audio.csv' ,index = False)
audio_iterator = pd.read_csv('/data/final/dataset/tables/audio/audio.csv', index_col = None, header = 0, iterator = True, chunksize = 10000)
audio_piece = [x.groupby(['date', 'uid'])[' audio inference'].agg(['sum', 'count']) for x in audio_iterator]
audio_daily = pd.concat(audio_piece).groupby(level=[0,1]).sum()
audio_daily.reset_index(inplace = True)
audio_daily['daily_aud_inf'] = audio_daily['sum']/audio_daily['count']
audio_daily_ave = audio_daily.groupby('uid').mean()
audio_daily_ave.reset_index(inplace =True)
audio_daily_ave = audio_daily_ave.drop(['sum', 'count'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
audio_daily_ave.to_csv('audio_daily_ave.csv', index = False)
del audio_piece
del audio_iterator

#Calculate average daily bedtime score and average inferred sleep length
os.chdir(r'/data/final/dataset/tables/bedtime')
bedtime = pd.read_csv('/data/final/dataset/tables/bedtime/bedtime.csv', index_col = None, header = 0)
bedtime_agg = bedtime.groupby('uid').mean()
bedtime_agg.reset_index(inplace=True)
bedtime_agg = bedtime_agg.drop(['Unnamed: 0','start', 'end', 'start_hour'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
bedtime_agg.to_csv('bedtime.csv' ,index = False)

bedtime_daily = bedtime.groupby(['date', 'uid']).mean()
bedtime_daily.reset_index(inplace=True)
bedtime_daily = bedtime_daily.groupby('uid').mean()
bedtime_daily = bedtime_daily.reset_index()
bedtime_daily = bedtime_daily.drop(['Unnamed: 0','start', 'end', 'start_hour'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
bedtime_daily.to_csv('bedtime_daily_ave.csv', index = False)
#shape = (49,3)

#Calculate average conversation duration and average daily conversation duration
os.chdir(r'/data/final/dataset/tables/conversation')
conversation = pd.read_csv(r'/data/final/dataset/tables/conversation/conversation.csv', index_col = None, header = 0)
conversation_agg = conversation.groupby('uid').mean()
conversation_agg = conversation_agg.drop(['start_timestamp', ' end_timestamp'], axis = 1)
conversation_agg.reset_index(inplace = True)
os.chdir(r'/data/StudentLife/Model Tables')
conversation_agg.to_csv('convo.csv' ,index = False)
#shape =(49, 2)
conversation_daily = conversation.groupby(['date','uid']).mean()
conversation_daily.reset_index(inplace=True)
conversation_daily = conversation_daily.groupby('uid').mean()
conversation_daily = conversation_daily.reset_index()
conversation_daily = conversation_daily.drop(['start_timestamp', ' end_timestamp'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
conversation_daily.to_csv('convo_daily_ave.csv' ,index = False)

#Calculate daytime average daily conversation
day_talk = pd.read_csv('/data/final/dataset/tables/day_talk/day_talk.csv', index_col = None, header = 0)
daily_agg = day_talk.groupby('uid').mean()
daily_agg = daily_agg.drop(['start_timestamp', ' end_timestamp','start_hour'], axis = 1)
daily_agg.reset_index(inplace = True)
os.chdir(r'/data/StudentLife/Model Tables')
daily_agg.to_csv('day_convo.csv' ,index = False)

day_talk_daily = day_talk.groupby(['date','uid']).mean()
day_talk_daily.reset_index(inplace=True)
day_talk_daily = day_talk_daily.groupby('uid').mean()
day_talk_daily = day_talk_daily.reset_index()
day_talk_daily = day_talk_daily.drop(['start_timestamp', ' end_timestamp','start_hour'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
day_talk_daily.to_csv('day_ave_convo.csv' ,index = False)

#Calculate average darkness duration and average daily darkness duration
os.chdir(r'/data/final/dataset/tables/dark')
dark = pd.read_csv(r'/data/final/dataset/tables/dark/dark.csv')
dark_agg = dark.groupby('uid').mean()
dark_agg.reset_index(inplace=True)
os.chdir(r'/data/StudentLife/Model Tables')
dark_agg.to_csv('dark.csv' ,index = False)
#shape =(49, 2)
dark_daily = dark.groupby(['date','uid']).mean()
dark_daily.reset_index(inplace=True)
dark_daily = dark_daily.groupby('uid').mean()
dark_daily = dark_daily.reset_index()
os.chdir(r'/data/StudentLife/Model Tables')
dark_daily.to_csv('dark_daily_ave.csv' ,index = False)

#Calculate average phonecharge duration and average daily phonecharge duration
os.chdir(r'/data/final/dataset/tables/phonecharge/')
phonecharge = pd.read_csv(r'/data/final/dataset/tables/phonecharge/phonecharge.csv', index_col = None, header = 0)
phonecharge_agg = phonecharge.groupby("uid").mean()
phonecharge_agg.reset_index(inplace=True)
phonecharge_agg = phonecharge_agg.drop(['start', 'end'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
phonecharge_agg.to_csv('phonecharge.csv' ,index = False)
#shape =(49, 2)
phonecharge_daily = phonecharge.groupby(['date','uid']).mean()
phonecharge_daily.reset_index(inplace=True)
phonecharge_daily = phonecharge_daily.groupby('uid').mean()
phonecharge_daily = phonecharge_daily.reset_index()
phonecharge_daily = phonecharge_daily.drop(['start', 'end'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
phonecharge_daily.to_csv('phonecharge_daily_ave.csv' ,index = False)

#Calculate phonelock duration and average daily phonelock duration
os.chdir(r'/data/final/dataset/tables/phonelock')
phonelock = pd.read_csv(r'/data/final/dataset/tables/phonelock/phonelock.csv',index_col = None, header = 0)
phonelock_daily = phonelock.groupby(['date','uid']).mean()
phonelock_daily.reset_index(inplace=True)
phonelock_daily = phonelock_daily.groupby('uid').mean()
phonelock_daily = phonelock_daily.reset_index()
phonelock_daily = phonelock_daily.drop(['start', 'end'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
phonelock_daily.to_csv('phone_daily_ave.csv' ,index = False)
#shape =(49, 2)
phonelock_agg = phonelock.groupby("uid").mean()
phonelock_agg.reset_index(inplace=True)
phonelock_agg = phonelock_agg.drop(['start', 'end'], axis = 1)
os.chdir(r'/data/StudentLife/Model Tables')
phonelock_agg.to_csv('phonelock.csv' ,index = False)
#shape =(49, 2)

#Calculate average study time, number of days studied, and total time spent studying
os.chdir(r'/data/final/dataset/tables/study_events')
study_event = pd.read_csv('/data/final/dataset/tables/study_events/study_events.csv',index_col = None, header = 0)
study_day = study_event.groupby('uid').count()
study_day = study_day.drop(['date', 'event_delta', 'event_start', 'event_end'], axis = 1)
study_day.reset_index(inplace=True)
study_day.columns = ('uid', 'days_studied')
#shape = (46,2)
study_agg = study_event.groupby('uid').mean()
study_agg.reset_index(inplace = True)
study_agg.columns = ('uid', 'ave_study_time')
study_agg.head()
#shape = (46,2)
study_tot = study_event.groupby('uid').sum()
study_tot.reset_index(inplace = True)
study_tot.columns = ('uid', 'all_study_time')
#shape = (46,2)
#Merge output sense it's all the same participants
study_habits = study_tot.merge(study_agg.merge(study_day),on = 'uid')
os.chdir(r'/data/StudentLife/Model Tables')
study_habits.to_csv('study_habits.csv' ,index = False)
#shape = (46,4)

#Calculate average noise level during study sessions
os.chdir(r'/data/final/dataset/tables/study_quiteness')
study_q = pd.read_csv(r'/data/final/dataset/tables/study_quiteness/study_quiteness.csv',index_col = None, header = 0)
study_q_agg = study_q.groupby('uid').mean()
study_q_agg.reset_index(inplace=True)
study_q_agg = study_q_agg.drop(['study_event', 'timestamp'],axis = 1)
study_q_agg.columns = ('uid', 'ave_study_audio')
os.chdir(r'/data/StudentLife/Model Tables')
study_q_agg.to_csv('study_quiet.csv' ,index = False)
#shape = (43,2)

#Calculate average activity level during study sessions
os.chdir(r'/data/final/dataset/tables/study_stillness')
study_s = pd.read_csv(r'/data/final/dataset/tables/study_stillness/study_stillness.csv',index_col = None, header = 0)
study_s_agg = study_s.groupby('uid').mean()
study_s_agg.reset_index(inplace=True)
study_s_agg = study_s_agg.drop(['study_event', 'timestamp'], axis =1)
os.chdir(r'/data/StudentLife/Model Tables')
study_s_agg.to_csv('study_still.csv' ,index = False)
#shape = (39,2)

os.chdir(r'/data/final/dataset/tables/EMA/Activity')
actema = pd.read_csv(r'/data/final/dataset/tables/EMA/Activity/ActivityEMA.csv',index_col = None, header = 0)
os.chdir(r'/data/StudentLife/Model Tables')
actema.to_csv('actema.csv' ,index = False)
#shape = (46, 5)

#Read in EMA Survey tables and output summary tables for exploratory analysis
os.chdir(r'/data/final/dataset/tables/EMA/Exercise')
exema = pd.read_csv(r'/data/final/dataset/tables/EMA/Exercise/Exercise_EMA.csv',index_col = None, header = 0)
os.chdir(r'/data/StudentLife/Model Tables')
exema.to_csv('exema.csv' ,index = False)
#shape (31, 5)
os.chdir(r'/data/final/dataset/tables/EMA/Mood')
moodema = pd.read_csv(r'/data/final/dataset/tables/EMA/Mood/Mood_EMA.csv',index_col = None, header = 0)
os.chdir(r'/data/StudentLife/Model Tables')
moodema.to_csv('moodema.csv' ,index = False)
#shape = (38,5)
os.chdir(r'/data/final/dataset/tables/EMA/Sleep')
sleepema = pd.read_csv(r'/data/final/dataset/tables/EMA/Sleep/SleepEMA.csv',index_col = None, header = 0)
os.chdir(r'/data/StudentLife/Model Tables')
sleepema.to_csv('sleepema.csv' ,index = False)
#shape = (47, 4)

os.chdir(r'/data/final/dataset/tables/EMA/Social')
socialema = pd.read_csv(r'/data/final/dataset/tables/EMA/Social/social_ema_response.csv',index_col = None, header = 0)
os.chdir(r'/data/StudentLife/Model Tables')
socialema.to_csv('socialema.csv' ,index = False)
#shape = (48, 2)
os.chdir(r'/data/final/dataset/tables/EMA/Stress')
stressema = pd.read_csv(r'/data/final/dataset/tables/EMA/Stress/Stress_EMA.csv',index_col = None, header = 0)
#shape = (48, 2)
os.chdir(r'/data/StudentLife/Model Tables')
stressema.to_csv('stressema.csv' ,index = False)
#Merge the tables to see total participants.
model_vars = activity_agg.merge(audio_agg, on = 'uid').merge(
    activity_daily, on = 'uid').merge(
    audio_agg, on = 'uid').merge(
    audio_daily, on = 'uid').merge(
    bedtime_agg, on = 'uid').merge(
    bedtime_daily, on = 'uid').merge(
    conversation_agg, on = 'uid').merge(
    conversation_daily, on = 'uid').merge(
    dark_agg, on = 'uid').merge(
    dark_daily, on = 'uid').merge(
    phonecharge_agg, on = 'uid').merge(
    phonecharge_daily, on = 'uid').merge(
    phonelock_daily, on = 'uid').merge(
    phonelock_agg, on = 'uid').merge(
    study_habits, on = 'uid').merge(
    study_q_agg, on = 'uid').merge(
    study_s_agg, on = 'uid').merge(
    actema, on = 'uid').merge(
    sleepema, on = 'uid').merge(
    socialema, on = 'uid').merge(
    stressema, on = 'uid')
#shape = (37,28)
model_vars.to_csv('/data/final/dataset/tables/Model_Tables/model_vars.csv', index = False)

