import numpy as np
import pandas as pd
import gc, glob, os, datetime, dateutil

#Loading survey response data
os.chdir('/data/final/dataset/EMA/response/Activity')
filelist = []
for files in glob.glob("*.json"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)
dflist = []
for x in filelist:
    df = pd.read_json(x, date_unit = 's')
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['id'] = idlist[x]
activity_ema = pd.concat(dflist)

#filter by date
activity_ema = activity_ema[activity_ema['resp_time'] > dateutil.parser.parse("2013-04-08")]
#filter by not null
activity_ema = activity_ema[pd.notnull(activity_ema['id'])]
activity_ema = activity_ema[pd.notnull(activity_ema['working'])]
activity_ema = activity_ema[pd.notnull(activity_ema['other_working'])]
activity_ema = activity_ema[pd.notnull(activity_ema['other_relaxing'])]
activity_ema = activity_ema[pd.notnull(activity_ema['relaxing'])]
#Summarize data to get mean survey response values
activity_ema_summary = activity_ema.groupby('id').mean()
activity_ema_summary.reset_index(inplace = True)
activity_ema_summary = activity_ema_summary.drop(['Social2', 'null'],1)
activity_ema_summary.columns = ('uid', 'relaxing_with_others', 'working_with_others', 'relaxing_alone', 'working_alone')
activity_ema_summary.to_csv('/data/final/dataset/tables/EMA/Activity/ActivityEMA.csv', index = False)

#Reading in exercise response data
os.chdir('/data/final/dataset/EMA/response/Exercise')
filelist = []
for files in glob.glob("*.json"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)

dflist = []
for x in filelist:
    df = pd.read_json(x)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['id'] = idlist[x]
exercise_ema = pd.concat(dflist)
exercise_intent = exercise_ema[['id', 'schedule']]
#Convert strings to numberic
exercise_intent = exercise_intent.loc[exercise_intent['schedule'].isin([1, '1'])]
exercise_intent = exercise_intent[pd.notnull(exercise_intent['schedule'])]
exercise_intent['schedule'] = exercise_intent['schedule'].astype(int)
#Calculate days where respondants said they planned to work out and skipped
exercise_intent_summary = exercise_intent.groupby('id').sum()
exercise_intent_summary.reset_index(inplace = True)
exercise_intent_summary.columns = ('uid', 'days_skipped_exercise')
#Calculate days exercised
exercise_days = exercise_ema[['id', 'have']]
exercise_days = exercise_days.loc[exercise_days['have'].isin([1, '1'])]
exercise_days = exercise_days[pd.notnull(exercise_days['have'])]
exercise_days['have'] = exercise_days['have'].astype(int)
exercise_days_summary = exercise_days.groupby('id').sum()
exercise_days_summary.reset_index(inplace = True)
exercise_days_summary.columns = ('uid', 'days_exercise')
#Survey responses for walking and exercising
exercise_summary = exercise_ema[['id','exercise', 'walk']]
exercise_summary = exercise_summary.groupby('id').mean()
exercise_summary.reset_index(inplace = True)
exercise_summary.columns = ('uid', 'time_exercise_factor', 'time_walking_factor')
#Merge the data
exercise_summary = exercise_summary.merge(exercise_days_summary, on = 'uid').merge(
exercise_intent_summary, on = 'uid')
exercise_summary.to_csv('/data/final/dataset/tables/EMA/Exercise/Exercise_EMA.csv', index = False)

#Survey responses for mood
os.chdir('/data/final/dataset/EMA/response/Mood')
filelist = []
for files in glob.glob("*.json"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)

dflist = []
for x in filelist:
    df = pd.read_json(x)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['id'] = idlist[x]
mood_ema = pd.concat(dflist)
#Re factoring and replacing nulls
mood_ema['happyornot'] = mood_ema['happyornot'].replace('null', np.nan)
mood_ema['sadornot'] = mood_ema['sadornot'].replace('null', np.nan)
mood_ema['happyornot'] = mood_ema['happyornot'].replace(2, 0)
mood_ema['happyornot'] = mood_ema['happyornot'].replace('2', 0)
mood_ema['happyornot'] = mood_ema['happyornot'].replace('1', 1)
#np.unique(mood_ema['happyornot'].values.ravel())
mood_ema['sadornot'] = mood_ema['sadornot'].replace('1', 1)
mood_ema['sadornot'] = mood_ema['sadornot'].replace('2', 0)
mood_ema['sadornot'] = mood_ema['sadornot'].replace(2, 0)
mood_response = mood_ema[['id', 'happyornot', 'sadornot']]
mood_response = mood_response.groupby('id').sum()
mood_response.reset_index(inplace = True)
mood_response.columns = ('uid', 'happy_responses', 'sad_responses')
mood_level = mood_ema[['id', 'happy', 'sad']]
mood_level = mood_level.groupby('id').mean()
mood_level.reset_index(inplace = True)
mood_level.columns = ('uid','happiness_level', 'sadness_level')
mood_ema_resp = mood_response.merge(mood_level, on = 'uid')
mood_ema_resp.to_csv('/data/final/dataset/tables/EMA/Mood/Mood_EMA.csv', index = False)

#Sleep responses
os.chdir('/data/final/dataset/EMA/response/Sleep')
filelist = []
for files in glob.glob("*.json"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)
dflist = []
for x in filelist:
    df = pd.read_json(x)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['id'] = idlist[x]
sleep_ema = pd.concat(dflist)
#Refactoring
sleep_ema = sleep_ema[sleep_ema['resp_time'] > dateutil.parser.parse("2013-04-02")]
def rev_factor(c):
    if c['rate'] == 1:
        return 10
    elif c['rate'] == 2:
        return 9
    elif c['rate'] == 3:
        return 8
    elif c['rate'] == 4:
        return 7
def rescale(c):
    if c['rate'] == 10:
        return 4
    elif c['rate'] == 9:
        return 3
    elif c['rate'] == 8:
        return 2
    elif c['rate'] == 7:
        return 1
#filtering nulls
sleep_ema = sleep_ema[pd.notnull(sleep_ema['id'])]
sleep_ema = sleep_ema[pd.notnull(sleep_ema['hour'])]
sleep_ema = sleep_ema[pd.notnull(sleep_ema['rate'])]
sleep_ema = sleep_ema[pd.notnull(sleep_ema['social'])]
sleep_ema['rate'] = sleep_ema.apply(rev_factor, axis = 1)
sleep_ema['rate'] = sleep_ema.apply(rescale, axis = 1)
sleep_ema_summary = sleep_ema.groupby('id').mean()
sleep_ema_summary.reset_index(inplace = True)
sleep_ema_summary.columns = ('uid', 'sleep_hours_factor', 'sleep_quality', 'sleep_rested')
sleep_ema_summary.to_csv('/data/final/dataset/tables/EMA/Sleep/SleepEMA.csv', index = False)

#Socializing responses
os.chdir('/data/final/dataset/EMA/response/Social')
filelist = []
for files in glob.glob("*.json"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)

dflist = []
for x in filelist:
    df = pd.read_json(x)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['id'] = idlist[x]
social_ema = pd.concat(dflist)
#Filter date
social_ema = social_ema[social_ema['resp_time'] > dateutil.parser.parse("2013-03-25")]
social_interactions_summary = social_ema[['id', 'number']]
social_interactions_summary = social_interactions_summary.groupby('id').mean()
social_interactions_summary.reset_index(inplace = True)
social_interactions_summary.columns = ('uid', 'people_interacted_with')
social_interactions_summary.to_csv('/data/final/dataset/tables/EMA/Social/social_ema_response.csv', index = False)

#Stress responses
os.chdir('/data/final/dataset/EMA/response/Stress')
filelist = []
for files in glob.glob("*.json"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)

dflist = []
for x in filelist:
    df = pd.read_json(x)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['id'] = idlist[x]
stress_ema = pd.concat(dflist)
#Filter date
stress_ema = stress_ema[stress_ema['resp_time'] > dateutil.parser.parse("2013-03-26")]
#Relevel factors
def stress_relevel(c):
    if c['level'] == 1:
        return 12
    elif c['level'] == 2:
        return 11
    elif c['level'] == 3:
        return 10
    elif c['level'] == 4:
        return 13
    elif c['level'] == 5:
        return 14
def stress_renumber(c):
    if c['level'] == 10:
        return 1
    elif c['level'] == 11:
        return 2
    elif c['level'] == 12:
        return 3
    elif c['level'] == 13:
        return 4
    elif c['level'] == 14:
        return 5
stress_summary = stress_ema[['id','level']]
stress_summary = stress_summary[pd.notnull(stress_summary['level'])]
stress_summary['level'] = stress_summary.apply(stress_relevel, axis =1)
stress_summary['level'] = stress_summary.apply(stress_renumber, axis =1)
stress_summary = stress_summary.groupby('id').mean()
stress_summary.reset_index(inplace = True)
stress_summary.columns = ('uid', 'stress_level')
stress_summary.to_csv('/data/final/dataset/tables/EMA/Stress/Stress_EMA.csv', index = False)

#Depression responses scores
phq = pd.read_csv('/data/final/dataset/survey/PHQ-9.csv', index_col = False)
phq.columns = ('uid','type','interest', 'depression', 'sleep','energy', 'appetite', 'self_image', 'concentration', 'manic_depressive', 'suicidal', 'response_difficulty')
#Turning factors in to scores
def factor_to_score_interest(c):
    if c['interest'] == 'Not at all':
        return 0
    elif c['interest'] == 'Several days':
        return 1
    elif c['interest'] == 'More than half the days':
        return 2
    elif c['interest'] == 'Nearly every day':
        return 3
def factor_to_score_depression(c):
    if c['depression'] == 'Not at all':
        return 0
    elif c['depression'] == 'Several days':
        return 1
    elif c['depression'] == 'More than half the days':
        return 2
    elif c['depression'] == 'Nearly every day':
        return 3
def factor_to_score_sleep(c):
    if c['sleep'] == 'Not at all':
        return 0
    elif c['sleep'] == 'Several days':
        return 1
    elif c['sleep'] == 'More than half the days':
        return 2
    elif c['sleep'] == 'Nearly every day':
        return 3
def factor_to_score_energy(c):
    if c['energy'] == 'Not at all':
        return 0
    elif c['energy'] == 'Several days':
        return 1
    elif c['energy'] == 'More than half the days':
        return 2
    elif c['energy'] == 'Nearly every day':
        return 3
def factor_to_score_appetite(c):
    if c['appetite'] == 'Not at all':
        return 0
    elif c['appetite'] == 'Several days':
        return 1
    elif c['appetite'] == 'More than half the days':
        return 2
    elif c['appetite'] == 'Nearly every day':
        return 3
def factor_to_score_self_image(c):
    if c['self_image'] == 'Not at all':
        return 0
    elif c['self_image'] == 'Several days':
        return 1
    elif c['self_image'] == 'More than half the days':
        return 2
    elif c['self_image'] == 'Nearly every day':
        return 3
def factor_to_score_concentration(c):
    if c['concentration'] == 'Not at all':
        return 0
    elif c['concentration'] == 'Several days':
        return 1
    elif c['concentration'] == 'More than half the days':
        return 2
    elif c['concentration'] == 'Nearly every day':
        return 3
def factor_to_score_manic_depressive(c):
    if c['manic_depressive'] == 'Not at all':
        return 0
    elif c['manic_depressive'] == 'Several days':
        return 1
    elif c['manic_depressive'] == 'More than half the days':
        return 2
    elif c['manic_depressive'] == 'Nearly every day':
        return 3
def factor_to_score_suicidal(c):
    if c['suicidal'] == 'Not at all':
        return 0
    elif c['suicidal'] == 'Several days':
        return 1
    elif c['suicidal'] == 'More than half the days':
        return 2
    elif c['suicidal'] == 'Nearly every day':
        return 3
#Calculate overall score
phq['interest_score'] = phq.apply(factor_to_score_interest, axis = 1)
phq['depression_score'] = phq.apply(factor_to_score_depression, axis = 1)
phq['sleep_score'] = phq.apply(factor_to_score_sleep, axis = 1)
phq['energy_score'] = phq.apply(factor_to_score_energy, axis = 1)
phq['appetite_score'] = phq.apply(factor_to_score_appetite, axis = 1)
phq['self_image_score'] = phq.apply(factor_to_score_self_image, axis = 1)
phq['concentration_score'] = phq.apply(factor_to_score_concentration, axis = 1)
phq['manic_depressive_score'] = phq.apply(factor_to_score_manic_depressive, axis = 1)
phq['suicidal_score'] = phq.apply(factor_to_score_suicidal, axis = 1)
phq.drop(phq.columns[[2,3,4,5,6,7,8,9,10,11]], axis = 1, inplace = True)
phq['total'] = phq.sum(axis = 1)
phq.drop(phq.columns[[2,3,4,5,6,7,8,9,10]], axis = 1, inplace = True)
phq_pre = phq.loc[phq.type == 'pre']
phq_post = phq.loc[phq.type == 'post']
#Assign depression risk if score is greater than 5
def dep_risk(c):
    if c['total'] > 5:
        return 1
    else:
        return -1
phq_pre['dep_risk'] = phq_pre.apply(dep_risk, axis = 1)
phq_post['dep_risk'] = phq_post.apply(dep_risk, axis = 1)
phq_pre.to_csv('/data/final/dataset/tables/phq/pre/phq_pre.csv', index = False)
phq_post.to_csv('/data/final/dataset/tables/phq/post/phq_post.csv', index = False)

#Grades
os.chdir('/data/final/dataset/education')
grades = pd.read_csv('grades.csv', index_col = None, header = 0)
grades.columns = ('uid', 'overall_gpa', 'spring_gpa', 'class_gpa')
#Assign dean's list (over 3.59 in Spring Semester 2013)
def deans_list(c):
    if c['spring_gpa'] >=  3.59:
        return 1
    else:
        return -1
grades['deans_list'] = grades.apply(deans_list, axis = 1)
grades.to_csv('/data/final/dataset/tables/grades/grades.csv', index = False)

