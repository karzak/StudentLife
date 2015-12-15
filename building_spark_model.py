from __future__ import division
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from sklearn.linear_model import LinearRegression
from sklearn.cross_validation import train_test_split
import pandas as pd
import numpy as np
import statsmodels.api as sm

#Note, you must first setup spark15 as your default spark
#if spark is installed at /data/spark15
#export SPARK=/data/spark15
#export SPARK_HOME=$SPARK
#export PATH=$SPARK/bin:$PATH
#If spark is not configured to use the same python as numpy, pandas, ect edit the spark-env.sh file in the conf director
#add the line 'export PYSPARK_PYTHON=/path/to/proper/python/installation
#Finally, you must load pypark with the spark-csv package
#pyspark  --packages com.databricks:spark-csv_2.10:1.3.0

conf = (SparkConf().setMaster("local").setAppName("Building Spark Models"))
sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)


#Building the model for depression risk

#loading tables
moodema = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema = 'true').load('/user/w205/tables/EMA/mood/mood_ema.csv')
moodema = moodema.toPandas()
moodema = moodema[['uid','sadness_level']]


study_event = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema = 'true').load('/user/w205/tables/study_events/study_events.csv')
study_event = study_event.toPandas()
study_agg = study_event.groupby('uid').mean()
study_agg.reset_index(inplace = True)
study_agg.columns = ('uid', 'ave_study_time')

sleepema = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema = 'true').load('/user/w205/tables/EMA/sleep/sleep_ema.csv')
sleepema = sleepema.toPandas()
sleepema = sleepema[['uid', 'sleep_hours_factor']]

exema = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema = 'true').load('/user/w205/tables/EMA/exercise/exercise_ema.csv')
exema = exema.toPandas()
exema = exema[['uid', 'days_skipped_exercise']]

phq_pre = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema = 'true').load('/user/w205/tables/phq/pre/phq_pre.csv')
phq_pre = phq_pre.toPandas()


#mean imputing model_vars
#find ids which are missing for each variable
mood_id = pd.unique(moodema['uid']).tolist()
study_id = pd.unique(study_agg['uid']).tolist()
sleep_id = pd.unique(sleepema['uid']).tolist()
ex_id = pd.unique(exema['uid']).tolist()

all_ids = np.unique(mood_id + study_id + sleep_id + ex_id)

#mean imputing for exercise_ema
ex_missing = [x for x in all_ids if x not in ex_id]
val = exema['days_skipped_exercise'].mean()
vals = np.repeat(val, len(ex_missing), axis = 0).tolist()
ix = np.arange(len(exema['uid'])+1, len(exema['uid'])+len(vals)+1)
exercise_adds = pd.DataFrame({'uid': ex_missing,'days_skipped_exercise': vals}, index = ix)

#mean imputing for mood_ema
mood_missing = [x for x in all_ids if x not in mood_id]
val1 = moodema['sadness_level'].mean()
vals1 = np.repeat(val1, len(mood_missing), axis = 0).tolist()
ix1 = np.arange(len(moodema['uid'])+1, len(moodema['uid'])+len(vals1)+1)
mood_adds = pd.DataFrame({'uid': mood_missing,'sadness_level': vals1}, index = ix1)

#mean imputing for mood_ema
sleep_missing = [x for x in all_ids if x not in sleep_id]
val2 = sleepema['sleep_hours_factor'].mean()
vals2 = np.repeat(val2, len(sleep_missing), axis = 0).tolist()
ix2 = np.arange(len(sleepema['uid'])+1, len(sleepema['uid'])+len(vals2)+1)
sleep_adds = pd.DataFrame({'uid': sleep_missing, 'sleep_hours_factor': vals2}, index = ix2)

#mean imputing for study sensor data
study_missing = [x for x in all_ids if x not in study_id]
val3 = study_agg['ave_study_time'].mean()
vals3 = np.repeat(val3, len(study_missing), axis= 0).tolist()
ix3 = np.arange(len(study_agg['uid']) +1, len(study_agg['uid']) + len(vals3) +1)
study_adds = pd.DataFrame({'uid': study_missing, 'ave_study_time': vals3}, index = ix3)

#remove participants with too much missing data
all_missing_ids = ex_missing + mood_missing + sleep_missing + study_missing
repeaters = []
for x in np.unique(all_missing_ids).tolist():
    if all_missing_ids.count(x) > 2:
        repeaters.append(x)

#Create mean imputed data frames of equal size
mood_full = pd.concat([moodema, mood_adds])
study_full = pd.concat([study_agg, study_adds])
sleep_full = pd.concat([sleepema, sleep_adds])
ex_full  = pd.concat([exema, exercise_adds])

#Merge into one data frame
mvars = mood_full.merge(study_full, on='uid').merge(
    sleep_full, on='uid').merge(
    ex_full, on='uid').merge(
    phq_pre, on='uid')
mvars = mvars[~mvars.uid.isin(repeaters)]

#create OLS regression model
X2 = mvars[['sadness_level', 'ave_study_time', 'sleep_hours_factor', 'days_skipped_exercise']]
y = mvars[['dep_risk']]
X2 = sm.add_constant(X2)
est2 = sm.OLS(y, X2).fit()
#sanity check to make sure model output is correct
est2.summary()
#Run simulated train/test splits of the model and calculate overall accuracy. 
nums = np.random.randint(100000, size =10000)
accuracy = []
averages = []
for i in nums:
    train = mvars.sample(frac=0.80, random_state = i)
    test = mvars.loc[~mvars.index.isin(train.index)]
    X1tn = train[['sadness_level', 'ave_study_time', 'sleep_hours_factor', 'days_skipped_exercise']].values
    X1ts = test[['sadness_level', 'ave_study_time', 'sleep_hours_factor', 'days_skipped_exercise']].values
    ytn = train.dep_risk.values
    yts = test.dep_risk.values
    DepressionModel = LinearRegression(fit_intercept = True, normalize = True)
    DepressionModel.fit(X1tn, ytn)
    DepressionModel_predictions = DepressionModel.predict(X1ts)
    pred_est = (2 * (DepressionModel_predictions > 0).astype(int)) - 1
    accuracy.append(sum(pred_est == yts)/len(yts))
    averages.append(dict(zip(test.uid.values.tolist(),DepressionModel_predictions)))
print("Average Depression Risk Model Accuracy of 10,000 simulations: {}%".format(round((sum(accuracy)/len(accuracy))*100, 2)))
#calculate average value for each participant and use that to make prediction. 
ids = mvars.uid.values.tolist()
predicted_vals = []
for i in ids:
    predicted_vals.append(sum([d[i] for d in averages if i in d])/len([d[i] for d in averages if i in d]))
mvars['predicted_dep_risk'] = [((2 * int(i > 0)) -1) for i in predicted_vals]
#Output model table
mvars.to_csv('/data/final/dataset/tables/model_output/Depression_Risk_Model_Output.csv', index = False)

#Dean's list model 
#Loading tables
sleepema = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema = 'true').load('/user/w205/tables/EMA/sleep/sleep_ema.csv')
sleepema = sleepema.toPandas()
sleepema = sleepema[['uid', 'sleep_hours_factor']]

grades = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema = 'true').load('/user/w205/tables/grades/grades.csv')
grades = grades.toPandas()
grades = grades[['uid','deans_list']]
#Read in hdfs table
activity = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema = 'true').load('/user/w205/tables/activity/activity.csv')
#Group hdfs table (make it smaller) and output to Pandas df
activity_daily = activity.groupBy(['date', 'uid']).mean()
activity_daily = activity_daily.toPandas()
activity_daily = activity_daily.groupby('uid').mean()
activity_daily.reset_index(inplace=True)
activity_daily.columns = ('uid', 'timestamp', 'daily_act_inf')
activity_daily = activity_daily[['uid', 'daily_act_inf']]

#mean imputing
act_id = pd.unique(activity_daily.uid).tolist()
sleep_id = pd.unique(sleepema.uid).tolist()
all_ids = pd.unique(act_id + sleep_id).tolist()

sleep_missing = [x for x  in all_ids if x not in sleep_id]
val2 = sleepema['sleep_hours_factor'].mean()
vals2 = np.repeat(val2, len(sleep_missing), axis = 0).tolist()
ix2 = np.arange(len(sleepema['uid'])+1, len(sleepema['uid'])+len(vals2)+1)
sleep_adds = pd.DataFrame({'uid': sleep_missing, 'sleep_hours_factor': vals2}, index = ix2)
#add values
sleep_full = pd.concat([sleepema, sleep_adds])
dvars = sleep_full.merge(activity_daily, on='uid').merge(
    grades, on='uid')

#make the model
X0 = dvars[['sleep_hours_factor', 'daily_act_inf']]
y = dvars[['deans_list']]
X0 = sm.add_constant(X0)
est0 = sm.OLS(y, X0).fit()
#sanity check to make sure model output is correct
est0.summary()

#Run similuations of train test split and check accuracy. 
nums = np.random.randint(100000, size =10000)
accuracy = []
averages = []
for i in nums:
    train = dvars.sample(frac=0.80, random_state = i)
    test = dvars.loc[~dvars.index.isin(train.index)]
    X1tn = train[['sleep_hours_factor', 'daily_act_inf']].values
    X1ts = test[['sleep_hours_factor', 'daily_act_inf']].values
    ytn = train.deans_list.values
    yts = test.deans_list.values
    GradesModel = LinearRegression(fit_intercept = True, normalize = True)
    GradesModel.fit(X1tn, ytn)
    GradesModel_predictions = GradesModel.predict(X1ts)
    pred_est = (2 * (GradesModel_predictions > 0).astype(int)) - 1
    accuracy.append(sum(pred_est == yts)/len(yts))
    accuracy.append(sum(pred_est == yts)/len(yts))
    averages.append(dict(zip(test.uid.values.tolist(),GradesModel_predictions)))
print("Average Dean's List Placement Model Accuracy of 10,000 simulations: {}%".format(round((sum(accuracy)/len(accuracy))*100, 2)))

#Calculate each participants average value and use that to make predictions. 
ids = dvars.uid.values.tolist()
predicted_vals = []
for i in ids:
    predicted_vals.append(sum([d[i] for d in averages if i in d])/len([d[i] for d in averages if i in d]))
dvars['predicted_deans_list'] = [((2 * int(i > 0)) -1) for i in predicted_vals]
#calculate percentage of correct classifications.
sum((dvars.deans_list == dvars.predicted_deans_list).astype(int))/len(dvars.deans_list)
dvars.to_csv('/data/final/dataset/tables/model_output/Deans_List_Model_Output.csv', index = False)

