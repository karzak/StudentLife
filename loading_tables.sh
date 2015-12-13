hdfs dfs -mkdir /user/w205/tables
hdfs dfs -mkdir /user/w205/tables/activity
hdfs dfs -mkdir /user/w205/tables/audio
hdfs dfs -mkdir /user/w205/tables/bluetooth
hdfs dfs -mkdir /user/w205/tables/conversation
hdfs dfs -mkdir /user/w205/tables/dark
hdfs dfs -mkdir /user/w205/tables/gps
hdfs dfs -mkdir /user/w205/tables/grades
hdfs dfs -mkdir /user/w205/tables/phonecharge
hdfs dfs -mkdir /user/w205/tables/phonelock
hdfs dfs -mkdir /user/w205/tables/study_events
hdfs dfs -mkdir /user/w205/tables/study_quiteness
hdfs dfs -mkdir /user/w205/tables/study_stillness
hdfs dfs -mkdir /user/w205/tables/wifi
hdfs dfs -mkdir /user/w205/tables/wifi_location

hdfs dfs -mkdir /user/w205/tables/EMA
hdfs dfs -mkdir /user/w205/tables/EMA/activity
hdfs dfs -mkdir /user/w205/tables/EMA/exercise
hdfs dfs -mkdir /user/w205/tables/EMA/mood
hdfs dfs -mkdir /user/w205/tables/EMA/sleep
hdfs dfs -mkdir /user/w205/tables/EMA/social
hdfs dfs -mkdir /user/w205/tables/EMA/stress

hdfs dfs -mkdir /user/w205/tables/phq
hdfs dfs -mkdir /user/w205/tables/phq/pre
hdfs dfs -mkdir /user/w205/tables/phq/post

hdfs dfs -copyFromLocal '/data/final/dataset/tables/activity/activity.csv' '/user/w205/tables/activity/activity.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/audio/audio.csv' '/user/w205/tables/audio/audio.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/bluetooth/bluetooth.csv' '/user/w205/tables/bluetooth/bluetooth.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/conversation/conversation.csv' '/user/w205/tables/conversation/conversation.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/dark/dark.csv' '/user/w205/tables/dark/dark.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/gps/gps.csv' '/user/w205/tables/gps/gps.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/grades/grades.csv' '/user/w205/tables/grades/grades.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/phonecharge/phonecharge.csv' '/user/w205/tables/phonecharge/phonecharge.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/phonelock/phonelock.csv' '/user/w205/tables/phonelock/phonelock.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/study_events/study_events.csv' '/user/w205/tables/study_events/study_events.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/study_quiteness/study_quiteness.csv' '/user/w205/tables/study_quiteness/study_quiteness.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/study_stillness/study_stillness.csv' '/user/w205/tables/study_stillness/study_stillness.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/wifi/wifi.csv' '/user/w205/tables/wifi/wifi.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/wifi_location/wifi_location.csv' '/user/w205/tables/wifi_location/wifi_location.csv'

hdfs dfs -copyFromLocal '/data/final/dataset/tables/EMA/Activity/ActivityEMA.csv' '/user/w205/tables/EMA/activity/activity_ema.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/EMA/Exercise/Exercise_EMA.csv' '/user/w205/tables/EMA/exercise/exercise_ema.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/EMA/Mood/Mood_EMA.csv' '/user/w205/tables/EMA/mood/mood_ema.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/EMA/Sleep/SleepEMA.csv' '/user/w205/tables/EMA/sleep/sleep_ema.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/EMA/Social/social_ema_response.csv' '/user/w205/tables/EMA/social/social_ema.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/EMA/Stress/Stress_EMA.csv' '/user/w205/tables/EMA/stress/stress_ema.csv'

hdfs dfs -copyFromLocal '/data/final/dataset/tables/phq/post/phq_post.csv' '/user/w205/tables/phq/post/phq_post.csv'
hdfs dfs -copyFromLocal '/data/final/dataset/tables/phq/pre/phq_pre.csv' '/user/w205/tables/phq/pre/phq_pre.csv'
