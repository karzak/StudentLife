pip install numpy
pip install pandas
cd /data
mkdir final
cd ./final
wget http://studentlife.cs.dartmouth.edu/dataset/dataset.tar.bz2
tar xvjf dataset.tar.bz2
mkdir -p /data/final/dataset/tables
mkdir -p '/data/StudentLife/Model Tables'
cd /data/final/dataset/tables
mkdir activity
mkdir audio
mkdir bedtime
mkdir bluetooth
mkdir conversation
mkdir dark
mkdir gps
mkdir model
mkdir phonecharge
mkdir phonelock
mkdir wifi
mkdir wifi_location
mkdir -p ./phq/pre
mkdir -p ./phq/post
mkdir study_events
mkdir study_stillness
mkdir study_quiteness
mkdir EMA
mkdir ./EMA/Activity
mkdir ./EMA/Exercise
mkdir ./EMA/Social
mkdir ./EMA/Sleep
mkdir ./EMA/Stress
mkdir ./EMA/Mood
mkdir grades
cd ..
mkdir scripts
cd /data
