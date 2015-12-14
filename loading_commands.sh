pip install numpy
pip install pandas
pip install statsmodels
pip install scipy
pip install sklearn

cd /data
mkdir final
cd ./final
wget http://studentlife.cs.dartmouth.edu/dataset/dataset.tar.bz2
tar xvjf dataset.tar.bz2
mkdir -p /data/final/dataset/tables

cd /data/final/dataset/tables
mkdir activity
mkdir audio
mkdir bedtime
mkdir bluetooth
mkdir conversation
mkdir dark
mkdir day_talk
mkdir gps
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
mkdir Model_Tables
mkdir model_output
cd ..
mkdir scripts
cd ./scripts
wget https://raw.githubusercontent.com/karzak/StudentLife/master/processing_sensor_data.py
wget https://raw.githubusercontent.com/karzak/StudentLife/master/loading_survey_data.py
wget https://raw.githubusercontent.com/karzak/StudentLife/master/exploratory_analysis.py
wget https://raw.githubusercontent.com/karzak/StudentLife/master/loading_tables.sh
wget https://raw.githubusercontent.com/karzak/StudentLife/master/building_spark_model.py

chmod +x processing_sensor_data.py
chmod +x loading_survey_data.py
chmod +x exploratory_analysis.py
chmod +x loading_tables.sh
chmod +x building_spark_model.py

python processing_sensor_data.py
python loading_survey_data.py
python exploratory_analysis.py
./loading_tables.sh
python building_spark_model.py

