import numpy as np
import pandas as pd
import gc, glob, os, datetime, dateutil

print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
pd.options.mode.chained_assignment = None
os.chdir('/data/final/dataset/sensing/activity')
filelist = []
for files in glob.glob("*.csv"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)
dflist = []
for x in range(len(filelist)):
    iter_csv = pd.read_csv(filelist[x], index_col=None, header = 0, iterator = True, chunksize = 1000)
    df = pd.concat([chunk[chunk.iloc[:,1] < 3] for chunk in iter_csv])
    df['uid'] = idlist[x]
    df['timestamp'] = df['timestamp'] - 14400
    df['time'] =  pd.to_datetime(df['timestamp'], unit = 's')
    df['date'] = pd.DatetimeIndex(df['time']).date
    dflist.append(df)
activity = pd.concat(dflist)
del dflist
gc.collect()
print 'saving activity'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
activity.to_csv('/data/final/dataset/tables/activity/activity.csv', index = False)
del activity
gc.collect()

os.chdir('/data/final/dataset/sensing/audio')
filelist = []
for files in glob.glob("*.csv"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)
dflist = []
for x in range(len(filelist)):
    iter_csv = pd.read_csv(filelist[x], index_col=None, header = 0, iterator = True, chunksize = 1000)
    df = pd.concat([chunk[chunk.iloc[:,1] < 3] for chunk in iter_csv])
    df['uid'] = idlist[x]
    df['timestamp'] = df['timestamp'] - 14400
    df['time'] =  pd.to_datetime(df['timestamp'], unit = 's')
    df['date'] = pd.DatetimeIndex(df['time']).date
    dflist.append(df)
audio = pd.concat(dflist)
del dflist
gc.collect()
print 'saving audio'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
audio.to_csv("/data/final/dataset/tables/audio/audio.csv", index = False)
del audio
gc.collect()

pd.options.mode.chained_assignment = None
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
os.chdir('/data/final/dataset/sensing/bluetooth')
filelist = []
#place all csv files and their corresponding ids in a list
for files in glob.glob("*.csv"):
    filelist.append(files)
    idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)
dflist = []
for x in filelist:
    df = pd.read_csv(x, index_col=None, header = 0)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['uid'] = idlist[x]
bluetooth = pd.concat(dflist)
print 'saving bluethooth'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
bluetooth.to_csv('/data/final/dataset/tables/bluetooth/bluetooth.csv', index = False)

os.chdir('/data/final/dataset/sensing/conversation')
filelist = []
#place all csv files and their corresponding ids in a list
for files in glob.glob("*.csv"):
    filelist.append(files)
    idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)

#read each csv into a pandas dataframe and add the id as a column
dflist = []
for x in filelist:
    df = pd.read_csv(x, index_col=None, header = 0)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['uid'] = idlist[x]
#merge the dataframes into the output dataframe
conversation = pd.concat(dflist)
conversation['start'] = pd.to_datetime(conversation['start_timestamp'], unit = 's')
conversation['end'] = pd.to_datetime(conversation[' end_timestamp'], unit = 's')
conversation['duration'] = (conversation['end']- conversation['start'])/np.timedelta64(1,'s')
conversation['date'] = pd.DatetimeIndex(conversation['start']).date
conversation['start_hour'] = pd.DatetimeIndex(conversation['start']).hour
print 'saving conversation'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
conversation.to_csv("/data/final/dataset/tables/conversation/conversation.csv", index =False)

os.chdir('/data/final/dataset/sensing/dark')
filelist = []
for files in glob.glob("*.csv"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)

dflist = []
for x in filelist:
    df = pd.read_csv(x, index_col=None, header = 0)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['uid'] = idlist[x]
dark = pd.concat(dflist)
dark['start'] = dark['start'] - 14400
dark['end'] = dark['end'] - 14400
dark['start_time'] = pd.to_datetime(dark['start'], unit = 's')
dark['end_time'] = pd.to_datetime(dark['end'], unit = 's')
dark['duration'] = (dark['end_time'] - dark['start_time'])/np.timedelta64(1,'m')
dark['start_hour'] = pd.DatetimeIndex(dark['start_time']).hour
dark['date'] = pd.DatetimeIndex(dark['start_time']).date
print 'saving dark'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
dark.to_csv("/data/final/dataset/tables/dark/dark.csv", index = False)

day_talk = conversation.loc[((conversation.start_hour >= 9) | (conversation.start_hour < 18))]
day_talk.to_csv('/data/final/dataset/tables/day_talk/day_talk.csv', index = False)

bedtime = dark.loc[((dark.start_hour >= 20) | (dark.start_hour < 6)) & (dark.duration >= 180.0)]
def early_to_bed(c):
    if c['start_hour'] == 20:
        return 1
    elif c['start_hour'] == 21:
        return 2
    elif c['start_hour'] == 22:
        return 3
    elif c['start_hour'] == 23:
        return 4
    elif c['start_hour'] == 24:
        return 5
    elif c['start_hour'] == 0:
        return 6
    elif c['start_hour'] == 1:
        return 7
    elif c['start_hour'] == 2:
        return 8
    elif c['start_hour'] == 3:
        return 9
    elif c['start_hour'] == 4:
        return 10
    elif c['start_hour'] == 5:
        return 11
bedtime['bedtime_early'] = bedtime.apply(early_to_bed, axis = 1)
bedtime = bedtime.rename(columns = {'duration': 'night_duration'})
bedtime.to_csv('/data/final/dataset/tables/bedtime/bedtime.csv')

os.chdir('/data/final/dataset/sensing/gps')
filelist = []
for files in glob.glob("*.csv"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)
dflist = []
for x in filelist:
    df = pd.read_csv(x, index_col=None, header = 0)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['id'] = idlist[x]
gps = pd.concat(dflist)
gps.reset_index(inplace = True)
gps.columns = ('timestamp', 'provider', 'network_type', 'accuracy', 'lat',
'lon', 'altitude', 'bearing' ,'speed', 'travelstate', 'null', 'uid')
gps = gps.drop("null", 1)
print 'saving gps'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
gps.to_csv('/data/final/dataset/tables/gps/gps.csv', index = False)
del gps
os.chdir('/data/final/dataset/sensing/phonecharge')
filelist = []
for files in glob.glob("*.csv"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)
dflist = []
for x in filelist:
    df = pd.read_csv(x, index_col=None, header = 0)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['uid'] = idlist[x]
phonecharge = pd.concat(dflist)
phonecharge['start'] = phonecharge['start'] - 14400
phonecharge['end'] = phonecharge['end'] - 14400
phonecharge['start_time'] = pd.to_datetime(phonecharge['start'], unit = 's')
phonecharge['end_time'] = pd.to_datetime(phonecharge['end'], unit = 's')
phonecharge['duration'] = (phonecharge['end_time'] - phonecharge['start_time'])/np.timedelta64(1,'s')
phonecharge['date'] = pd.DatetimeIndex(phonecharge['start_time']).date
print 'saving phonecharge'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
phonecharge.to_csv('/data/final/dataset/tables/phonecharge/phonecharge.csv', index = False)

os.chdir('/data/final/dataset/sensing/phonelock')
filelist = []
for files in glob.glob("*.csv"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)
dflist = []
for x in filelist:
    df = pd.read_csv(x, index_col=None, header = 0)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['uid'] = idlist[x]
phonelock = pd.concat(dflist)
phonelock['start'] = phonelock['start'] - 14400
phonelock['end'] = phonelock['end'] - 14400
phonelock['start_time'] = pd.to_datetime(phonelock["start"], unit = 's')
phonelock['end_time'] = pd.to_datetime(phonelock["end"], unit = 's')
phonelock['duration'] = (phonelock['end_time'] - phonelock.start_time)/np.timedelta64(1,'s')
phonelock['date'] = pd.DatetimeIndex(phonelock['start_time']).date
print phonelock.head()
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
phonelock.to_csv('/data/final/dataset/tables/phonelock/phonelock.csv', index = False)

os.chdir('/data/final/dataset/sensing/wifi')
filelist = []
for files in glob.glob("*.csv"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[1]).split(".")[0]
    idlist.append(x)
dflist = []
for x in range(len(filelist)):
    iter_csv = pd.read_csv(filelist[x], index_col=None, header = 0, iterator = True, chunksize = 1000)
    df = pd.concat([chunk for chunk in iter_csv])
    df['uid'] = idlist[x]
    dflist.append(df)
wifi = pd.concat(dflist)
print 'saving wifi'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
wifi.to_csv('/data/final/dataset/tables/wifi/wifi.csv', index = False)
del wifi

os.chdir('/data/final/dataset/sensing/wifi_location')
filelist = []
for files in glob.glob("*.csv"):
    filelist.append(files)
idlist = []
for i in filelist:
    x = (i.split("_")[2]).split(".")[0]
    idlist.append(x)
dflist = []
for x in filelist:
    df = pd.read_csv(x, index_col=None, header = 0)
    dflist.append(df)
for x in range(len(dflist)):
    dflist[x]['uid'] = idlist[x]
wifi_location = pd.concat(dflist)
wifi_location.reset_index(inplace = True)
wifi_location.columns = ("timestamp", "location", "null", "uid")
wifi_location = wifi_location.drop("null", 1)
wifi_location['timestamp'] = wifi_location['timestamp'] - 14400
wifi_location['time'] = pd.to_datetime(wifi_location['timestamp'], unit = 's')
print 'saving wifi_location'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
wifi_location.to_csv('/data/final/dataset/tables/wifi_location/wifi_location.csv', index = False)

#Obtaining study events from wifi_location
study_locs = ['in[baker-berry]', 'in[dana-library]', 'in[feldberg_library]',
'in[sanborn]', 'in[dartmouth_hall]', 'in[silsby-rocky]']
df_study = wifi_location.loc[wifi_location['location'].isin(study_locs)]
#del wifi_location
#Calculate time spent continuously in study location
df_study['delta'] = (df_study['time']-df_study['time'].shift()).fillna(0)
def study_events(c):
    global i
    if c['location'] == c['shift'] and c['delta'] < datetime.timedelta(minutes= 20):
        try:
            return i
        except:
            i = 1
            return i
    else:
        try:
            i += 1
            return i
        except:
            i = 1
            return i
df_study['shift'] = df_study['location'].shift().fillna(df_study['location'])
df_study['study_event'] = df_study.apply(study_events, axis= 1)

def event_delta(c):
    if c['delta'] < datetime.timedelta(minutes = 20):
        return c['delta']
    else:
        return datetime.timedelta(seconds = 0)
df_study['event_delta'] = df_study.apply(event_delta, axis = 1)
df_study['event_delta'] = (df_study['event_delta']/np.timedelta64(1, 'm'))
df_study['date'] = pd.DatetimeIndex(df_study['time']).date
#df_study.to_csv('/data/final/dataset/tables/study/study.csv')
dropcols = ['timestamp','time', 'delta', 'shift']
cols = [c for c in df_study.columns.tolist() if c not in dropcols]
df_study_events = df_study[cols]
df_study_events = df_study_events.groupby(['study_event', 'date', 'uid']).sum().reset_index()
df_study_events["event_start"] = np.nan
df_study_events["event_end"] = np.nan

for i in range(1,len(df_study_events['study_event'])):
    df_study_events['event_start'][i-1] = df_study.loc[df_study['study_event'] == i]['time'].min()
    df_study_events['event_end'][i-1] = df_study.loc[df_study['study_event'] == i]['time'].max()
df_study_events['event_delta'] = np.round(df_study_events['event_delta'], 0)
df_study_events = df_study_events.loc[df_study_events['event_delta'] >= 20]
print 'saving study_events'
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
df_study_events.to_csv('/data/final/dataset/tables/study_events/study_events.csv', index = False)
del df_study_events

#Obtaining average noise level during study events
df_study_events = pd.read_csv('/data/final/dataset/tables/study_events/study_events.csv',
index_col = None, header = 0)
audio_iterator = pd.read_csv('/data/final/dataset/tables/audio/audio.csv',
index_col = None, header = 0, iterator = True, chunksize = 10000)
ids = np.unique(df_study_events['uid'].values.ravel()).tolist()
print datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
audio = pd.concat([chunk for chunk in audio_iterator])
dflist = []
for i in ids:
    df = df_study_events.loc[df_study_events['uid'] == i]
    ef = audio.loc[audio['uid'] == i]
    for index, row in df.iterrows():
        ff = ef.loc[(ef['time'] >= row['event_start']) & (ef['time'] <= row['event_end'])]
        ff['study_event'] = row['study_event']
        dflist.append(ff)
del audio
gc.collect()
noise = pd.concat(dflist)
focus = noise.groupby(['study_event','uid']).mean()
focus.reset_index(inplace = True)
print np.unique(focus['uid'].values.ravel())
focus.to_csv('/data/final/dataset/tables/study_quiteness/study_quiteness.csv', index = False)

