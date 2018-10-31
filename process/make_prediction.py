import utils
from collections import defaultdict
import numpy as np
import pandas as pd
import sys
import json
import argparse
import configparser
from keras.utils import np_utils
from keras.preprocessing import sequence

#get the course name and start date of the course from command line
parser = argparse.ArgumentParser()
parser.add_argument("course_name", help="input the course you want to make prediction")
parser.add_argument("start_date", help="input the start date of the course in this format: YYYY-MM-DD")
parser.add_argument("end_date", help="input the end date of the course in this format: YYYY-MM-DD")
args = parser.parse_args()
course=arges.course_name
start_date=datetime.datetime.strptime(arges.start_date, '%Y-%m-%d')
start_date=datetime.datetime.strptime(arges.start_date, '%Y-%m-%d')

# get the data directory
home_dir=path.dirname(os.getcwd())
data_dir=home_dir+'/data'
log_file=data_dir+ '/'+course+'-event.log'
predictions_file=data_dir+'/predictions.csv'
cut_off_file=data_dir+ 'cut_off.json'
config_file=home_dir+'/config.ini'

config = ConfigParser.ConfigParser()
config.read(config_file)

# Generate dictionary of student to list of his/her actions sorted chronologically
student_sorted=utils.generate_ordered_event_copy(log_file)
# Generate dictionary of course event to integer encoding
ce_types = utils.get_ce_types()
# get the pre_action list for events
pre_action = config.get('Options', 'preaction')

print('data processing')

#make sure the log is processing the "real time" log
now=datetime.datetime.now()
if now<end_time:
    end_time=now
#get the cutoffs
with open(cut_off_file, 'r') as f:
    time_interval=json.load(f)  
#initiate the user and user_sequence list
users_ID=[]
users_seq=[]
#get the sequence
for user,actions in student_sorted.items():
    u_seq=[]
    temp=[]
    for line in actions:
        tt =  line['time'].split('+')[0]
        time_element = datetime.datetime.strptime(tt, '%Y-%m-%dT%H:%M:%S.%f' if '.' in tt else '%Y-%m-%dT%H:%M:%S')
        try:
            parsed_event = process_utils.parse_event(line)
        except ValueError:
            print("Unable to parse:", line)
            continue
        if parsed_event in ce_types and time_element<end_time:
            temp.append((ce_types[parsed_event]+2, time_element))
    #change the category by time intervals    
    if len(temp)==1:
        u_seq=temp
    else:
        for i in range(0,len(temp)):
            time=temp[i][1]
            event=temp[i][0]
            if (event-2) in pre_action:
                if i==0:
                    delta=datetime.timedelta(seconds=0)
                else:
                    delta=time-temp[i-1][1]
            else:
                if event==4:
                    delta=datetime.timedelta(seconds=0)
                else:
                    if i==(len(temp)-1):
                        delta=end_time-time
                    else:
                        delta=temp[i+1][1]-time

            if str(event-2) not in cutoff.keys() or delta<=datetime.timedelta(seconds=cutoff[str(event-2)][0]):
                u_seq.append((event,time))
            elif delta<=datetime.timedelta(seconds=cutoff[str(event-2)][1]):
                u_seq.append((event+77,time))
            elif delta<=datetime.timedelta(minutes=30):
                u_seq.append((event+2*77,time))
            else:
                u_seq.append((event+3*77,time))
    #add the time markers
    weekend=start_time
    # add weekends
    while(1):
        if weekend>end_time:
            break
        u_seq.append((0, weekend))
        weekend = weekend+ datetime.timedelta(days=7)
    # add days
    dayend=start_time
    while(1):
        dayend = dayend+ datetime.timedelta(days=1)
        if dayend>end_time:
            break
        u_seq.append((1, dayend))
    # sort again
    s = sorted(u_seq, key=lambda p: p[1])
    seq= [item[0] for item in s]
    #generate the list
    users_seq.append(seq)
    users_ID.append(user)
event_df = pd.DataFrame({'userID': users_ID, 'seq':users_seq})

print('making predictions')

# generating the train matrix
max_seq_len = int(config.get('Options', 'max_seq_len'))
max_input_dim = int(config.get('Options', 'max_input_dim'))
batch_size = int(config.get('Options', 'batch_size'))
event_df.reset_index(drop=True, inplace=True)
event_df.reindex(np.random.permutation(event_df.index))
event_list = event_df['seq'].values
event_list_binary = [np_utils.to_categorical(x, max_input_dim) for x in event_list]
x_train = sequence.pad_sequences(event_list_binary, maxlen=max_seq_len, dtype='bool',
                                     padding='post', truncating='post')
# Load model weights and get predictions
model = utils.load_keras_weights_from_disk()
y_pred = model.predict(x_test, batch_size=batch_size, verbose=1)
y_pred = y_pred[:, -1, :]
y_pred_pass = y_pred[event_df, 0]
y_pred_comp = y_pred[event_df, 1]
y_pred_stop = y_pred[event_df, 2]
result_df = pd.DataFrame({'userID': users_ID, 'y_pass':y_pred_pass,'y_comp':y_pred_comp,'y_stop':y_pred_stop})

print("saving the prediction in data directory")

#generate the prediciton file
header = ["User_id", "pass_prediction", "completion_prediction", "stopout_prediction"]
result.to_csv(predictions_file, index=False, columns = header)
