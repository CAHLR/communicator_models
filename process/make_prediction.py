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
end_date=datetime.datetime.strptime(arges.end_date, '%Y-%m-%d')

# get the data directory
home_dir=path.dirname(os.getcwd())
data_dir=home_dir+'/data'
predictions_file=data_dir+'/predictions.csv'
config_file=home_dir+'/config.ini'

config = ConfigParser.ConfigParser()
config.read(config_file)

#get the event dataframe from the pre-processing function
event_df=pre_process(course, start_date,end_date)

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
