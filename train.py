from keras.models import model_from_json, Sequential
from keras.utils import multi_gpu_model
from keras.layers.wrappers import TimeDistributed
from keras.layers.core import Masking
from keras.layers import LSTM, Dense, Activation
from keras.optimizers import Adam, SGD, RMSprop
from keras.callbacks import ModelCheckpoint, EarlyStopping
from sklearn.model_selection import train_test_split
from keras.preprocessing import sequence
import numpy as np
import tensorflow as tf
import os
import prediction_utils_new
import argparse
from keras.backend.tensorflow_backend import set_session
import setproctitle
try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser
from keras import backend as K
#

def load_data(data_dir, model_name):
    x = None
    y = None
    data_length = None
    week_labels = None
    for dirc in os.listdir(data_dir):
        if dirc.startswith('.'):
            continue
        x_data = np.load(os.path.join(data_dir, dirc, model_name, 'x0_train.npy'))
        week_labels_tmp = np.load(os.path.join(data_dir, dirc, model_name, 'week_label0.npy'))
        data_length_tmp = np.load(os.path.join(data_dir, dirc, model_name, 'ori_length0.npy'))
        if 'pass' in model_name:
            y_data = np.load(os.path.join(data_dir, dirc, model_name, 'y0_pass.npy'))
        elif 'stop' in model_name:
            y_data = np.load(os.path.join(data_dir, dirc, model_name, 'y0_stopout.npy'))
        elif 'comp' in model_name:
            y_data = np.load(os.path.join(data_dir, dirc, model_name, 'y0_comp.npy'))
        else:
            print('No such model name: %s' %model_name)
            exit()
        if x is None or y is None or data_length is None or week_labels is None:
            x = x_data
            y = y_data
            week_labels = week_labels_tmp
            data_length = data_length_tmp
        else:
            x = np.concatenate((x, x_data), axis=0)
            y = np.concatenate((y, y_data), axis=0)
            week_labels = week_labels_tmp
            data_length = data_length_tmp
    return x, y, week_labels, data_length


def main(args):
    setproctitle.setproctitle('Communicator_train_'+args.model)
    config = ConfigParser.ConfigParser()
    config.read(args.config_file)
    #os.environ["CUDA_VISIBLE_DEVICES"] = args.gpu
    model_dir = os.path.join(config.get('Paths','model_path'), args.model)
    max_seq_len = int(config.get('Options', 'max_seq_len'))
    max_input_dim = int(config.get('Options', 'max_input_dim'))
    hidden_size = int(config.get('Options', 'hidden_size'))
    batch_size = int(config.get('Options', 'batch_size'))
    drop_out = float(config.get('Options', 'drop_out'))
    epoch = int(config.get('Options', 'epoch'))
    if args.log_file is None:
        log_file = args.model + '_weight.h5'
    else:
        log_file = args.log_file
    if not os.path.exists(model_dir):
        os.mkdir(model_dir)
    train_dir = config.get('Paths','train_path')
    test_dir = config.get('Paths','test_path')

    config_tf = tf.ConfigProto(allow_soft_placement = True)
    config_tf.gpu_options.allow_growth = True
    set_session(tf.Session(config=config_tf))

    #build model
    # with open(os.path.join(model_dir,args.model+'.json'), 'r') as json_file:
    #     model = model_from_json(json_file.readline())


    model = Sequential()
    model.add(Masking(mask_value=0., input_shape=(max_seq_len, max_input_dim)))
    model.add(LSTM(hidden_size, dropout=drop_out, return_sequences=True))
    model.add(LSTM(hidden_size, dropout=drop_out, return_sequences=True))
    model.add(TimeDistributed(Dense(1)))
    model.add(Activation('sigmoid'))
    model_json = model.to_json()
    with open(os.path.join(model_dir, args.model+".json"), "w") as json_file:
        json_file.write(model_json)

    model = multi_gpu_model(model, gpus = len(args.gpu.split(',')))

    model.compile(loss='binary_crossentropy', optimizer=Adam(), metrics=['accuracy'])
    x_train, y_train, wls_train, dls_train = load_data(train_dir, args.model)
    x_test, y_test, wls_test, dls_test = load_data(test_dir, args.model)

    print('Padding for %s datasets' %args.model)
    # x_train = sequence.pad_sequences(x_train, maxlen=max_seq_len, dtype='int32',
    #                                  padding='post', truncating='post')
    # x_test = sequence.pad_sequences(x_test, maxlen=max_seq_len, dtype='int32',
    #                                  padding='post', truncating='post')
    x_train = x_train.astype(np.float16)
    x_test = x_test.astype(np.float16)
    y_test = y_test.astype(np.float16)
    y_train = y_train.astype(np.float16)
    print('x_train shape: (%s, %s, %s)' %(x_train.shape[0], x_train.shape[1], x_train.shape[2]))
    print('y_train shape: (%s, %s)' %(y_train.shape[0], y_train.shape[1]))
    print('x_test shape: (%s, %s, %s)' % (x_test.shape[0], x_test.shape[1], x_test.shape[2]))
    print('y_test shape: (%s, %s)' % (y_test.shape[0], y_test.shape[1]))
    print('wls_test length:%s' %len(wls_test))
    y_train = np.expand_dims(y_train, axis=2)
    y_test = np.expand_dims(y_test, axis=2)


    if args.train == True:
        print('Training starts.')
        checkpoint_callback = ModelCheckpoint()
        model.fit(x_train, y_train, epochs= epoch, batch_size= batch_size, shuffle=True, callbacks=[ ])
        print('Saving to %s' % os.path.join(model_dir, log_file))
        model.save(os.path.join(model_dir, log_file))
    else:
        print('Loading weights from %s' %log_file)
        model.load_weights(os.path.join(model_dir, log_file))
    #automatically
    print('Evaluatin automatically')
    #model.evaluate(x_test, y_test)

    #manually by weekends
    print('Predicting manually')
    y_test_pred = model.predict(x_test)
    week_counts = len(wls_test[0])

    f = open(os.path.join(model_dir,'result.txt'), 'w')
    f.write('week acc auc\n')
    for week in range(week_counts):
        y_pred_week = []
        y_true_week = []
        for i, wl in enumerate(wls_test):
            if len(wl) != week_counts:
                print('week_counts are not the same!')
                exit(0)
            y_pred_week.append(y_test_pred[i][wl[week]][0])
            y_true_week.append(y_test[i][wl[week]][0])
        acc = my_acc(np.array(y_true_week), np.array(y_pred_week))
        auc, pfas, ptas,  = my_auc(np.array(y_true_week), np.array(y_pred_week))
        np.save(os.path.join(model_dir, 'y_result.npy'), {'y': y_true_week, 'y_pred': y_pred_week})
        print('result in week %s: acc = %s, auc = %s' %(week, acc, auc))
        np.save('roc.npy', {'pfas':pfas, 'ptas':ptas})
        f.write('%s %s %s \n' %(week, acc, auc))

# AUC for a binary classifier
def my_auc(y_true, y_pred):
    ptas_raw = [binary_PTA(y_true,y_pred,k) for k in np.linspace(0, 1, 1000)]
    pfas_raw = [binary_PFA(y_true,y_pred,k) for k in np.linspace(0, 1, 1000)]
    pfas = np.concatenate((np.ones((1,)) ,pfas_raw),axis=0)
    binSizes = -(pfas[1:]-pfas[:-1])
    s = ptas_raw*binSizes
    return np.sum(s), pfas_raw, ptas_raw
#-----------------------------------------------------------------------------------------------------------------------------------------------------
# PFA, prob false alert for binary classifier
def binary_PFA(y_true, y_pred, threshold=0.5):
    y_pred = (y_pred >= threshold).astype(np.float32)
    # N = total number of negative labels
    N = np.sum((1-y_true).astype(np.float32))
    # FP = total number of false alerts, alerts from the negative class labels
    FP = np.sum(y_pred - y_pred * y_true)
    return FP/N
#-----------------------------------------------------------------------------------------------------------------------------------------------------
# P_TA prob true alerts for binary classifier
def binary_PTA(y_true, y_pred, threshold=K.variable(value=0.5)):
    y_pred = (y_pred >= threshold).astype(np.float32)
    # P = total number of positive labels
    P = np.sum(y_true.astype(np.float32))
    # TP = total number of correct alerts, alerts from the positive class labels
    TP = np.sum(y_pred * y_true)
    return TP/P

def my_acc(y_true, y_pred, threshold = 0.5):
    y_pred = [pred > threshold for pred in y_pred]
    return 1 - np.sum(np.abs(y_pred-y_true))/len(y_true)



def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', type=str, default='pass')
    parser.add_argument('--gpu', type = str, default= '0,1,2,3')
    parser.add_argument('--train', type = str2bool, default= True, help='Whether to train a new model or to load a old one')
    parser.add_argument('--log_file', type = str, default = None, help = 'The name for log file')
    parser.add_argument('--config_file', type = str, default= './config.ini', help = 'Dir for config file')
    args = parser.parse_args()
    main(args)