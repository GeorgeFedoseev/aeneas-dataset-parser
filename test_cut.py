#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os
import pandas
import sys

import re

import subprocess


import csv_utils

from multiprocessing.pool import ThreadPool

reload(sys)
sys.setdefaultencoding('utf8')

DATASET_NAME = 'echo-msk'

curr_dir_path = os.path.dirname(os.path.realpath(__file__))
dataset_folder_path = os.path.join(curr_dir_path, DATASET_NAME+"-dataset/")

if not os.path.exists(dataset_folder_path):
    os.makedirs(dataset_folder_path)


def convert_to_wav(in_audio_path, out_audio_path):
    print 'converting to big wav'
    p = subprocess.Popen(["ffmpeg", "-y",
         "-i", in_audio_path,         
         "-ac", "1",
         "-ab", "16",
         "-ar", "16000",         
         out_audio_path
         ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out, err = p.communicate()

    if p.returncode != 0:
        print("failed_ffmpeg_conversion "+str(err))
        return False
    return True

def cut_audio_piece_to_wav(in_audio_path, out_audio_path, start_sec, end_sec):
    p = subprocess.Popen(["ffmpeg", "-y",
         "-i", in_audio_path,
         "-ss", str(start_sec),
         "-to", str(end_sec),
         "-ac", "1",
         "-ab", "16",
         "-ar", "16000",         
         out_audio_path
         ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    out, err = p.communicate()

    if p.returncode != 0:
        print("failed_ffmpeg_conversion "+str(err))
        return False
    return True

def is_bad_piece(wav_path, transcript):
    SAMPLE_RATE = 16000
    MAX_SECS = 10
    MIN_SECS = 1

    frames = int(subprocess.check_output(['soxi', '-s', wav_path], stderr=subprocess.STDOUT))
    

    if int(frames/SAMPLE_RATE*1000/10/2) < len(transcript):
        # Excluding samples that are too short to fit the transcript
        return True
    elif frames/SAMPLE_RATE > MAX_SECS:
        # Excluding very long samples to keep a reasonable batch-size
        return True
    elif frames/SAMPLE_RATE < MIN_SECS:
        # Excluding too small
        return True

def is_bad_subs(subs_text):
    bad = False

    if subs_text.strip() == "":
        bad = True

    if len(re.findall(r'[0-9]+', subs_text)) > 0:
        bad = True
    if len(re.findall(r'[A-Za-z]+', subs_text)) > 0:
        bad = True

    return bad

def cut(file_id, above_progress=0):
    pieces_rows = []
    
    data_path = os.path.join(curr_dir_path, "data/")

    audio_folder_path = os.path.join(data_path, 'audio/')
    audio_path_mp3 = os.path.join(audio_folder_path, ('%i.mp3' % file_id))
    if not os.path.exists(audio_path_mp3):
        raise Exception('cant find mp3 path %s' % audio_path_mp3)

    map_path_txt = os.path.join(data_path, ('maps/%i.txt' % file_id))
    if not os.path.exists(map_path_txt):
        raise Exception('cant find txt map path %s' % map_path_txt)

    
    # prepare folders
    
    

    parts_folder_path = os.path.join(dataset_folder_path, "parts/")

    if not os.path.exists(parts_folder_path):
        os.makedirs(parts_folder_path)

    # cut mp3 according to map

    m = pandas.read_table(map_path_txt, sep=' ', quotechar='"').as_matrix()

    audio_path_wav = os.path.join(audio_folder_path, "%i.wav" % file_id)
    if not os.path.exists(audio_path_wav):
        convert_to_wav(audio_path_mp3, audio_path_wav)

    
   

    def cutter_thread_method(d):
        i, row = d

        print  '%.2f %.2f' % (float(i)/len(m)*100, above_progress*100)

        start = row[1]
        end = row[2]

        if start >= end:
            return None

        transcript = row[3]

        if not (type(transcript) is str):
            return None

        transcript = transcript.decode('utf-8').replace("\n", " ").replace(' ', ' ')
        #print transcript.strip().lower()
        transcript = re.sub(u'[^а-яё ]', '', transcript.strip().lower()).strip()
        print transcript


        if is_bad_subs(transcript):
            return None

        #stats_good_subs_count += 1

        audio_piece_path = os.path.join(
                parts_folder_path, str(file_id) + "-" + str(int(start*1000)) + "-" + str(int(end*1000)) + ".wav")

        
        if not os.path.exists(audio_piece_path):
            cut_audio_piece_to_wav(audio_path_wav, audio_piece_path, start, end)

        if is_bad_piece(audio_piece_path, transcript):
            return None

        #stats_good_piece_count += 1

        file_size = os.path.getsize(audio_piece_path)

        row = [audio_piece_path, str(file_size), transcript]

    pool = ThreadPool(20)
    pieces_rows = pool.map(cutter_thread_method, enumerate(m))

    


    return pieces_rows


def create_dataset():
    
    if not os.path.exists(dataset_folder_path):
        os.makedirs(dataset_folder_path)

    export_all_csv_path = os.path.join(dataset_folder_path, DATASET_NAME+"-all.csv")
    export_train_csv_path = os.path.join(dataset_folder_path, DATASET_NAME+"-train.csv")
    export_dev_csv_path = os.path.join(dataset_folder_path, DATASET_NAME+"-dev.csv")
    export_test_csv_path = os.path.join(dataset_folder_path, DATASET_NAME+"-test.csv")

    export_vocabulary_txt_path = os.path.join(dataset_folder_path, DATASET_NAME+"-vocabulary.txt")


    all_rows = []

    folders_count = 50

    for i in range(1, folders_count+1):
        all_rows.extend(cut(i, float(i)/folders_count))
    

    all_count = len(all_rows)

    if all_count < 20:
        raise Exception('too small dataset < 20')

    # shuffle 

    random.shuffle(all_rows)

    # split in 3 groups train:dev:test 80:10:10
    train_rows = []
    dev_rows = []
    test_rows = []

    train_count = int(all_count*0.8)
    dev_count = int(all_count*0.1)


    all_rest = list(all_rows)
    train_rows = all_rest[:train_count]
    del all_rest[:train_count]
    dev_rows = all_rest[:dev_count]
    del all_rest[:dev_count]
    test_rows = all_rest

    print 'devided train:dev:test = '+str(len(train_rows))+':'+str(len(dev_rows))+':'+str(len(test_rows))


    # write sets
    header = ['wav_filename', 'wav_filesize', 'transcript']

    csv_utils.write_rows_to_csv(export_train_csv_path, [header])
    csv_utils.write_rows_to_csv(export_dev_csv_path, [header])
    csv_utils.write_rows_to_csv(export_test_csv_path, [header])

    csv_utils.append_rows_to_csv(export_train_csv_path, train_rows)
    csv_utils.append_rows_to_csv(export_dev_csv_path, dev_rows)
    csv_utils.append_rows_to_csv(export_test_csv_path, test_rows)


    # export vocabulary
    vocabulary = open(export_vocabulary_txt_path, "w")   
    vocabulary.writelines([x[2]+"\n" for x in all_rows])
    vocabulary.close()

create_dataset()
