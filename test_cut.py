
import os
import pandas
import sys

reload(sys)
sys.setdefaultencoding('utf8')

def cut(file_id):
    curr_dir_path = os.path.dirname(os.path.realpath(__file__))
    data_path = os.path.join(curr_dir_path, "data/")

    audio_path_mp3 = os.path.join(data_path, ('audio/%i.mp3' % file_id))
    if not os.path.exists(audio_path_mp3):
        raise Exception('cant find mp3 path %s' % audio_path_mp3)

    map_path_txt = os.path.join(data_path, ('maps/%i.txt' % file_id))
    if not os.path.exists(map_path_txt):
        raise Exception('cant find txt map path %s' % map_path_txt)

    m = pandas.read_table(map_path_txt, sep=' ', quotechar='"')
    print m.as_matrix()[0][3]

cut(1)

