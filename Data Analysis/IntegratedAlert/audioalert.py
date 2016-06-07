import ConfigParser
import os
import pyglet
pyglet.options['debug_gl'] = False
from datetime import datetime, timedelta

output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

def up_one(p):
    out = os.path.abspath(os.path.join(p, '..'))
    return out

cfg = ConfigParser.ConfigParser()
cfg.read(up_one(os.path.dirname(__file__)) + '/server-config.txt')    

gsm_alert = cfg.get('I/O','gsmalert')
output_file_path = output_path + cfg.get('I/O','OutputFilePath')

if os.stat(output_file_path+gsm_alert).st_size != 0:
    start = datetime.now()
    audio_source = pyglet.media.load('alert.wav', streaming=False)
    looper = pyglet.media.SourceGroup(audio_source.audio_format, None)
    looper.loop = False
    looper.queue(audio_source)
    player = pyglet.media.Player()
    player.queue(looper)
    player.play()
    f = open(output_file_path+gsm_alert)
    text = f.read()
    f.close()
    while (datetime.now() - start) <= (timedelta(hours = 0.005)):
        print text
    