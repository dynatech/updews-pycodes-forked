import ConfigParser
import os
import pyglet
pyglet.options['debug_gl'] = False

cfg = ConfigParser.ConfigParser()
cfg.read('server-config.txt')    

gsm_alert = cfg.get('I/O','gsmalert')
output_file_path = cfg.get('I/O','OutputFilePath')

if os.stat(output_file_path+gsm_alert).st_size != 0:
    audio_source = pyglet.media.load('alert.wav', streaming=False)
    looper = pyglet.media.SourceGroup(audio_source.audio_format, None)
    looper.loop = True
    looper.queue(audio_source)
    player = pyglet.media.Player()
    player.queue(looper)
    player.play()
    f = open(output_file_path+gsm_alert)
    text = f.read()
    f.close()
    print text
    while True:
        continue