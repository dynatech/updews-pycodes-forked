import socket
import time
from websocket import create_connection

def fortmat_send_status_request(outbox_id, send_status, gsm_id):
	json_text = """{"type":"smssent","outbox_id":"%s","send_status":"%s",\
                "gsm_id":"%s"}""" % (outbox_id, send_status, gsm_id)
	send_wss(json_text);

def send_wss(msg = ""):
    sc = mem.server_config()
	host = sc["wss_credentials"]["host"]
	port = sc["wss_credentials"]["port"]
    try:
        ws = create_connection("ws://%s:%s" % (host, port))
        ws.send(msg)
        print "Sent %s" % (msg)
        ws.close()
        return 0
    except:
        print "Failed to send data. Please check your internet connection"
        return -1

def connect_wss(host, port):
    try:
        ws = create_connection("ws://%s:%s" % (host, port))
        return 0
    except:
        print "Failed to send data. Please check your internet connection"
        return -1

def reconnection(host, port):
    url = "ws://%s:%s/" % (host, port)
    delay = 5
    while True:
        try:
            print "Receiving..."
            result = ws.recv()
            delay = 5
        except Exception, e:
            try:
                print "Connecting to Websocket Server..."
                ws = create_connection(url)
            except Exception, e:
                print "Disconnected! will attempt reconnection in %s \
                        seconds..." % (delay)
                time.sleep(delay)

                if delay < 10:
                    delay += 1

    ws.close()