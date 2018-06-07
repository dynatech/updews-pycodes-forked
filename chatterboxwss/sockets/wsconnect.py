from websocket import create_connection
import time
import volatile.memory as mem
import wsslib as wss

sc = mem.server_config()
url = sc["wss_credentials"]["host"]
port = sc["wss_credentials"]["port"]
wss.reconnection(url, port)