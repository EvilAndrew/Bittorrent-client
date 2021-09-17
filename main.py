import bencodepy
from pprint import pprint
from protocols import BaseTorrentClass, UDPTorrent, HTTPTorrent
import time
import asyncio
import socket
import random

async def process(obj, loop):
    while True:
        if (not obj.last_announce_time) or (time.time() - obj.last_announce_time >= obj.interval):
            obj.interval = obj.start()
            obj.last_announce_time = time.time()

            await asyncio.gather(
                *[peer.proceed_peer_wrapper(obj, loop) for peer in obj.peers if peer.peer_id == None]
            )
        await asyncio.sleep(5)
        # return
        # if obj.is_finished():
        #     obj.finish()
        #     return
            

def establish_connection(torrent_name):
    with open(torrent_name, mode='rb') as file:
        decoded_file = bencodepy.decode(file.read())

        obj = BaseTorrentClass(decoded_file)
        obj.convert(obj.protocolType)

        # with open('output.txt', mode='w') as temp_f:
        #     temp_f.write(str(obj.decoded_file))
        # pprint(obj.decoded_file)
        # sync_single_peer_conn_test_func(obj)
        # return
        loop = asyncio.get_event_loop()
        loop.run_until_complete(process(obj, loop))

if __name__ == '__main__':
    torrent_name = "C:\\Users\\Andrew\\Desktop\\ubuntu-21.04-desktop-amd64.iso.torrent"
    establish_connection(torrent_name)