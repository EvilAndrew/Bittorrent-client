from protocols import BaseTorrentClass
from rich.console import Console

import bencodepy
import time
import asyncio
import sys

async def async_downloaded_wrapper(obj, console):
    while True:
        console.print(f'Dowloaded: {obj.percent_downloaded()}%')
        await asyncio.sleep(10)

async def finish_checker(obj, console):
    while True:
        if obj.is_finished():
            console.print('The torrent is downloaded, finishing the download')
            obj.finish()
            console.print('The torrent is fully downloaded')
            sys.exit(0)
        await asyncio.sleep(30)

async def process(obj, loop, console):
    console.print('starting download')
    while True:
        if (not obj.last_announce_time) or (time.time() - obj.last_announce_time >= obj.interval):
            obj.interval = obj.start()
            obj.last_announce_time = time.time()

            await asyncio.gather(
                *[peer.proceed_peer_wrapper(obj, loop) for peer in obj.peers if peer.peer_id == None],
                async_downloaded_wrapper(obj, console),
                finish_checker(obj, console)
            )

def establish_connection(torrent_name, console):
    with open(torrent_name, mode='rb') as file:
        decoded_file = bencodepy.decode(file.read())

        obj = BaseTorrentClass(decoded_file)
        obj.convert(obj.protocolType)

        try:
            loop = asyncio.get_event_loop()

            loop.run_until_complete(process(obj, loop, console))

        except Exception as E:
            console.print('Something went wrong, the application is stopped')

if __name__ == '__main__':
    args = sys.argv[1:]

    console = Console(color_system=None)

    torrent_name = args[0]

    establish_connection(torrent_name, console)