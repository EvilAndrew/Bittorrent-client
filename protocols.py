import socket
import random
import struct
import bencodepy
import hashlib
import time
import aiohttp
import asyncio
import urllib
import requests
from urllib.parse import urlparse
from pprint import pprint
from peer_controller import Peer

class BaseTorrentClass:
    def __init__(self, decoded_file):
        self.interval = 1_000_000_000
        self.decoded_file = decoded_file
        self.protocolType = None
        self.announce_url = decoded_file[b'announce']
        self.info_hash = hashlib.sha1(
            bencodepy.encode(decoded_file[b'info'])
        ).digest()
        self.peers = []
        self.peer_id = self.generate_peer_id()
        self.last_announce_time = None
        self.downloaded = dict()
        self.left = dict()
        self.in_process = dict()
        self.piece_data = dict()

        if self.announce_url.startswith(b'udp'):
            self.protocolType = UDPTorrent
        if self.announce_url.startswith(b'http'):
            self.protocolType = HTTPTorrent
    
    def convert(self, newClass):
        self.__class__ = newClass
    
    def get_piece_length(self, piece_index):
        piece_length = self.decoded_file[b'info'][b'piece length']
        if piece_index + 1 == len(self.decoded_file[b'info'][b'pieces']):
            total_length = self.decoded_file[b'info'][b'length']
            return total_length - piece_length*piece_index
        return piece_length

    def get_address_by_hostname(self, hostname):
        parsed = urlparse(hostname)
        return socket.gethostbyname(parsed.hostname), parsed.port
    
    def generate_peer_id(self):
        return '-PC0001-' + ''.join([str(random.randint(0, 9)) for _ in range(12)])
    
    def check_time(self):
        return time.time() - self.last_announce_time < self.interval
    
    def is_finished(self):
        return len(self.left) == 0 and len(self.in_process) == 0

    def finish(self):
        name = self.decoded_file[b'info'][b'name'].decode('utf-8')
        with open(name, mode='wb+') as file:
            for i in range(len(self.decoded_file[b'info'][b'pieces'])):
                with open(name + ';' + str(i), mode='rb') as piece_file:
                    file.write(piece_file.read())

class UDPTorrent(BaseTorrentClass):
    def create_connection_request(self):
        connection_id = 0x41727101980               # special constant
        action = 0x0
        transaction_id = random.randint(0, 65535)
        
        buffer = struct.pack("!q", connection_id)   # 8 bytes
        buffer += struct.pack("!i", action)         # 4 bytes
        buffer += struct.pack("!i", transaction_id) # 4 bytes

        return buffer, transaction_id

    def create_announce_request(self, connection_id):
        action = 0x01
        transaction_id = random.randint(0, 65535)
        info_hash = self.info_hash
        peer_id = self.peer_id
        downloaded = sum(self.downloaded.values())
        left = self.decoded_file[b'info'][b'length'] - downloaded
        uploaded = 0
        event = 0
        ip = 0
        key = 0
        num_want = -1
        port = 9999

        buffer = struct.pack("!q", connection_id)       # 8 bytes
        buffer += struct.pack("!i", action)             # 4 bytes
        buffer += struct.pack("!i", transaction_id)     # 4 bytes
        buffer += struct.pack("!20s", info_hash)        # 20 bytes
        buffer += struct.pack("!20s", peer_id.encode()) # 20 bytes
        buffer += struct.pack("!q", downloaded)         # 8 bytes
        buffer += struct.pack("!q", left)               # 8 bytes
        buffer += struct.pack("!q", uploaded)           # 8 bytes
        buffer += struct.pack("!i", event)              # 4 bytes
        buffer += struct.pack("!i", ip)                 # 4 bytes
        buffer += struct.pack("!i", key)                # 4 bytes
        buffer += struct.pack("!i", num_want)           # 4 bytes
        buffer += struct.pack("!h", port)               # 2 bytes

        return buffer, transaction_id
    
    def connect_to_tracker(self, sock, address):
        while True:
            try:
                buffer, transaction_id = self.create_connection_request()

                sock.sendto(buffer, address)

                res_bytes = sock.recvfrom(1024)[0]
                if len(res_bytes) < 16:
                    continue

                _action, _transaction_id, _connection_id = struct.unpack('!iiq', res_bytes[:16])
                if _action != 0 or _transaction_id != transaction_id:
                    continue
                return _connection_id
            except:
                continue
    
    def announce_to_tracker(self, sock, address, connection_id):
        OFFSET_BYTES = 20
        SEEDER_LENGTH = 6
        while True:
            try:
                buffer, transaction_id = self.create_announce_request(connection_id)
                sock.sendto(buffer, address)
                res_bytes = sock.recvfrom(65536)[0]
                if len(res_bytes) < OFFSET_BYTES:
                    continue

                _action, _transaction_id, interval, leechers, seeders = struct.unpack('!iiiii', res_bytes[:OFFSET_BYTES])
                if _action != 1 or _transaction_id != transaction_id:
                    continue

                self.interval = interval
                self.peers = []

                for i in range(seeders):
                    try:
                        # ip is 4-bytes sequence, but for our purpose it is more convenient to read it as 4 separate bytes
                        *ip, port = struct.unpack('!BBBBH',
                                                  res_bytes[OFFSET_BYTES + i*SEEDER_LENGTH : OFFSET_BYTES + (i+1)*SEEDER_LENGTH]
                                                  )
                        self.peers.append(
                            Peer('.'.join(map(str, ip)), port)
                        )
                    except Exception as e:
                        print('abacaba e:', e)
                        break
                break
            except Exception as E:
                print('E:', E)
                return

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            announce_url = self.decoded_file[b'announce']
            address = self.get_address_by_hostname(announce_url)

            connection_id = self.connect_to_tracker(sock, address)

            self.announce_to_tracker(sock, address, connection_id)
            
            return self.interval


class HTTPTorrent(BaseTorrentClass):
    def initial_announce_parameters(self, info_hash, peer_id, total_bytes):
        return {
            "info_hash": info_hash,
            "peer_id": peer_id,
            "uploaded": 0,
            "downloaded": 0,
            "left": total_bytes,
            "port": 9999,
            "compact": True
        }
    
    def start(self):
        params = self.initial_announce_parameters(self.info_hash, self.peer_id, self.decoded_file[b'info'][b'length'])
        # url = self.announce_url.decode() + '?' + urllib.urlencode(params)
        response = requests.get(self.announce_url.decode(), params=params)
        response = bencodepy.decode(response.content)
        peer_bin_string = response[b'peers']

        try:
            SEEDER_LENGTH = 6
            for i in range(0, len(peer_bin_string), 6):
                *ip, port = struct.unpack('!BBBBH',
                    peer_bin_string[i : i + SEEDER_LENGTH]
                )
                self.peers.append(
                    Peer('.'.join(map(str, ip)), port)
                )
        except Exception as E:
            print(f'peers exception: {E}')

        return response[b'interval']