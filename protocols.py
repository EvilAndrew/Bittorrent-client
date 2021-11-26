import socket
import random
import struct
import bencodepy
import hashlib
import time
import requests
import os

from urllib.parse import urlparse
from peer_controller import Peer

class BaseTorrentClass:
    def __init__(self, decoded_file):

        """
            period of time 
            after which the repeated request to tracker must happen
        """
        self.interval = 1_000_000_000

        # parsed .torrent file
        self.decoded_file = decoded_file

        # whether it is HTTP or UDP
        self.protocolType = None

        # url of the tracker
        self.announce_url = decoded_file[b'announce']

        self.info_hash = hashlib.sha1(
            bencodepy.encode(decoded_file[b'info'])
        ).digest()

        # future list of peers returned by the tracker
        self.peers = []

        # self-generated peer_id
        self.peer_id = self.generate_peer_id()

        # the last time announce happened
        self.last_announce_time = None

        """
            dictionaries used for managing information
            of how many bytes are already downloaded
            in particular pieces
        """
        self.downloaded = dict()
        self.left = dict()
        self.in_process = dict()
        self.piece_data = dict()

        # //20 because the line refers hash of a piece is of length 20
        self.pieces_number = len(self.decoded_file[b'info'][b'pieces'])//20

        """
            calculates sizes of pieces;
            all of pieces usually have the same size,
            except for the last one,
            as the size of the last one may differ
        """
        for i in range(self.pieces_number):
            total_length = decoded_file[b'info'][b'length']

            piece_length = decoded_file[b'info'][b'piece length']

            self.left[i] = min(piece_length,
                               total_length - i*piece_length)

        # determines which protocol the tracker follows
        if self.announce_url.startswith(b'udp'):
            self.protocolType = UDPTorrent

        elif self.announce_url.startswith(b'http'):
            self.protocolType = HTTPTorrent

    def percent_downloaded(self):
        ratio = sum(self.downloaded.values())\
                + sum(self.in_process.values())

        ratio /= self.decoded_file[b'info'][b'length']

        # rounds up to the first sign after point
        return int(ratio * 1000)/10

    """
        converts either to HTTPTorrent class
        or to UDPTorrent
    """
    def convert(self, newClass):
        self.__class__ = newClass

    def get_piece_hash(self, piece_index):
        length = self.pieces_number

        # all hashes in .torrent file are of fixed length 20
        SHA1_LENGTH = 20

        """
            calculate the borders of the hash we need
            that corresponds to piece_index
        """
        L = SHA1_LENGTH*piece_index

        R = SHA1_LENGTH*(piece_index + 1)

        R = min(R, length * 20)

        return self.decoded_file[b'info'][b'pieces'][L:R]

    """
        the function is needed as not all pieces
        are equal in terms of their sizes
    """
    def get_piece_length(self, piece_index):
        piece_length = self.decoded_file[b'info'][b'piece length']

        if piece_index + 1 == self.pieces_number:
            total_length = self.decoded_file[b'info'][b'length']

            return total_length - piece_length*piece_index

        return piece_length

    # return IP and port of a particular hostname
    def get_address_by_hostname(self, hostname):
        parsed = urlparse(hostname)

        return socket.gethostbyname(parsed.hostname), parsed.port

    # randomly generates our peer_id of length 20
    def generate_peer_id(self):
        return '-PC0001-' + ''.join([str(random.randint(0, 9)) for _ in range(12)])

    def check_time(self):
        return time.time() - self.last_announce_time < self.interval

    """
        if we neither have any pieces left to be downloaded
        nor we are downloading any piece right now
    """
    def is_finished(self):
        return len(self.left) == 0 and len(self.in_process) == 0

    """
        merges all the bytes in required order
        into a single file which name is in accordance with the name
        provided by .torrent file
    """
    def finish(self):
        name = self.decoded_file[b'info'][b'name'].decode('utf-8')

        with open(name, mode='wb+') as file:

            """
                iterates through pieces files we saved
                in order,
                copies their information to the single file,
                and then deletes that piece-file
            """
            for i in range(self.pieces_number):

                with open(name + ';' + str(i), mode='rb') as piece_file:
                    file.write(piece_file.read())

                os.remove(name + ';' + str(i))


class UDPTorrent(BaseTorrentClass):
    def create_connection_request(self):
        
        """
            special constant;
            do not think about how it is derived
        """
        connection_id = 0x41727101980

        # specified id of action in accordance with the protocol
        action = 0x0

        transaction_id = random.randint(0, 65535)
        
        # 8 bytes
        buffer = struct.pack("!q", connection_id)

        # 4 bytes
        buffer += struct.pack("!i", action)

        # 4 bytes
        buffer += struct.pack("!i", transaction_id)

        return buffer, transaction_id

    def create_announce_request(self, connection_id):

        # specified id of action in accordance with the protocol
        action = 0x01

        transaction_id = random.randint(0, 65535)

        info_hash = self.info_hash

        peer_id = self.peer_id

        downloaded = sum(self.downloaded.values())

        left = self.decoded_file[b'info'][b'length'] - downloaded

        # as uploading is not yet implemented in the project
        uploaded = 0

        # simple constants
        event = 0
        ip = 0
        key = 0
        num_want = -1
        port = 9999

        # 8 bytes
        buffer = struct.pack("!q", connection_id)

        # 4 bytes
        buffer += struct.pack("!i", action)

        # 4 bytes
        buffer += struct.pack("!i", transaction_id)

        # 20 bytes
        buffer += struct.pack("!20s", info_hash)

        # 20 bytes
        buffer += struct.pack("!20s", peer_id.encode())

        # 8 bytes
        buffer += struct.pack("!q", downloaded)

        # 8 bytes
        buffer += struct.pack("!q", left)

        # 8 bytes
        buffer += struct.pack("!q", uploaded)

        # 4 bytes
        buffer += struct.pack("!i", event)

        # 4 bytes
        buffer += struct.pack("!i", ip)

        # 4 bytes
        buffer += struct.pack("!i", key)

        # 4 bytes
        buffer += struct.pack("!i", num_want)

        # 2 bytes
        buffer += struct.pack("!h", port)

        return buffer, transaction_id

    """
        tries to connect to the torrent tracker
        until the connection is successful
    """
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

            except Exception as E:
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
                        """
                            ip is 4-part sequence separated by dots
                            but for our purpose it is more convenient
                            to read it as 4 separate bytes
                        """
                        *ip, port = struct.unpack(
                            '!BBBBH',
                            res_bytes[OFFSET_BYTES + i*SEEDER_LENGTH :\
                                         OFFSET_BYTES + (i+1)*SEEDER_LENGTH]
                        )
                        self.peers.append(
                            Peer('.'.join(map(str, ip)), port)
                        )
                    except Exception as e:
                        break
                break
            except Exception as E:
                return

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            announce_url = self.decoded_file[b'announce']

            address = self.get_address_by_hostname(announce_url)

            connection_id = self.connect_to_tracker(sock, address)

            self.announce_to_tracker(sock, address, connection_id)
            
            return self.interval


"""
    the requesting part is easier
    than with UDP
    because with UDP the connection
    had to be managed by ourselves;
    in case of HTTP, "requests" library handles it for us
"""
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
        params = self.initial_announce_parameters(
            self.info_hash,
            self.peer_id,
            self.decoded_file[b'info'][b'length']
        )

        response = requests.get(self.announce_url.decode(), params=params)

        response = bencodepy.decode(response.content)

        peer_bin_string = response[b'peers']

        try:
            SEEDER_LENGTH = 6

            for i in range(0, len(peer_bin_string), 6):

                *ip, port = struct.unpack(
                    '!BBBBH',
                    peer_bin_string[i : i + SEEDER_LENGTH]
                )

                self.peers.append(
                    Peer('.'.join(map(str, ip)), port)
                )

        except Exception as E:
            pass

        return response[b'interval']
