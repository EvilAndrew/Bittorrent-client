import asyncio
import struct
import enum
import aiofiles
import hashlib

"""
    class that does the main work of downloading data
    from a single peer
"""
class Peer:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.peer_id = None
        self.bitfield = None
        self.has_pieces = set()
        self.states = {PeerMessage.CHOKE, PeerMessage.NOT_INTERESTED}
        self.available = True

    def __repr__(self):
        return f'({self.ip}, {self.port})'

    def generate_handshake_message(self, info_hash, peer_id):
        header = b'\x13BitTorrent protocol\x00\x00\x00\x00\x00\x00\x00\x00'

        return header + info_hash + bytes(peer_id, encoding="UTF-8")

    def check_handshake_message(self, message, info_hash):
        beginning = b"\x13BitTorrent protocol"

        length = len(beginning)

        RESERVED_LENGTH = 8

        if len(message) < length or message[:length] != beginning[:]:
            return False

        message = message[length + RESERVED_LENGTH:]
        
        # info_hash + peer_id
        if len(message) != 40: 
            return False

        return message[:20] == info_hash
    
    def parse_handshake_message(self, message):
        HEADER_LENGTH = len(b"\x13BitTorrent protocol")

        RESERVED_LENGTH = 8

        INFO_HASH_LENGTH = 20

        return struct.unpack('!20s', message[HEADER_LENGTH + RESERVED_LENGTH + INFO_HASH_LENGTH:])

    # initiating connection with the peer
    async def handshake(self, info_hash, peer_id, loop):
        buffer = self.generate_handshake_message(info_hash, peer_id)

        data = None
        reader = None
        writer = None

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.port),
                timeout=60
            )

            writer.write(buffer)
            await writer.drain()

            data = await reader.read(len(buffer))
            if not data:
                raise Exception('Peer disconnected')
        
        except Exception as E:

            if writer != None and not writer.is_closing:
                writer.close()
                await writer.wait_closed()

                reader, writer = None

        finally:
            return data, reader, writer

    def create_request_message(self, piece_index, l, r):
        # <length=13><id=6><piece_index><offset_within_piece><block_length>
        return struct.pack('!IBIII', 13, 6, piece_index, l, r - l)

    # sends request for a block of piece of certain BLOCK_LENGTH
    async def send_request(self, reader, writer, obj, piece_index):
        BLOCK_LENGTH = 2**14
        piece_length = obj.get_piece_length(piece_index)

        downloaded = obj.in_process[piece_index]
        writer.write(
            self.create_request_message(
                piece_index, downloaded, min(piece_length, downloaded + BLOCK_LENGTH)
            )
        )
        await writer.drain()

    def clear_peer(self):
        self.__init__(self.port, self.ip)

    """
        retrieves hash from the .torrent files
        and compares it with what we have now
    """
    def compare_hash(self, obj, piece, piece_index):
        need_hash = obj.get_piece_hash(piece_index)

        cur_hash = hashlib.sha1(piece).digest()

        return cur_hash == need_hash

    """
        saves the piece into a separate file
        to "clear" RAM used by the Python program
        and instead translate that piece into hard-disk space
    """
    async def finish_downloading_piece(self, obj, piece_index):

        piece = obj.piece_data[piece_index]

        del obj.piece_data[piece_index]
        del obj.in_process[piece_index]

        piece_length = obj.get_piece_length(piece_index)

        """
            what if something has gone wrong
            and piece-hashes do not match
        """
        if not self.compare_hash(obj, piece, piece_index):
            obj.left[piece_index] = piece_length

        # if everything is OK and this is bit-by-bit the piece
        else:
            obj.downloaded[piece_index] = piece_length

            filename = obj.decoded_file[b'info'][b'name'].decode('utf-8')

            filename += ';' + str(piece_index)

            self.available = True

            """
                asynchronous library to work with files
                so that we did not wait additional time
            """
            async with aiofiles.open(filename, mode='wb+') as piece_file:
                await piece_file.write(piece)

    async def save_block(self, reader, writer, obj,
                        piece_index, piece_offset, block):

        piece_length = obj.get_piece_length(piece_index)

        obj.piece_data[piece_index] =\
            obj.piece_data.get(piece_index, b'') + block

        # if the piece is already downloaded and can be saved
        if piece_offset + len(block) >= piece_length:
            await self.finish_downloading_piece(obj, piece_index)

        # if the piece has not been downloaded entirely
        else:
            obj.in_process[piece_index] = len(obj.piece_data[piece_index])

            await self.send_request(reader, writer, obj, piece_index)

    async def make_interested(self, writer):
        self.states.discard(PeerMessage.NOT_INTERESTED)

        self.states.add(PeerMessage.INTERESTED)

        writer.write(struct.pack('!IB', 1, 2))

        await writer.drain()

    """
        the core method of the whole class
        manages all the downloading/requesting parts
        depending on the type of byte messages received
        from the implemented iterator
    """
    async def start_chatting(self, reader, writer, obj, loop):
        try:
            async for byte_message in PeerMessageIterator(reader):
                if not obj.check_time():
                    raise Exception("Tracker timeout (needs reconnecting)")

                if byte_message is None:
                    continue

                type = PeerMessage.determine_type(byte_message)

                length = byte_message[: int(PeerMessage.HEADER_LENGTH)]

                if type is PeerMessage.KEEP_ALIVE:
                    pass

                elif type is PeerMessage.CHOKE:
                    self.states.discard(PeerMessage.UNCHOKE)

                    self.states.add(PeerMessage.CHOKE)

                elif type is PeerMessage.UNCHOKE:
                    self.states.discard(PeerMessage.CHOKE)

                    self.states.add(PeerMessage.UNCHOKE)

                # to be implemented for uploading pieces in the future
                elif type is PeerMessage.INTERESTED:
                    pass

                # to be implemented for uploading pieces in the future
                elif type is PeerMessage.NOT_INTERESTED:
                    pass

                elif type is PeerMessage.HAVE:
                    piece_index = byte_message[
                        int(PeerMessage.HEADER_LENGTH) +\
                        int(PeerMessage.ID_LENGTH) :
                    ]

                    self.has_pieces.add(int.from_bytes(piece_index, "big"))

                elif type is PeerMessage.BITFIELD:
                    bitfield = byte_message[
                        int(PeerMessage.HEADER_LENGTH) +\
                        int(PeerMessage.ID_LENGTH) :
                    ]

                    """
                        unparses bitfield because it is received
                        in composed format
                        containing 0s and 1s
                        but in byte-length integers
                    """

                    for i in range(len(bitfield)):
                        # iterating through byte-length
                        for j in range(8):
                            # if j-th bit in i-th byte is 1
                            if (1 << j) & bitfield[i]:
                                self.has_pieces.add(i*8 + j)
                
                elif type is PeerMessage.REQUEST:
                    pass

                    """
                    if the thing that has come in byte_message
                    is file-data (i.e., a piece of that file-data)
                    """

                elif type is PeerMessage.PIECE:

                    # requires some parsing first
                    l = int(PeerMessage.HEADER_LENGTH)\
                        + int(PeerMessage.ID_LENGTH)

                    r = l + int(PeerMessage.INT_LENGTH)

                    piece_index = byte_message[l:r]

                    piece_index = int.from_bytes(piece_index, "big")

                    piece_offset = byte_message[r : r + int(PeerMessage.INT_LENGTH)]

                    piece_offset = int.from_bytes(piece_offset, "big")

                    block = byte_message[r + int(PeerMessage.INT_LENGTH) :]

                    """
                        pieces of file-data do not come entirely
                        so the file-data is divided into pieces
                        which are divided into blocks
                        and save_block(...) saves the blocks inside pieces
                        and controls what is requested next in the corresponding piece
                    """

                    await self.save_block(reader, writer, obj,
                            piece_index,
                            piece_offset, block)

                elif type is PeerMessage.CANCEL:
                    pass

                elif type is PeerMessage.PORT:
                    pass

                if PeerMessage.INTERESTED not in self.states:
                    await self.make_interested(writer)
 
                # if we already can request a piece from the peer
                if PeerMessage.INTERESTED in self.states\
                    and PeerMessage.UNCHOKE in self.states\
                    and self.available:

                    """
                        which pieces the peers has
                        is derived from HAVE and BITFIELD messages
                    """
                    for piece_index in self.has_pieces:

                        """
                            if we have not started downloading the piece
                            nor it is in process of downloading either
                        """
                        if piece_index in obj.left:
                            obj.in_process[piece_index] = 0
                            del obj.left[piece_index]

                            self.available = False
                            await self.send_request(reader, writer, obj, piece_index)

        except Exception as E:
            pass

    """
        simply a function wrapper
        in case something goes wrong in that function
    """
    async def proceed_peer_wrapper(self, obj, loop):
        try:
            await self.proceed_peer(obj, loop)

        except:
            self.clear_peer()

    async def proceed_peer(self, obj, loop):
        if not obj.check_time():
            raise Exception("Tracker timeout (needs reconnecting)")

        data, reader, writer = await self.handshake(obj.info_hash, obj.peer_id, loop)

        """
            in case something went wrong
            and handshake did not return reader or writer objects
        """
        if not reader or not writer:
            return

        """
            if handshake data has not been read successfully
            or the handshake message itself is wrong
        """
        if not data or not self.check_handshake_message(data, obj.info_hash):
            return

        # returns unique peer_id from the byte handshake message
        self.peer_id = self.parse_handshake_message(data)

        try:
            if not obj.check_time():
                raise Exception("Tracker timeout (needs reconnecting)")

            await self.start_chatting(reader, writer, obj, loop)
        except Exception as E:

            """
                closes the writer object
                if it has not been closed yet
            """
            if writer != None and not writer.is_closing:
                writer.close()
                await writer.wait_closed()

            # "restarts" the peer if something has gone wrong
            self.clear_peer()


"""
    implementation of asynchronous iterator
    i.e., __aiter__ and __anext__

    Implemented with saving everything in buffer by chunks
    until at least one message can be parsed
"""
class PeerMessageIterator:

    """
        customly identified chunk size
        which is to be read from peers
        every time
    """
    CHUNK_SIZE = 1024

    def __init__(self, reader):
        self.reader = reader
        self.buffer = b''

    # initiates iteration
    def __aiter__(self):
        return self

    """
        returns message from buffer
        works asynchronously
        and is necessary for "async for"
        from "start_chatting" method
    """
    async def __anext__(self):
        try:
            data = await self.reader.read(PeerMessageIterator.CHUNK_SIZE)
            self.buffer += data

            # changes buffer and its length if successful
            message = self.parse()

            if message is not None:
                return message

            if not data:
                raise StopAsyncIteration
        except:
            raise StopAsyncIteration

    """
        parses byte_message from buffer
        because buffer may contain multiple messages
    """
    def parse(self):
        HEADER_LENGTH = int(PeerMessage.HEADER_LENGTH)

        if len(self.buffer) < HEADER_LENGTH:
            return None

        length = struct.unpack('!I', self.buffer[: HEADER_LENGTH])[0]

        if len(self.buffer) < length + HEADER_LENGTH:
            return None

        message = self.buffer[: HEADER_LENGTH + length]

        self.buffer = self.buffer[HEADER_LENGTH + length :]

        return message


"""
    class for identifying types of message
    during communication with peers
    also contains important byte-length of some types
    enum.IntEnum is analogous to C-like enums
"""
class PeerMessage(enum.IntEnum):
    KEEP_ALIVE = -1

    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8
    PORT = 9

    HEADER_LENGTH = 4
    ID_LENGTH = 1
    INT_LENGTH = 4

    # returns type of byte message
    def determine_type(byte_message : bytes):
        HEADER_LENGTH = int(PeerMessage.HEADER_LENGTH)

        length = struct.unpack('!I', byte_message[: HEADER_LENGTH])[0]

        if length == 0:
            return PeerMessage.KEEP_ALIVE

        id = byte_message[HEADER_LENGTH]

        return PeerMessage(id)
