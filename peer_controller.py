import asyncio
import struct
import enum
import aiofiles
import hashlib

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
        if len(message) != 40: # info_hash + peer_id
            return False
        return message[:20] == info_hash
    
    def parse_handshake_message(self, message):
        HEADER_LENGTH = len(b"\x13BitTorrent protocol")
        RESERVED_LENGTH = 8
        INFO_HASH_LENGTH = 20
        return struct.unpack('!20s', message[HEADER_LENGTH + RESERVED_LENGTH + INFO_HASH_LENGTH:])

    async def handshake(self, info_hash, peer_id, loop):
        buffer = self.generate_handshake_message(info_hash, peer_id)

        data = None
        reader = None
        writer = None

        try:
            print(f'open connection {self.ip}')
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.ip, self.port),
                timeout=60
            )

            print(f'writing {self.ip}')

            writer.write(buffer)
            await writer.drain()

            print(f'reading {self.ip}')
            data = await reader.read(len(buffer))
            print(f'data: {data}')
            if not data:
                raise Exception('Peer disconnected')
        
        except Exception as E:
            # print(f'handshake: {E}')
            # print()

            if writer != None and not writer.is_closing:
                writer.close()
                await writer.wait_closed()

                reader, writer = None

        finally:
            return data, reader, writer

    def create_request_message(self, piece_index, l, r):
        # <length=13><id=6><piece_index><offset_within_piece><block_length>
        return struct.pack('!IBIII', 13, 6, piece_index, l, r - l)

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

    def compare_hash(self, obj, piece, piece_index):
        need_hash = obj.get_piece_hash(piece_index)
        cur_hash = hashlib.sha1(piece).digest()

        return cur_hash == need_hash

    async def finish_downloading_piece(self, obj, piece_index):
        print("FINISHED DOWNLOADING PIECE NUMBER", piece_index)

        piece = obj.piece_data[piece_index]
        del obj.piece_data[piece_index]
        del obj.in_process[piece_index]

        piece_length = obj.get_piece_length(piece_index)

        if not self.compare_hash(obj, piece, piece_index):
            obj.left[piece_index] = piece_length
        else:
            print()
            print("SUCCESFULLY FINISHED DOWNLOADING PIECE NUMBER", piece_index)
            print()

            obj.downloaded[piece_index] = piece_length
            filename = obj.decoded_file[b'info'][b'name'].decode('utf-8')
            filename += ';' + str(piece_index)
            self.available = True
            async with aiofiles.open(filename, mode='wb+') as piece_file:
                await piece_file.write(piece)

    async def save_block(self, reader, writer, obj,
                        piece_index, piece_offset, block):
        piece_length = obj.get_piece_length(piece_index)
        obj.piece_data[piece_index] =\
            obj.piece_data.get(piece_index, b'') + block
        if piece_offset + len(block) >= piece_length:
            await self.finish_downloading_piece(obj, piece_index)
        else:
            obj.in_process[piece_index] = len(obj.piece_data[piece_index])
            await self.send_request(reader, writer, obj, piece_index)

    async def make_interested(self, writer):
        self.states.discard(PeerMessage.NOT_INTERESTED)
        self.states.add(PeerMessage.INTERESTED)

        writer.write(struct.pack('!IB', 1, 2))
        await writer.drain()

    async def start_chatting(self, reader, writer, obj, loop):
        print("start_chatting started")
        try:
            async for byte_message in PeerMessageIterator(reader):
                if not obj.check_time():
                    raise Exception("Tracker timeout (needs reconnecting)")
                if byte_message is None:
                    continue

                type = PeerMessage.determine_type(byte_message)
                length = byte_message[: int(PeerMessage.HEADER_LENGTH)]

                print(type)

                if type is PeerMessage.KEEP_ALIVE:
                    pass

                elif type is PeerMessage.CHOKE:
                    self.states.discard(PeerMessage.UNCHOKE)
                    self.states.add(PeerMessage.CHOKE)

                elif type is PeerMessage.UNCHOKE:
                    self.states.discard(PeerMessage.CHOKE)
                    self.states.add(PeerMessage.UNCHOKE)

                elif type is PeerMessage.INTERESTED:
                    pass
                elif type is PeerMessage.NOT_INTERESTED:
                    pass

                elif type is PeerMessage.HAVE:
                    piece_index = byte_message[
                        int(PeerMessage.HEADER_LENGTH) +\
                        int(PeerMessage.ID_LENGTH) :
                    ]
                    print('HAVE', int.from_bytes(piece_index, "big"))
                    self.has_pieces.add(int.from_bytes(piece_index, "big"))

                elif type is PeerMessage.BITFIELD:
                    print('BITFIELD')
                    bitfield = byte_message[
                        int(PeerMessage.HEADER_LENGTH) +\
                        int(PeerMessage.ID_LENGTH) :
                    ]
                    for i in range(len(bitfield)):
                        for j in range(8):
                            if (1 << j) & bitfield[i]:
                                self.has_pieces.add(i*8 + j)
                
                elif type is PeerMessage.REQUEST:
                    pass
                
                elif type is PeerMessage.PIECE:
                    print("A PIECE HAS COME")
                    l = int(PeerMessage.HEADER_LENGTH)\
                        + int(PeerMessage.ID_LENGTH)
                    r = l + int(PeerMessage.INT_LENGTH)
                    piece_index = byte_message[l:r]
                    piece_index = int.from_bytes(piece_index, "big")

                    piece_offset = byte_message[r : r + int(PeerMessage.INT_LENGTH)]
                    piece_offset = int.from_bytes(piece_offset, "big")

                    block = byte_message[r + int(PeerMessage.INT_LENGTH) :]

                    print("SAVING THE BLOCK", piece_index, piece_offset)
                    await self.save_block(reader, writer, obj,
                            piece_index,
                            piece_offset, block)
                    print("SAVED THE BLOCK")
                elif type is PeerMessage.CANCEL:
                    pass
                elif type is PeerMessage.PORT:
                    pass

                if PeerMessage.INTERESTED not in self.states:
                    await self.make_interested(writer)
 
                if (PeerMessage.INTERESTED in self.states
                    and PeerMessage.UNCHOKE in self.states
                    and self.available):

                    print("TRYING TO REQUEST")

                    for piece_index in self.has_pieces:
                        if piece_index in obj.left:
                            obj.in_process[piece_index] = 0
                            del obj.left[piece_index]

                            self.available = False
                            print("REQUEST SENT")
                            await self.send_request(reader, writer, obj, piece_index)
        except Exception as E:
            print("ASYNC FOR EXCEPTION", E)

    async def proceed_peer_wrapper(self, obj, loop):
        try:
            await self.proceed_peer(obj, loop)
        except:
            self.clear_peer()

    async def proceed_peer(self, obj, loop):
        if not obj.check_time():
            raise Exception("Tracker timeout (needs reconnecting)")
        data, reader, writer = await self.handshake(obj.info_hash, obj.peer_id, loop)
        # print(data, reader, writer)
        if not reader or not writer:
            return
        if not data or not self.check_handshake_message(data, obj.info_hash):
            return
        # print('handshake parse: in')
        self.peer_id = self.parse_handshake_message(data)
        # print('handshake parse: out')
        print(f'Handshake initiated {self.ip}, {self.port}, {self.peer_id}')

        try:
            if not obj.check_time():
                raise Exception("Tracker timeout (needs reconnecting)")
            await self.start_chatting(reader, writer, obj, loop)
        except:
            if writer != None and not writer.is_closing:
                writer.close()
                await writer.wait_closed()
            self.clear_peer()


class PeerMessageIterator:
    CHUNK_SIZE = 1024
    def __init__(self, reader):
        self.reader = reader
        self.buffer = b''

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            data = await self.reader.read(PeerMessageIterator.CHUNK_SIZE)
            self.buffer += data
            message = self.parse() # changes buffer and its length if successful
            if message is not None:
                return message
            if not data:
                raise StopAsyncIteration
        except:
            raise StopAsyncIteration

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

    def determine_type(byte_message : bytes):
        HEADER_LENGTH = int(PeerMessage.HEADER_LENGTH)
        length = struct.unpack('!I', byte_message[: HEADER_LENGTH])[0]
        if length == 0:
            return PeerMessage.KEEP_ALIVE

        id = byte_message[HEADER_LENGTH]

        return PeerMessage(id)
