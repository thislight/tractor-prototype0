"""
This is a Directory Server
Directory server in tractor provide directory of contents and avaliable devices can connect to for one profile,
which can by controlled by real user or a bot software.
This prototype uses ZeroMQ.
We have two ports opened for apps:
* ROUTER 5350: command port
* PUB 5351: file changes
These are commands used by apps:
device.cast_address | name: str | address: str -> 0
device.get_addresses | name -> 0 | addresses: str (json list of str)
ping | device_name: str -> "pong" | peer_address: str
fs.list -> 0 | file_list: str (json list of str)
fs.declare | filename: str -> 0
fs.disown | filename: str -> 0
fs.get | filename: str -> 0 | devices: str (json list of str)
"""
import zmq
import json
from dataclasses import dataclass
from zmq import Socket, Context, Poller, Frame
from typing import List, Iterable, Dict, Tuple

@dataclass
class Device(object):
    name: str
    cast_addresses: List[str]

@dataclass
class VirtualFile(object):
    name: str
    declared_devices: List[Device]

class DirectoryServerStore(object):
    def __init__(self):
        self.devices: Dict[str, Device] = {}
        self.files: Dict[str, VirtualFile] = {}

def ping_handler(store: DirectoryServerStore, sock: Socket, argframes: List[Frame], id_frame: Frame) -> None:
    device_name = str(argframes.pop(0).bytes, encoding='utf8')
    reply = [id_frame, Frame(), Frame(sock.getsockopt_string("Peer-Address"))]
    sock.send_multipart(reply)
    if device_name not in store.devices:
        store.devices[device_name] = Device(device_name, [])

def casting_address_handler(store: DirectoryServerStore, sock: Socket, argframes: List[Frame], id_frame: Frame) -> None:
    device_name = str(argframes.pop(0).bytes, encoding='utf8')
    address = str(argframes.pop(0).bytes, encoding='utf8')
    if device_name not in store.devices:
        store.devices[device_name] = Device(device_name, [])
    device = store.devices[device_name]
    if address not in device.cast_addresses:
        device_name.cast_addresses.append(address)
    reply = [id_frame, Frame(), Frame(0)]
    sock.send_multipart(reply)

def directory_server(store: DirectoryServerStore):
    zmq_context = Context.instance()
    # pylint: disable=no-member # These zmq.ROUTER and zmq.PUB must be actually exists
    entrypoint: Socket = zmq_context.socket(zmq.ROUTER)
    entrypoint.bind("tcp://127.0.0.1:5350") # This is just a PROTOTYPE!
    pub_file_changes: Socket = zmq_context.socket(zmq.PUB)
    pub_file_changes.bind("tcp://127.0.0.1:5351")
    poller = Poller()
    poller.register(entrypoint, flags=zmq.POLLIN)
    while True:
        events: List[Tuple[Socket, int]] = poller.poll(1)
        for socket, in events:
            frames: List[Frame] = socket.recv_multipart()
            id_frame: Frame = frames.pop(0)
            empty_frame: Frame = frames.pop(0)
            assert(len(empty_frame.bytes) == 0)
            command_frame: Frame = frames.pop(0)
            command = str(command_frame.bytes, encoding='utf8')
            if command == 'ping':
                ping_handler(store, socket, frames, id_frame)
            elif command == 'device.cast_address':
                casting_address_handler(store, socket, frames, id_frame)
            elif command == 'device.get_addresses':
                pass

def main():
    store = DirectoryServerStore()
    directory_server(store)

if __name__ == "__main__":
    main()
