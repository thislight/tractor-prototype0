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
fs.declare | device_name: str | filename: str -> 0
fs.disown | device_name: str | filename: str -> 0
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
    refcount: int

class DirectoryServerStore(object):
    def __init__(self):
        self.devices: Dict[str, Device] = {}
        self.files: Dict[str, VirtualFile] = {}

def ping_handler(store: DirectoryServerStore, sock: Socket, argframes: List[Frame], id_frame: Frame) -> None:
    device_name_frame = argframes.pop(0)
    device_name = str(device_name_frame.bytes, encoding='utf8')
    peer_addr = device_name_frame.get("Peer-Address")
    print("Ping from {}".format(peer_addr))
    reply = [id_frame, Frame(), Frame(b"pong"), Frame(bytes(peer_addr, encoding='utf8'))]
    sock.send_multipart(reply)
    if device_name not in store.devices:
        store.devices[device_name] = Device(device_name, [])
        print("New device {} is created".format(device_name))

def casting_address_handler(store: DirectoryServerStore, sock: Socket, argframes: List[Frame], id_frame: Frame) -> None:
    device_name = str(argframes.pop(0).bytes, encoding='utf8')
    address = str(argframes.pop(0).bytes, encoding='utf8')
    if device_name not in store.devices:
        store.devices[device_name] = Device(device_name, [])
    device = store.devices[device_name]
    if address not in device.cast_addresses:
        device.cast_addresses.append(address)
        print("Device {} casted entry point {}".format(device_name, address))
    reply = [id_frame, Frame(), Frame(bytes([0]))]
    sock.send_multipart(reply)

def get_addresses_handler(store: DirectoryServerStore, sock: Socket, argframes: List[Frame], id_frame: Frame) -> None:
    device_name = str(argframes.pop(0).bytes, encoding='utf8')
    if device_name in store.devices:
        device = store.devices[device_name]
        playload = json.dumps(device.cast_addresses)
        reply = [id_frame, Frame(), Frame(0), Frame(playload)]
        sock.send_multipart(reply)
    else:
        reply = [id_frame, Frame(), Frame(1)]
        sock.send_multipart(reply)

def file_list_handler(store: DirectoryServerStore, sock: Socket, id_frame: Frame) -> None:
    file_list = store.files.keys()
    reply = [id_frame, Frame(), Frame(0), Frame(json.dumps(file_list))]
    sock.send_multipart(reply)

def file_declare_handler(store: DirectoryServerStore, sock: Socket, argframes: List[Frame], id_frame: Frame, changes_pub: Socket) -> None:
    device_name = str(argframes.pop(0).bytes, encoding='utf8')
    filename = str(argframes.pop(0).bytes, encoding='utf8')
    new_file_flag = False
    if filename not in store.files:
        store.files[filename] = VirtualFile(filename, [], 0)
        print("New file {} created".format(filename))
        new_file_flag = True
    vfile = store.files[filename]
    device = store.devices.get(device_name, None)
    if device and (device not in vfile.declared_devices):
        vfile.declared_devices.append(device_name)
        if not device_name.startswith("storage"): # In reality we may use another way to identify if we need to count reference for the device
            vfile.refcount += 1
    sock.send_multipart([id_frame, Frame(), Frame(bytes([0]))])
    if new_file_flag:
        changes_pub.send_multipart([Frame(b"fs.new_file"), Frame(bytes(filename, 'utf8'))])
        print("fs.new_file is sent")

def file_disown_handler(store: DirectoryServerStore, sock: Socket, argframes: List[Frame], id_frame: Frame, changes_pub: Socket) -> None:
    device_name = str(argframes.pop(0).bytes, encoding='utf8')
    filename = str(argframes.pop(0).bytes, encoding='utf8')
    if (filename in store.files) and (device_name in store.devices):
        vfile = store.files[filename]
        device = store.devices[device_name]
        if device in vfile.declared_devices:
            vfile.declared_devices.remove(device)
            if not device_name.startswith("storage"):
                vfile.refcount -= 1
        if vfile.refcount == 0:
            store.files.pop(filename)
            changes_pub.send_multipart([Frame(b"fs.delete_file", Frame(filename))])
        sock.send_multipart([id_frame, Frame(), Frame(bytes([0]))])
    else:
        sock.send_multipart([id_frame, Frame(), Frame(bytes([1]))])

def file_get_handler(store: DirectoryServerStore, sock: Socket, argframes: List[Frame], id_frame: Frame) -> None:
    filename = str(argframes.pop(0).bytes, encoding='utf8')
    if filename in store.files:
        vfile = store.files[filename]
        device_names = list(map(lambda d: d.name, vfile.declared_devices))
        sock.send_multipart([id_frame, Frame(), Frame(bytes([0])), Frame(bytes(json.dumps(device_names), 'utf8'))])
    else:
        sock.send_multipart([id_frame, Frame(), Frame(bytes([1]))])

def directory_server(store: DirectoryServerStore, zmq_context: Context):
    # pylint: disable=no-member # These zmq.ROUTER and zmq.PUB must be actually exists
    print("Starting on libzmq {} with PyZMQ {}".format(zmq.zmq_version(), zmq.pyzmq_version()))
    entrypoint: Socket = zmq_context.socket(zmq.ROUTER)
    entrypoint.bind("tcp://127.0.0.1:5350") # This is just a PROTOTYPE!
    pub_file_changes: Socket = zmq_context.socket(zmq.PUB)
    pub_file_changes.bind("tcp://127.0.0.1:5351")
    poller = Poller()
    poller.register(entrypoint, flags=zmq.POLLIN)
    print("Directory server is started on 127.0.0.1:5350 (commands) and 127.0.0.1:5351 (file_changes_push)")
    while True:
        events: List[Tuple[Socket, int]] = poller.poll()
        for socket, _ in events:
            frames: List[Frame] = socket.recv_multipart(copy=False)
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
                get_addresses_handler(store, socket, frames, id_frame)
            elif command == 'fs.list':
                file_list_handler(store, socket, id_frame)
            elif command == 'fs.declare':
                file_declare_handler(store, socket, frames, id_frame, pub_file_changes)
            elif command == 'fs.disown':
                file_disown_handler(store, socket, frames, id_frame, pub_file_changes)
            elif command == 'fs.get':
                file_get_handler(store, socket, frames, id_frame)

def main():
    store = DirectoryServerStore()
    context = Context.instance()
    try:
        directory_server(store, context)
    except KeyboardInterrupt:
        context.destroy()
        print('')
    except BaseException as e:
        context.destroy()
        raise e

if __name__ == "__main__":
    main()
