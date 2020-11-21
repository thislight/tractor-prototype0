"""
    this file is part of tractor-prototype0
    Copyright (C) 2020 thisLight

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
     any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

This is Storage Server
Storage Servers are actually client as the view of Directory Server.
The only point is, Storage Servers should declare all the files they have on Directory Server.
NOTICE: Actually there is not "Storage Server", this is a name to identified to the other clients.
The mainly part of the implementation is the file protocol, for now it's unclear and i would use a very simple overall implementation (send the whole 'file' in one frame).
All at all it's just a prototype. ;)
In the complete implementation, other clients should be verified to get the files. I will not cover that in this prototype.
This implementation use zeromq, too.
Opened Port(s):
* ROUTER 5354 (command port)
Command(s):
* fs.read_file | filename: str -> 0 | content: bytes
"""
import asyncio
import zmq
import json
from dataclasses import dataclass
from zmq import Frame
from zmq.asyncio import Socket, Context, Poller
from typing import List, Iterable, Dict, Tuple

@dataclass
class VirtualFile(object):
    name: str
    content: bytes
    declared_device_names: List[str]

class StorageServerStore(object):
    def __init__(self) -> None:
        self.files: Dict[str, VirtualFile] = {}

async def ping(sock: Socket, device_name: str) -> str:
    await sock.send_multipart([Frame(b"ping"), Frame(bytes(device_name, encoding='utf8'))])
    frames: List[Frame] = await sock.recv_multipart(copy=False)
    command_frame = frames.pop(0)
    assert(command_frame.bytes == b"pong")
    addr_frame = frames.pop(0)
    return str(addr_frame, encoding='utf8')

async def cast_address(sock: Socket, device_name: str, addr: str) -> None:
    await sock.send_multipart([Frame(b"device.cast_address"), Frame(bytes(device_name, "utf8")), Frame(bytes(addr, 'utf8'))])
    result: bytes = await sock.recv()
    assert result[0] == 0

async def get_file_declared_devices(sock: Socket, filename: str) -> List[str]:
    await sock.send_multipart([b"fs.get", bytes(filename, 'utf8')])
    frames: List[bytes] = await sock.recv_multipart()
    assert(frames.pop(0)[0] == 0)
    device_list_frame = frames.pop(0)
    result = json.loads(device_list_frame)
    assert(isinstance(result, list))
    return result

async def get_devices_declared_addresses(sock: Socket, device_name: str) -> List[str]:
    await sock.send_multipart([b"device.get_addresses", bytes(device_name, 'utf8')])
    frames: List[bytes] = await sock.recv_multipart()
    assert(frames.pop(0)[0] == 0)
    device_addresses_frame = frames.pop(0)
    result = json.loads(device_addresses_frame)
    assert(isinstance(result, list))
    return result

async def download_file(context: Context, dirserv_sock: Socket, filename: str) -> bytes:
    devices = await get_file_declared_devices(dirserv_sock, filename)
    all_declared_addresses = []
    for dev_name in devices:
        addresses = await get_devices_declared_addresses(dirserv_sock, dev_name)
        all_declared_addresses += addresses
    used_address = all_declared_addresses[0] # we use only one connection to 'download' files here,
    # but a complete implementation must download them from different devices to speed up the process
    download_sock: Socket = context.socket(zmq.REQ)
    download_sock.connect(used_address)
    await download_sock.send_multipart([b"fs.read_file", bytes(filename, 'utf8')])
    frames: List[bytes] = await download_sock.recv_multipart()
    # This is just a sample protocol, and it does not need complex functions to deal with big contents
    download_sock.close()
    if frames[0][0] == 0:
        return frames.pop(1)
    else:
        return None

async def declare_file(sock: Socket, filename: str, device_name: str) -> None:
    await sock.send_multipart([b"fs.declare", bytes(device_name, 'utf8'), bytes(filename, 'utf8')])
    frames: List[bytes] = await sock.recv_multipart()
    assert(frames[0][0] == 0)

async def disown_file(sock: Socket, filename: str, device_name: str) -> None:
    await sock.send_multipart([b"fs.disown", bytes(device_name, 'utf8'), bytes(filename, 'utf8')])
    await sock.recv_multipart() # Eat result sliently

async def new_file_event_callback(store: StorageServerStore, argframes: List[Frame], dirserv_sock: Socket, context: Context, device_name: str) -> None:
    filename = str(argframes.pop(0).bytes, 'utf8')
    store.files[filename] = VirtualFile(filename, None, [])
    print("New virtual file '{}' added".format(filename))
    store.files[filename].content = await download_file(context, dirserv_sock, filename)
    await declare_file(dirserv_sock, filename, device_name)

async def delete_file_event_callback(store: StorageServerStore, argframes: List[Frame], dirserv_sock: Socket, device_name: str) -> None:
    filename = str(argframes.pop(0).bytes, 'utf8')
    store.files.pop(filename)
    await disown_file(dirserv_sock, filename, device_name)

async def read_file_handler(store: StorageServerStore, argframes: List[Frame], sock: Socket, id_frame: Frame):
    filename = str(argframes.pop(0).bytes, 'utf8')
    vfile = store.files.get(filename, None)
    if vfile:
        sock.send_multipart([id_frame, Frame(), bytes([0]), vfile.content])
    else:
        sock.send(bytes([0]))


async def storage_server(store: StorageServerStore, context: Context, name: str):
    print("Starting...")
    dirserv_commands = context.socket(zmq.REQ)
    dirserv_commands.connect("tcp://127.0.0.1:5350")
    self_addr = await asyncio.wait_for(ping(dirserv_commands, name), 5)
    print("Directory server report this client is run on {}".format(self_addr))
    self_entrypoint_addr = "tcp://{}:{}".format(self_addr, 5354)
    command_port = context.socket(zmq.ROUTER)
    command_port.bind("tcp://127.0.0.1:5354")
    await asyncio.wait_for(cast_address(dirserv_commands, name, self_entrypoint_addr), 5)
    print("Address {} casted on directory server".format(self_entrypoint_addr))
    file_changes_sub = context.socket(zmq.SUB)
    file_changes_sub.connect("tcp://127.0.0.1:5351")
    file_changes_sub.setsockopt(zmq.SUBSCRIBE, b"fs")
    poller = Poller()
    poller.register(file_changes_sub, zmq.POLLIN)
    poller.register(command_port, zmq.POLLIN)
    print("Storage server is started")
    while True:
        events: List[Tuple[Socket, int]] = await poller.poll()
        for socket, mark in events:
            frames: List[Frame] = await socket.recv_multipart(copy=False)
            if socket == command_port:
                id_frame = frames.pop(0)
                frames.pop(0)
                command_frame = frames.pop(0)
                command = str(command_frame.bytes, 'utf8')
                if command == 'fs.read_file':
                    await read_file_handler(store, frames, socket, id_frame)
            elif socket == file_changes_sub:
                command_frame = frames.pop(0)
                command = str(command_frame.bytes, 'utf8')
                print("File change received: {}".format(command))
                if command == 'fs.delete_file':
                    await delete_file_event_callback(store, frames, dirserv_commands, name)
                elif command == 'fs.new_file':
                    await new_file_event_callback(store, frames, dirserv_commands, context, name)


def main():
    import sys
    name = sys.argv[1]
    store = StorageServerStore()
    context = Context()
    try:
        asyncio.run(storage_server(store, context, "storage+" + name))
    except KeyboardInterrupt:
        context.destroy()
        print('')
    except BaseException as e:
        context.destroy()
        raise e

if __name__ == "__main__":
    main()
