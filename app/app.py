"""
    this file is part of tractor-prototype0
    Copyright (C) 2020 thisLight

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.

This is a smaple application.
As you can see, this most of this file is copied from storage server. It shows that the storage server actually is client in the design.

Usage:
* `python app.py declare testdata1.txt`: read `testdata1.txt` into a ramfs-like space and declare the app has it on directory server
* `python app.py disown testdata1.txt`: disown `testdata1.txt` on directory server
* `python app.py show testdata1.txt`: show the content of testdata1.txt on remote server
"""
import asyncio
import zmq
import json
from dataclasses import dataclass
from zmq import Frame
from zmq.asyncio import Socket, Context, Poller
from typing import List, Iterable, Dict, Tuple
from random import choice

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
    used_address = choice(all_declared_addresses)# we use only one connection to 'download' files here,
    # but a complete implementation must download them from different devices to speed up the process
    print("download_file(): using address {}".format(used_address))
    download_sock: Socket = context.socket(zmq.REQ)
    download_sock.connect(used_address)
    await download_sock.send_multipart([b"fs.read_file", bytes(filename, 'utf8')])
    frames: List[bytes] = await asyncio.wait_for(download_sock.recv_multipart(), 5)
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

async def read_file_handler(store: StorageServerStore, argframes: List[Frame], sock: Socket, id_frame: Frame):
    filename = str(argframes.pop(0).bytes, 'utf8')
    print("Read file {}".format(filename))
    vfile = store.files.get(filename, None)
    if vfile:
        sock.send_multipart([id_frame, Frame(), bytes([0]), vfile.content])
    else:
        sock.send(bytes([0]))

async def app(store: StorageServerStore, context: Context, name: str, command: str, arg: str):
    print("Starting...")
    dirserv_commands = context.socket(zmq.REQ)
    dirserv_commands.connect("tcp://127.0.0.1:5350")
    print("App is started")
    if command == "declare":
        self_addr = await asyncio.wait_for(ping(dirserv_commands, name), 5)
        print("Directory server report this client is run on {}".format(self_addr))
        command_port: Socket = context.socket(zmq.ROUTER)
        port = command_port.bind_to_random_port("tcp://127.0.0.1")
        self_entrypoint_addr = "tcp://{}:{}".format(self_addr, port)
        await asyncio.wait_for(cast_address(dirserv_commands, name, self_entrypoint_addr), 5)
        print("Address {} casted on directory server".format(self_entrypoint_addr))
        with open(arg, mode='rb') as f:
            store.files[arg] = VirtualFile(arg, f.read(), [])
        await declare_file(dirserv_commands, arg, name)
        print("File {} is declared, serving file...".format(arg))
        poller = Poller()
        poller.register(command_port, zmq.POLLIN)
        while True:
            events: List[Tuple[Socket, int]] = await poller.poll()
            for socket, mark in events:
                frames: List[Frame] = await socket.recv_multipart(copy=False)
                id_frame = frames.pop(0)
                frames.pop(0)
                command_frame = frames.pop(0)
                command = str(command_frame.bytes, 'utf8')
                if socket == command_port:
                    if command == 'fs.read_file':
                        await read_file_handler(store, frames, socket, id_frame)
    elif command == "disown":
        await disown_file(dirserv_commands, arg, name)
        context.destroy()
        return
    elif command == "show":
        content = await download_file(context, dirserv_commands, arg)
        print("==== Content of '{}' ====".format(arg))
        print(str(content, 'utf8'))
        context.destroy()
        return
    else:
        print("Unknown command {}".format(command))
        context.destroy()
        return
    


def main():
    import sys
    command = sys.argv[1]
    arg = sys.argv[2]
    store = StorageServerStore()
    context = Context()
    try:
        asyncio.run(app(store, context, "app", command, arg))
    except KeyboardInterrupt:
        context.destroy()
        print('')
    except BaseException as e:
        context.destroy()
        raise e

if __name__ == "__main__":
    main()
