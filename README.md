# Tractor-Prototype0
This is a showcase prototype for the network structure of tractor.

## Run
### Requirements
- libzmq 4.2 or later
- Python 3.6+ (you need to install `dataclasses` on pypi for Python 3.6)
- Programs are tested under Python 3.9.0 and Fedora 33
- pyzmq 20.0.0 (you can use the `requirements.txt` to install though pip, `pip install -r requirements.txt`)

### Project Structure
- `server/`: files for directory server
- `storage/`: files for storage "server"
- `app/`: files for a minial application to upload and download files

### Steps
The directory server is the centre of the network, then you need to one or more storage server to store files (in this storage server implementation just allow one instance).

1. (optional) create a virtual environment and use. `virtualenv venv && source venv/bin/activate`
2. install python-side requirements. `pip install -r requirements.txt`
3. run directory server. `python server/server.py`
4. run storage server (maybe you need a new terminal window). `python storage/storage.py`

Then open a new terminal window to use app: `cd app`
- `python app.py declare testdata1.txt`: read `testdata1.txt` into a ramfs-like space and declare the app has it on directory server
- `python app.py disown testdata1.txt`: disown `testdata1.txt` on directory server
- `python app.py show testdata1.txt`: show the content of testdata1.txt on remote server

## Notice
This prototype just a showcase for the powerful network design and it does not cover many keys in the complete design.

## LICENSE
````
    tractor-prototype0
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

````
Disclaim: The `testdata1.txt` under `app` is not a part of tractor-prototype0 and it is not a copyrighted content owned by anyone
