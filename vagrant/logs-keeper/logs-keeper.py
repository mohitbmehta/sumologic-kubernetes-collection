#!/usr/bin/env python3
import logging
import os
import shutil
import sys
import time

USAGE="""
Log Keeper follows all symlinks in given directory and creates hardlinks to prevent data loss because of file rotation

For example for given directory structure:

tree /logs-keeper --inodes
logs-keeper
├── [5774569]  containers
│   └── [5774571]  example_file_in_containers.json -> /logs-keeper/docker/example_file.json
└── [5774570]  docker
    └── [5774571]  example_file.json

and <directory_path> set to /logs-keeper/containers, logs-keeper is going to create some additional directories:

tree /logs-keeper --inodes
logs-keeper
├── [5774569]  containers
│   ├── [5774571]  example_file_in_containers.json -> /logs-keeper/docker/example_file.json
│   └── [5774580]  sumologic
│       └── [5774581]  5774571
│           └── [5774571]  example_file.json -> /logs-keeper/docker/sumologic/5774571/1613145170/example_file_in_containers.json
└── [5774570]  docker
    ├── [5774571]  example_file.json
    └── [5774574]  sumologic
        └── [5774578]  5774571
            └── [5774579]  1613145170
                └── [5774571]  example_file_in_containers.json

It has been created sumologic directory in both places (where the symlink is and where the link target is).
In this directory another one is created and named with inode value. In the target directory additionaly
timestamped dir is created

Below rotation scenario is presented.

Moment of rotation:

tree /logs-keeper --inodes
logs-keeper
├── [5774569]  containers
│   ├── [5774571]  example_file_in_containers.json -> /logs-keeper/docker/example_file.json
│   └── [5774580]  sumologic
│       └── [5774581]  5774571
│           └── [5774571]  example_file.json -> /logs-keeper/docker/sumologic/5774571/1613145170/example_file_in_containers.json
└── [5774570]  docker
    ├── [5774572]  example_file.json
    ├── [5774571]  example_file.json.1
    └── [5774574]  sumologic
        └── [5774578]  5774571
            └── [5774579]  1613145170
                └── [5774571]  example_file_in_containers.json

Handling rotation by logs-keeper:

tree /logs-keeper --inodes
logs-keeper
├── [5774569]  containers
│   ├── [5774571]  example_file_in_containers.json -> /logs-keeper/docker/example_file.json
│   └── [5774580]  sumologic
│       |── [5774581]  5774571
│           └── [5774571]  example_file.json -> /logs-keeper/docker/sumologic/5774571/1613145170/example_file_in_containers.json
│       └── [5774576]  5774572
│           └── [5774572]  example_file.json -> /logs-keeper/docker/sumologic/5774572/1613145543/example_file_in_containers.json
└── [5774570]
    ├── [5774572]  example_file.json
    ├── [5774571]  example_file.json.1
    └── [5774574]  sumologic
        ├── [5774578]  5774571
        |   └── [5774579]  1613145170
        |       └── [5774571]  example_file_in_containers.json
        └── [5774573]  5774572
            └── [5774575]  1613145543
                └── [5774572]  example_file_in_containers.json

Usage:

KEEP_TIME=3600 ./logs-keeper <directory_path>
"""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger()
KEEP_TIME = int(os.getenv("KEEP_TIME_S", 120))
KEEP_DIRECTORY = 'sumologic'

def main(monitor_directory):
    while True:
        files = [
            os.path.abspath(os.path.join(monitor_directory, f))
                for f in os.listdir(monitor_directory)
                    if os.path.isfile(os.path.join(monitor_directory, f))
                ]
        for file_path in files:
            fm = FileMonitor(file_path)
            fm.link_file()

            fm.expire_files()
        
        sumo_dir = os.path.join(monitor_directory, KEEP_DIRECTORY)
        if not os.path.isdir(sumo_dir):
            os.mkdir(sumo_dir)
        inodes = os.listdir(sumo_dir)
        for inode in inodes:
            file = os.listdir(os.path.join(sumo_dir, inode))[0]
            path = os.readlink(os.path.join(sumo_dir, inode, file))

            if not os.path.isfile(path):
                log.info(f'Removing symlink to non-existing file: {inode}')
                shutil.rmtree(os.path.join(sumo_dir, inode))

        time.sleep(0.2)


class FileMonitor:
    def __init__(self, file_path):
        self._file_path = file_path
        self.filename = os.path.basename(self._file_path)
    
    @property
    def dst_path(self):
        return_value = self._file_path
        return_value_dirname = os.path.dirname(return_value)
        while os.path.islink(return_value):
            return_value = os.readlink(return_value)
    
            # Get abspath of the link
            if os.path.abspath(return_value) != return_value:
                return_value = os.path.abspath(os.path.join(return_value_dirname, return_value))

            return_value_dirname = os.path.dirname(return_value)

        return return_value
    
    @property
    def dst_dirname(self):
        return os.path.dirname(self.dst_path)
    
    @property
    def dst_sumo_dir(self):
        return os.path.join(self.dst_dirname, KEEP_DIRECTORY)
    
    @property
    def dst_inode_dir(self):
        return self.get_dst_inode_dir(str(self.inode))
    
    def dst_timestamp_dir(self):
        return os.path.join(self.dst_inode_dir, str(int(time.time())))
    
    @property
    def src_dirname(self):
        return os.path.dirname(self._file_path)

    @property
    def src_sumo_dir(self):
        return os.path.join(self.src_dirname, KEEP_DIRECTORY)

    @property
    def src_inode_dir(self):
        return self.get_src_inode_dir(str(self.inode))
    
    def get_dst_inode_dir(self, inode):
        return os.path.join(self.dst_sumo_dir, inode)

    def get_src_inode_dir(self, inode):
        return os.path.join(self.src_sumo_dir, inode)
    
    @property
    def inode(self):
        return os.stat(self.dst_path, follow_symlinks=True).st_ino
    
    def link_file(self):
        """
        self.file_path -> dirname(self.file_path)/<KEEP_DIRECTORY>/<inode>/<timestamp>/basename(self.file_path)

        Takes real path of the file (following symlinks) and create hardlink in subdirectory to it
        """
        if os.path.isdir(self.dst_inode_dir):
            # Skip already linked file
            return

        # Create hard link for inode
        log.info(f'Creating link for {self.inode}:{self.filename}')
        ts_dir = self.dst_timestamp_dir()
        os.makedirs(ts_dir, exist_ok=True)
        hardlink = os.path.join(ts_dir, self.filename)
        os.link(self.dst_path, hardlink)

        # Create symbolic link to hardlink
        os.makedirs(self.src_inode_dir)
        symlink = os.path.join(self.src_inode_dir, self.filename)
        os.symlink(hardlink, symlink)
    
    def expire_files(self):
        """
        Scan for files which already expired and remove them
        """
        # 1. Get all subdirectories names (inodes) from KEEP_DIRECTORY directory
        try:
            inodes = os.listdir(self.dst_sumo_dir)
        except:
            log.info(f'{self.dst_sumo_dir} doesn\'t exist')
            return

        # 2. For every inode check creation time (subdirectory name) and remove if it exists longer than KEEP_TIME
        for inode in inodes:
            if int(inode) == self.inode:
                # Skip not rotated file
                # ToDo: update not rotated inode timestamp
                log.debug(f'Skipping not rotated file: {inode}')
                continue

            current = self.get_dst_inode_dir(inode)

            # FixMe: Check if still needed
            try:
                timestamp = int(os.listdir(current)[0])
            except:
                timestamp = 0

            if timestamp + KEEP_TIME < time.time():
                log.info(f'Removing {inode}')
                try:
                    shutil.rmtree(current)
                except FileNotFoundError:
                    pass

                try:
                    shutil.rmtree(self.get_src_inode_dir(inode))
                except FileNotFoundError:
                    pass

if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(os.path.realpath(sys.argv[1]))
    else:
        print(USAGE)
