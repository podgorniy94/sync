import hashlib
import logging
import os
import shutil
import sys
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_log = logging.StreamHandler()
file_log = logging.FileHandler("sync.log")

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

console_log.setFormatter(formatter)
file_log.setFormatter(formatter)
logger.addHandler(console_log)
logger.addHandler(file_log)


def get_hash(file):
    BUF_SIZE = 65536
    sha256 = hashlib.sha256()
    with open(file, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()


def get_replica_src_path(root, obj, source, replica):
    source_path = os.path.join(root, obj)
    relative_path = os.path.relpath(source_path, source)
    replica_path = os.path.join(replica, relative_path)
    return source_path, replica_path


def get_replica_path(root, obj, source, replica):
    replica_path = get_replica_src_path(root, obj, source, replica)[1]
    return replica_path


def sync_folders(source_folder, replica_folder, purge=False):
    for root, dirs, files in os.walk(source_folder):
        for dir in dirs:
            replica_path = get_replica_path(root, dir, source_folder, replica_folder)

            if not os.path.exists(replica_path):
                if not purge:
                    os.makedirs(replica_path)
                    logging.info(f"Folder created: {replica_path}")
                else:
                    dir_path = os.path.join(root, dir)
                    shutil.rmtree(dir_path)
                    logging.info(f"Folder deleted: {replica_path}")

        for file in files:
            paths = get_replica_src_path(root, file, source_folder, replica_folder)
            source_path, replica_path = paths

            if not os.path.exists(replica_path):
                if not purge:
                    shutil.copy2(source_path, replica_path)
                    logger.info(f"File created: {replica_path}")
                else:
                    file_path = os.path.join(root, file)
                    os.remove(file_path)
                    logger.info(f"File removed: {replica_path}")
            elif get_hash(source_path) != get_hash(replica_path):
                shutil.copy2(source_path, replica_path)
                logger.info(f"File updated: {replica_path}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("python3 <source_folder> <replica_folder> <sleep_duration_seconds>")
        sys.exit(1)

    source_folder = sys.argv[1]
    replica_folder = sys.argv[2]
    sleep_duration = int(sys.argv[3])

    while True:
        sync_folders(source_folder, replica_folder)
        sync_folders(
            purge=True, source_folder=replica_folder, replica_folder=source_folder
        )
        time.sleep(sleep_duration)
