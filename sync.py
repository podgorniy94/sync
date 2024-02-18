import getpass
import hashlib
import logging
import os
import shutil
import sys
import time

# Creating and configuring logging into console and file
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
    """
    Returns the checksum value of a file.
    """
    BUF_SIZE = 65536
    sha256 = hashlib.sha256()
    with open(file, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()


def get_rep_src_path(root: str, obj: str, source: str, replica: str) -> tuple[str, str]:
    """
    Builds valid paths for copying and automatic cleanup.
    """
    source_path = os.path.join(root, obj)
    relative_path = os.path.relpath(source_path, source)
    replica_path = os.path.join(replica, relative_path)
    return source_path, replica_path


def get_replica_path(root: str, obj: str, source: str, replica: str) -> str:
    replica_path = get_rep_src_path(root, obj, source, replica)[1]
    return replica_path


def sync_folders(source_folder: str, replica_folder: str, purge: bool = False) -> None:
    """
    Copies files from the source and automatically performs cleanup in the replica directory for full synchronization.
    """
    for root, dirs, files in os.walk(source_folder):
        for dir in dirs:
            replica_path = get_replica_path(root, dir, source_folder, replica_folder)
            try:
                if not os.path.isdir(replica_path):
                    if not purge:
                        os.makedirs(replica_path)
                        logging.info(f"Folder created: {replica_path}")
                    else:
                        dir_path = os.path.join(root, dir)
                        shutil.rmtree(dir_path)
                        logging.info(f"Folder deleted: {dir_path}")
            except Exception as e:
                logging.error(f"Error processing directory '{dir}': {e}")

        for file in files:
            paths = get_rep_src_path(root, file, source_folder, replica_folder)
            source_path, replica_path = paths
            try:
                if not os.path.exists(replica_path):
                    if not purge:
                        shutil.copy2(source_path, replica_path)
                        logger.info(f"File created: {replica_path}")
                    else:
                        file_path = os.path.join(root, file)
                        os.remove(file_path)
                        logger.info(f"File removed: {file_path}")
                elif get_hash(source_path) != get_hash(replica_path):
                    shutil.copy2(source_path, replica_path)
                    logger.info(f"File updated: {replica_path}")
            except Exception as e:
                logging.error(f"Error processing file '{file}': {e}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("python3 <source_folder> <replica_folder> <sleep_duration_seconds>")
        sys.exit(1)

    source_folder = sys.argv[1]
    replica_folder = sys.argv[2]
    sleep_duration = int(sys.argv[3])

    try:
        while True:
            sync_folders(source_folder, replica_folder)
            sync_folders(
                purge=True, source_folder=replica_folder, replica_folder=source_folder
            )
            time.sleep(sleep_duration)
    except KeyboardInterrupt:
        username = getpass.getuser()
        logger.info(f"Script interrupted by {username}")
        sys.exit(0)
