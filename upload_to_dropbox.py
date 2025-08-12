import os
import glob
import tarfile
import logging
import tempfile
import sys
import time
import dropbox
from dropbox.exceptions import ApiError, AuthError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='upload.log'
)

MASTER_NAME = "Bots_V3_splunkapps.tgz"
MASTER_PATH = os.path.abspath(MASTER_NAME)

class DropboxUploader:
    def __init__(self, access_token):
        self.dbx = dropbox.Dropbox(access_token)
        self._verify_token()

    def _verify_token(self):
        try:
            account = self.dbx.users_get_current_account()
            logging.info(f"Connected to Dropbox as: {account.name.display_name}")
        except AuthError:
            logging.error("Invalid access token or missing permissions")
            raise
        except Exception as e:
            logging.error(f"Connection failed: {str(e)}")
            raise

    def find_latest_archive(self):
        """Find most recent .tar.gz or .tgz in cwd, excluding master tar itself."""
        patterns = ["*.tar.gz", "*.tgz"]
        files = []
        for p in patterns:
            files.extend(glob.glob(p))
        # Exclude master file and this script itself
        files = [f for f in files if os.path.abspath(f) != MASTER_PATH and os.path.basename(f) != os.path.basename(__file__)]
        if not files:
            raise FileNotFoundError("No archive files found (*.tar.gz, *.tgz)")
        latest = max(files, key=os.path.getmtime)
        logging.info(f"Found file: {latest}")
        return latest

    def download_master_file(self):
        """Download existing master file from Dropbox if present."""
        try:
            metadata, res = self.dbx.files_download(f"/{MASTER_NAME}")
            with open(MASTER_NAME, "wb") as f:
                f.write(res.content)
            logging.info("Downloaded master file from Dropbox.")
            return True
        except ApiError as e:
            if e.error.is_path() and e.error.get_path().is_not_found():
                logging.info("Master file not found on Dropbox; will create new one locally.")
                return False
            logging.error(f"Failed to download master file: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error while downloading master file: {e}")
            raise

    def _make_arcname(self, archive_file):
        """Return desired arcname inside master tar: convert .tar.gz -> .tgz"""
        base = os.path.basename(archive_file)
        if base.endswith(".tar.gz"):
            return base[:-7] + ".tgz"
        return base

    def _convert_to_tgz_if_needed(self, archive_file):
        """Convert .tar.gz to .tgz format if needed, returns path to final file"""
        if archive_file.endswith(".tar.gz"):
            new_name = archive_file[:-7] + ".tgz"
            os.rename(archive_file, new_name)
            logging.info(f"Renamed {archive_file} to {new_name}")
            return new_name
        return archive_file

    def update_master_file(self, archive_file):
        """
        Rebuild master tar with updated timestamps.
        - Converts .tar.gz to .tgz
        - Replaces existing versions
        - Updates all timestamps to current time
        """
        archive_file = self._convert_to_tgz_if_needed(archive_file)
        arcname = self._make_arcname(archive_file)
        current_time = time.time()
        
        if os.path.exists(MASTER_NAME):
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tar")
            os.close(tmp_fd)
            try:
                with tarfile.open(MASTER_NAME, 'r:') as old_tar, \
                     tarfile.open(tmp_path, 'w:') as new_tar:
                    
                    # Set modification time for the archive
                    new_tar.mtime = current_time
                    
                    for member in old_tar.getmembers():
                        if member.name == arcname:
                            logging.info(f"Replacing existing {member.name}")
                            continue
                        fobj = old_tar.extractfile(member)
                        new_tar.addfile(member, fobj)
                    
                    # Add new file with current timestamp
                    new_tar.add(archive_file, arcname=arcname)
                
                # Replace original and update timestamps
                os.replace(tmp_path, MASTER_NAME)
                os.utime(MASTER_NAME, (current_time, current_time))
                logging.info(f"Updated {MASTER_NAME} with {arcname}")
                
            except Exception as e:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                logging.error(f"Failed to update master file: {str(e)}")
                raise
        else:
            try:
                with tarfile.open(MASTER_NAME, 'w:') as master_tar:
                    master_tar.mtime = current_time
                    master_tar.add(archive_file, arcname=arcname)
                os.utime(MASTER_NAME, (current_time, current_time))
                logging.info(f"Created new {MASTER_NAME} with {arcname}")
            except Exception as e:
                logging.error(f"Failed to create master file: {str(e)}")
                raise

    def upload_file(self):
        """Upload master tar to Dropbox with chunked upload if large."""
        CHUNK_SIZE = 8 * 1024 * 1024
        local_path = MASTER_NAME
        dropbox_path = f"/{MASTER_NAME}"

        if not os.path.exists(local_path):
            logging.error(f"Local master file not found: {local_path}")
            raise FileNotFoundError(local_path)

        file_size = os.path.getsize(local_path)
        try:
            with open(local_path, 'rb') as f:
                if file_size <= CHUNK_SIZE:
                    self.dbx.files_upload(
                        f.read(), 
                        dropbox_path, 
                        mode=dropbox.files.WriteMode.overwrite,
                        client_modified=datetime.now(timezone.utc)
                    )
                else:
                    upload_session_start_result = self.dbx.files_upload_session_start(f.read(CHUNK_SIZE))
                    cursor = dropbox.files.UploadSessionCursor(
                        session_id=upload_session_start_result.session_id,
                        offset=f.tell())
                    commit = dropbox.files.CommitInfo(
                        path=dropbox_path,
                        mode=dropbox.files.WriteMode.overwrite,
                        client_modified=datetime.now(timezone.utc))
                    
                    while f.tell() < file_size:
                        bytes_left = file_size - f.tell()
                        if bytes_left <= CHUNK_SIZE:
                            self.dbx.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
                        else:
                            self.dbx.files_upload_session_append_v2(f.read(CHUNK_SIZE), cursor)
                            cursor.offset = f.tell()
            logging.info(f"Successfully uploaded {MASTER_NAME} to Dropbox.")
            return True
        except ApiError as e:
            logging.error(f"Dropbox API error: {e}")
            return False
        except Exception as e:
            logging.exception(f"Upload failed: {e}")
            return False

def main():
    try:
        ACCESS_TOKEN = os.environ.get("DROPBOX_ACCESS_TOKEN")
        if not ACCESS_TOKEN:
            raise ValueError("DROPBOX_ACCESS_TOKEN environment variable missing")

        uploader = DropboxUploader(ACCESS_TOKEN)

        archive_file = uploader.find_latest_archive()
        uploader.download_master_file()
        uploader.update_master_file(archive_file)
        success = uploader.upload_file()
        if not success:
            raise RuntimeError("Upload failed")
        logging.info("✅ Backup completed successfully")
        print("✅ Backup completed successfully")
        return 0
    except Exception as e:
        logging.exception(f"❌ Backup failed: {e}")
        print(f"❌ Backup failed: {e}", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
