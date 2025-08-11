import os
import glob
import tarfile
import logging
import tempfile
import sys
import dropbox
from dropbox.exceptions import ApiError, AuthError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='upload.log'
)

MASTER_NAME = "Bots_V3_splunkapps.tar"
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
            # check not_found
            try:
                if e.error.is_path() and e.error.get_path().is_not_found():
                    logging.info("Master file not found on Dropbox; will create new one locally.")
                    return False
            except Exception:
                pass
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

    def update_master_file(self, archive_file):
        """
        Rebuild master tar so that any existing entry with the same name is replaced.
        If master doesn't exist, create new tar and add the archive (stored as .tgz arcname).
        """
        arcname = self._make_arcname(archive_file)
        logging.info(f"Desired arcname inside master: {arcname}")

        if os.path.exists(MASTER_NAME):
            # Rebuild master into a temp tar: copy all entries except ones matching arcname
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tar")
            os.close(tmp_fd)
            try:
                with tarfile.open(MASTER_NAME, 'r:') as old_tar, \
                     tarfile.open(tmp_path, 'w:') as new_tar:
                    for member in old_tar.getmembers():
                        if member.name == arcname:
                            logging.info(f"Skipping existing entry {member.name} (will be replaced).")
                            continue
                        fobj = old_tar.extractfile(member)
                        if fobj is None:
                            # directory or special file
                            new_tar.addfile(member)
                        else:
                            new_tar.addfile(member, fobj)
                    # add the new archive file as a single file entry with arcname
                    new_tar.add(archive_file, arcname=arcname)
                # replace original master with rebuilt one
                os.replace(tmp_path, MASTER_NAME)
                logging.info(f"Rebuilt {MASTER_NAME} and added {arcname}")
            except Exception:
                # cleanup on error
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                logging.exception("Failed while rebuilding master tar")
                raise
        else:
            # Create new master and add the archive
            try:
                with tarfile.open(MASTER_NAME, 'w:') as master_tar:
                    master_tar.add(archive_file, arcname=arcname)
                logging.info(f"Created {MASTER_NAME} and added {arcname}")
            except Exception:
                logging.exception("Failed to create master tar")
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
                    self.dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
                else:
                    # start session
                    upload_session_start_result = self.dbx.files_upload_session_start(f.read(CHUNK_SIZE))
                    cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id, offset=f.tell())
                    commit = dropbox.files.CommitInfo(path=dropbox_path, mode=dropbox.files.WriteMode.overwrite)

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
        uploader.download_master_file()  # may return True/False, not used directly here
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
