import os
import dropbox
import glob
import tarfile
import logging
from dropbox.exceptions import ApiError, AuthError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='upload.log'
)

class DropboxUploader:
    def __init__(self, access_token):
        self.dbx = dropbox.Dropbox(access_token)
        self._verify_token()

    def _verify_token(self):
        """Verify the token has required permissions"""
        try:
            account = self.dbx.users_get_current_account()
            logging.info(f"Connected to Dropbox as: {account.name.display_name}")
        except AuthError as e:
            logging.error("Invalid access token or missing permissions")
            raise
        except Exception as e:
            logging.error(f"Connection failed: {str(e)}")
            raise

    def find_latest_archive(self):
        """Find most recent archive file in current directory"""
        files = glob.glob("*.tar.gz") + glob.glob("*.tgz") + glob.glob("*.tar")
        if not files:
            raise FileNotFoundError("No archive files found (*.tar.gz, *.tgz, *.tar)")
        latest = max(files, key=os.path.getmtime)
        logging.info(f"Found file: {latest}")
        return latest

    def download_master_file(self):
        """Download the existing master file from Dropbox"""
        try:
            with open("Bots_V3_splunkapps.tar", "wb") as f:
                metadata, res = self.dbx.files_download("/Bots_V3_splunkapps.tar")
                f.write(res.content)
            logging.info("Downloaded master file: Bots_V3_splunkapps.tar")
        except ApiError as e:
            if e.error.is_path() and e.error.get_path().is_not_found():
                logging.info("Master file doesn't exist yet, will create new one")
                return False
            logging.error(f"Failed to download file: {e}")
            raise
        return True

    def update_master_file(self, archive_file):
        """Update the master tar file with the new archive"""
        # If the file is a tar.gz or tgz, we'll add it as is
        # If it's a .tar, we'll extract and add its contents
        if archive_file.endswith('.tar'):
            with tarfile.open(archive_file, 'r:*') as src_tar:
                with tarfile.open("Bots_V3_splunkapps.tar", 'a:') as master_tar:
                    for member in src_tar.getmembers():
                        file_obj = src_tar.extractfile(member)
                        master_tar.addfile(member, file_obj)
                    logging.info(f"Added contents from: {archive_file}")
        else:
            with tarfile.open("Bots_V3_splunkapps.tar", 'a:') as master_tar:
                master_tar.add(archive_file, arcname=os.path.basename(archive_file))
                logging.info(f"Added archive: {archive_file}")

    def upload_file(self):
        """Upload the updated master file back to Dropbox"""
        CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
        local_path = "Bots_V3_splunkapps.tar"
        dropbox_path = "/Bots_V3_splunkapps.tar"
        
        try:
            file_size = os.path.getsize(local_path)
            
            with open(local_path, 'rb') as f:
                if file_size <= CHUNK_SIZE:
                    self.dbx.files_upload(
                        f.read(), 
                        dropbox_path, 
                        mode=dropbox.files.WriteMode.overwrite
                    )
                else:
                    upload_session = self.dbx.files_upload_session_start(f.read(CHUNK_SIZE))
                    cursor = dropbox.files.UploadSessionCursor(
                        session_id=upload_session.session_id,
                        offset=f.tell()
                    )
                    
                    while f.tell() < file_size:
                        remaining = file_size - f.tell()
                        chunk = f.read(min(CHUNK_SIZE, remaining))
                        self.dbx.files_upload_session_append_v2(chunk, cursor)
                        cursor.offset = f.tell()
                    
                    self.dbx.files_upload_session_finish(
                        b"", 
                        cursor, 
                        dropbox.files.CommitInfo(
                            path=dropbox_path,
                            mode=dropbox.files.WriteMode.overwrite
                        )
                    )
            
            logging.info(f"Successfully uploaded updated master file")
            return True
            
        except ApiError as e:
            logging.error(f"Dropbox API error: {e}")
            return False
        except Exception as e:
            logging.error(f"Upload failed: {str(e)}")
            return False

if __name__ == "__main__":
    try:
        ACCESS_TOKEN = os.environ.get("DROPBOX_ACCESS_TOKEN")
        if not ACCESS_TOKEN:
            raise ValueError("DROPBOX_ACCESS_TOKEN environment variable missing")

        uploader = DropboxUploader(ACCESS_TOKEN)
        
        # Step 1: Find the latest archive file
        archive_file = uploader.find_latest_archive()
        
        # Step 2: Download existing master file (if it exists)
        uploader.download_master_file()
        
        # Step 3: Update the master file with the new archive
        uploader.update_master_file(archive_file)
        
        # Step 4: Upload the updated master file back to Dropbox
        success = uploader.upload_file()
        
        if not success:
            raise RuntimeError("Upload failed")
            
        logging.info("✅ Backup completed successfully")
        
    except Exception as e:
        logging.error(f"❌ Backup failed: {str(e)}", exc_info=True)
        raise
