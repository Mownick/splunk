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

    def find_latest_tgz(self):
        """Find most recent .tgz file in current directory"""
        files = glob.glob("*.tgz")
        if not files:
            raise FileNotFoundError("No .tgz files found")
        latest = max(files, key=os.path.getmtime)
        logging.info(f"Found .tgz file: {latest}")
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
                # Create empty tar file if it doesn't exist
                with tarfile.open("Bots_V3_splunkapps.tar", "w:") as tar:
                    pass
                return
            logging.error(f"Failed to download file: {e}")
            raise

    def add_to_master_tar(self, tgz_file):
        """Add the .tgz file to the master tar file"""
        try:
            with tarfile.open("Bots_V3_splunkapps.tar", "a:") as master_tar:
                # Check if file already exists in tar
                member_name = os.path.basename(tgz_file)
                try:
                    master_tar.getmember(member_name)
                    # If exists, remove it first
                    self._remove_from_tar(master_tar, member_name)
                except KeyError:
                    pass  # File doesn't exist in tar yet
                
                # Add the new file
                master_tar.add(tgz_file, arcname=member_name)
                logging.info(f"Added {tgz_file} to master tar")
        except Exception as e:
            logging.error(f"Failed to update master tar: {str(e)}")
            raise

    def _remove_from_tar(self, tar, member_name):
        """Helper function to remove a file from tar"""
        # Create temporary tar without the member
        temp_tar_path = "temp_Bots_V3_splunkapps.tar"
        with tarfile.open(temp_tar_path, "w:") as temp_tar:
            for member in tar.getmembers():
                if member.name != member_name:
                    file_obj = tar.extractfile(member)
                    temp_tar.addfile(member, file_obj)
        
        # Replace original with temp
        os.remove("Bots_V3_splunkapps.tar")
        os.rename(temp_tar_path, "Bots_V3_splunkapps.tar")
        logging.info(f"Removed old version of {member_name} from master tar")

    def upload_master_file(self):
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
            
            logging.info("Successfully uploaded updated master file")
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
        
        # Step 1: Find the latest .tgz file
        tgz_file = uploader.find_latest_tgz()
        
        # Step 2: Download existing master file (or create new if doesn't exist)
        uploader.download_master_file()
        
        # Step 3: Add the .tgz file to master tar (replacing if exists)
        uploader.add_to_master_tar(tgz_file)
        
        # Step 4: Upload the updated master file back to Dropbox
        success = uploader.upload_master_file()
        
        if not success:
            raise RuntimeError("Upload failed")
            
        logging.info("✅ Backup completed successfully")
        
    except Exception as e:
        logging.error(f"❌ Backup failed: {str(e)}", exc_info=True)
        raise
