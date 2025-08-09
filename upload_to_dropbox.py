import os
import dropbox
import glob
import tarfile
import logging
from dropbox.exceptions import ApiError, AuthError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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

    def find_latest_tar_gz(self):
        """Find most recent .tar.gz file in current directory"""
        files = glob.glob("*.tar.gz")
        if not files:
            raise FileNotFoundError("No .tar.gz files found")
        latest = max(files, key=os.path.getmtime)
        logging.info(f"Found file: {latest}")
        return latest

    def convert_to_tgz(self, input_path):
        """Convert .tar.gz to .tgz format"""
        output_path = input_path.replace('.tar.gz', '.tgz')
        
        # Skip conversion if file already exists
        if os.path.exists(output_path):
            logging.info(f"TGZ file already exists: {output_path}")
            return output_path
            
        with tarfile.open(input_path, 'r:gz') as tar_in:
            with tarfile.open(output_path, 'w:gz') as tar_out:
                for member in tar_in.getmembers():
                    tar_out.addfile(member, tar_in.extractfile(member))
        logging.info(f"Converted to: {output_path}")
        return output_path

    def upload_file(self, local_path, dropbox_path):
        """Upload file with chunked upload for large files"""
        CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks
        
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
            
            logging.info(f"Successfully uploaded to: {dropbox_path}")
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
        
        # Step 1: Find file
        tar_file = uploader.find_latest_tar_gz()
        
        # Step 2: Convert format
        tgz_file = uploader.convert_to_tgz(tar_file)
        
        # Step 3: Upload
        dropbox_path = f"/Splunk_Backups/{os.path.basename(tgz_file)}"  # Change this path
        success = uploader.upload_file(tgz_file, dropbox_path)
        
        if not success:
            raise RuntimeError("Upload failed")
            
        logging.info("✅ Backup completed successfully")
        
    except Exception as e:
        logging.error(f"❌ Backup failed: {str(e)}", exc_info=True)
        raise
