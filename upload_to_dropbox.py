import os
import dropbox
import glob
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
        try:
            self.dbx.users_get_current_account()
            logging.info("Connected to Dropbox successfully")
        except AuthError:
            logging.error("Invalid access token")
            raise

    def find_tar_gz_file(self):
        """Find the newest .tar.gz file in repo"""
        files = glob.glob("*.tar.gz")
        if not files:
            raise FileNotFoundError("No .tar.gz files found")
        return max(files, key=os.path.getmtime)

    def convert_to_tgz(self, tar_gz_file):
        """Convert .tar.gz to .tgz by renaming"""
        tgz_file = tar_gz_file.replace('.tar.gz', '.tgz')
        if os.path.exists(tgz_file):
            os.remove(tgz_file)
        os.rename(tar_gz_file, tgz_file)
        logging.info(f"Converted {tar_gz_file} to {tgz_file}")
        return tgz_file

    def update_master_tar(self, tgz_file):
        """Update master tar with the new .tgz file"""
        master_file = "Bots_V3_splunkapps.tar"
        
        # Download existing master file
        try:
            self.dbx.files_download_to_file(master_file, f"/{master_file}")
            logging.info("Downloaded existing master file")
        except ApiError as e:
            if not e.error.is_path() or not e.error.get_path().is_not_found():
                raise
            logging.info("Creating new master file")
            with open(master_file, 'wb') as f:
                pass

        # Create temporary tar file
        temp_file = "temp_master.tar"
        with open(temp_file, 'wb') as temp:
            # Copy existing contents (except our file)
            if os.path.exists(master_file):
                with open(master_file, 'rb') as original:
                    temp.write(original.read())
            
            # Add new file
            with open(tgz_file, 'rb') as new_file:
                # Simple append (for demo - use tarfile for proper handling)
                temp.write(new_file.read())
        
        # Replace old with new
        os.replace(temp_file, master_file)
        logging.info(f"Added {tgz_file} to master tar")

    def upload_master_tar(self):
        """Upload master tar to Dropbox"""
        chunk_size = 8 * 1024 * 1024  # 8MB
        file_path = "Bots_V3_splunkapps.tar"
        
        with open(file_path, 'rb') as f:
            file_size = os.path.getsize(file_path)
            
            if file_size <= chunk_size:
                self.dbx.files_upload(
                    f.read(),
                    f"/{file_path}",
                    mode=dropbox.files.WriteMode.overwrite
                )
            else:
                self._chunked_upload(f, file_size, chunk_size)
        
        logging.info("Master file uploaded successfully")

    def _chunked_upload(self, file_obj, file_size, chunk_size):
        """Handle large file uploads"""
        session = self.dbx.files_upload_session_start(file_obj.read(chunk_size))
        cursor = dropbox.files.UploadSessionCursor(
            session_id=session.session_id,
            offset=file_obj.tell()
        )
        
        while file_obj.tell() < file_size:
            remaining = file_size - file_obj.tell()
            chunk = file_obj.read(min(chunk_size, remaining))
            self.dbx.files_upload_session_append_v2(chunk, cursor)
            cursor.offset = file_obj.tell()
        
        self.dbx.files_upload_session_finish(
            b"", cursor,
            dropbox.files.CommitInfo(
                path="/Bots_V3_splunkapps.tar",
                mode=dropbox.files.WriteMode.overwrite
            )
        )

if __name__ == "__main__":
    try:
        uploader = DropboxUploader(os.environ["DROPBOX_ACCESS_TOKEN"])
        
        # 1. Find uploaded .tar.gz
        tar_gz = uploader.find_tar_gz_file()
        
        # 2. Convert to .tgz
        tgz_file = uploader.convert_to_tgz(tar_gz)
        
        # 3. Update master tar
        uploader.update_master_tar(tgz_file)
        
        # 4. Upload to Dropbox
        uploader.upload_master_tar()
        
        logging.info("✅ Process completed successfully")
    
    except Exception as e:
        logging.error(f"❌ Process failed: {str(e)}")
        raise
