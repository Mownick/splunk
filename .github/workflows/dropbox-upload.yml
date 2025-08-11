import os
import dropbox
import glob
import tarfile
import logging
from dropbox.exceptions import ApiError, AuthError

# Logging setup
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
            logging.info("Dropbox connection successful")
        except AuthError:
            logging.error("Invalid access token")
            raise

    def find_upload_file(self):
        """Find the newest .tar.gz file in repo"""
        files = glob.glob("*.tar.gz")
        if not files:
            raise FileNotFoundError("No .tar.gz files found in repository")
        return max(files, key=os.path.getmtime)

    def convert_to_tgz(self, tar_gz_file):
        """Convert .tar.gz to .tgz (same format, just extension change)"""
        tgz_file = tar_gz_file.replace('.tar.gz', '.tgz')
        
        # Just rename the file (no re-compression needed)
        if os.path.exists(tgz_file):
            os.remove(tgz_file)
        os.rename(tar_gz_file, tgz_file)
        return tgz_file

    def update_master_tar(self, tgz_file):
        """Update master tar file with the new .tgz file"""
        master_file = "Bots_V3_splunkapps.tar"
        
        # Download existing master file from Dropbox
        try:
            self.dbx.files_download_to_file(master_file, "/"+master_file)
        except ApiError as e:
            if not e.error.is_path() or not e.error.get_path().is_not_found():
                raise
            # Create new empty tar if doesn't exist
            with tarfile.open(master_file, "w:"):
                pass

        # Update the master tar
        with tarfile.open(master_file, "a:") as tar:
            file_name = os.path.basename(tgz_file)
            # Remove if already exists
            try:
                tar.getmember(file_name)
                self._remove_from_tar(tar, file_name)
            except KeyError:
                pass
            # Add the new file
            tar.add(tgz_file, arcname=file_name)

    def _remove_from_tar(self, tar, member_name):
        """Remove a file from tar archive"""
        temp_file = "temp_master.tar"
        with tarfile.open(temp_file, "w:") as new_tar:
            for member in tar.getmembers():
                if member.name != member_name:
                    new_tar.addfile(member, tar.extractfile(member))
        os.replace(temp_file, "Bots_V3_splunkapps.tar")

    def upload_to_dropbox(self):
        """Upload updated master file to Dropbox"""
        chunk_size = 8 * 1024 * 1024  # 8MB
        file_path = "Bots_V3_splunkapps.tar"
        file_size = os.path.getsize(file_path)
        
        with open(file_path, 'rb') as f:
            if file_size <= chunk_size:
                self.dbx.files_upload(
                    f.read(),
                    "/Bots_V3_splunkapps.tar",
                    mode=dropbox.files.WriteMode.overwrite
                )
            else:
                self._chunked_upload(f, file_size, chunk_size)
        
        logging.info("Master file updated successfully on Dropbox")

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
        
        # 1. Find the uploaded .tar.gz file
        tar_gz_file = uploader.find_upload_file()
        
        # 2. Convert to .tgz
        tgz_file = uploader.convert_to_tgz(tar_gz_file)
        
        # 3. Update master tar file
        uploader.update_master_tar(tgz_file)
        
        # 4. Upload to Dropbox
        uploader.upload_to_dropbox()
        
        logging.info("✅ Process completed successfully")
    
    except Exception as e:
        logging.error(f"❌ Process failed: {str(e)}")
        raise
