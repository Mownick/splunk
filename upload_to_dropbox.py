import os
import sys
import tarfile
import tempfile
import dropbox
from dropbox.exceptions import AuthError, ApiError
from pathlib import Path

# Configuration
DROPBOX_FILE_PATH = '/Bots_V3_splunkapps.tar'
TEMP_DIR = tempfile.gettempdir()
MASTER_TAR_NAME = 'Bots_V3_splunkapps.tar'

def initialize_dropbox(access_token):
    """Initialize Dropbox client with access token."""
    try:
        dbx = dropbox.Dropbox(access_token)
        dbx.users_get_current_account()
        return dbx
    except AuthError:
        print("ERROR: Invalid Dropbox access token")
        sys.exit(1)

def download_from_dropbox(dbx):
    """Download the master tar file from Dropbox."""
    local_tar_path = os.path.join(TEMP_DIR, MASTER_TAR_NAME)
    try:
        dbx.files_download_to_file(local_tar_path, DROPBOX_FILE_PATH)
        return local_tar_path
    except ApiError as err:
        if err.error.is_path() and err.error.get_path().is_not_found():
            print("Master tar file not found in Dropbox, creating new one.")
            return None
        print(f"Error downloading from Dropbox: {err}")
        sys.exit(1)

def upload_to_dropbox(dbx, local_path):
    """Upload file to Dropbox."""
    try:
        with open(local_path, 'rb') as f:
            dbx.files_upload(f.read(), DROPBOX_FILE_PATH, mode=dropbox.files.WriteMode.overwrite)
        print(f"Successfully uploaded {local_path} to Dropbox")
    except Exception as e:
        print(f"Error uploading to Dropbox: {e}")
        sys.exit(1)

def convert_to_tgz_if_needed(file_path):
    """Convert .tar.gz to .tgz if needed, returns new path."""
    if file_path.endswith('.tar.gz'):
        new_path = file_path.replace('.tar.gz', '.tgz')
        with tarfile.open(file_path, 'r:gz') as tar:
            with tarfile.open(new_path, 'w:gz') as new_tar:
                for member in tar.getmembers():
                    file_obj = tar.extractfile(member)
                    new_tar.addfile(member, file_obj)
        os.remove(file_path)
        return new_path
    return file_path

def update_master_tar(master_tar_path, new_tgz_path):
    """Update master tar with new tgz file."""
    new_tgz_name = os.path.basename(new_tgz_path)
    temp_tar_path = os.path.join(TEMP_DIR, f"temp_{MASTER_TAR_NAME}")
    
    # Create new tar if it doesn't exist
    if not master_tar_path:
        with tarfile.open(temp_tar_path, 'w') as tar:
            tar.add(new_tgz_path, arcname=new_tgz_name)
        return temp_tar_path
    
    # Extract existing tar, add/replace file, then recreate
    with tempfile.TemporaryDirectory() as tmpdir:
        # Extract existing tar contents
        with tarfile.open(master_tar_path, 'r') as tar:
            tar.extractall(path=tmpdir)
        
        # Remove existing file if it exists
        existing_file = os.path.join(tmpdir, new_tgz_name)
        if os.path.exists(existing_file):
            os.remove(existing_file)
        
        # Copy new file into directory
        import shutil
        shutil.copy(new_tgz_path, os.path.join(tmpdir, new_tgz_name))
        
        # Create new tar file
        with tarfile.open(temp_tar_path, 'w') as tar:
            for file in os.listdir(tmpdir):
                tar.add(os.path.join(tmpdir, file), arcname=file)
    
    return temp_tar_path

def process_files(changed_files, access_token):
    """Main processing function."""
    dbx = initialize_dropbox(access_token)
    
    for file_path in changed_files.split(','):
        if not file_path.strip():
            continue
            
        file_path = file_path.strip()
        print(f"Processing file: {file_path}")
        
        # Step 1: Convert to .tgz if needed
        processed_path = convert_to_tgz_if_needed(file_path)
        tgz_name = os.path.basename(processed_path)
        
        # Step 2: Download existing master tar from Dropbox
        master_tar_path = download_from_dropbox(dbx)
        
        # Step 3: Update master tar with new file
        updated_tar_path = update_master_tar(master_tar_path, processed_path)
        
        # Step 4: Upload updated tar to Dropbox
        upload_to_dropbox(dbx, updated_tar_path)
        
        # Cleanup
        if master_tar_path and os.path.exists(master_tar_path):
            os.remove(master_tar_path)
        if os.path.exists(updated_tar_path):
            os.remove(updated_tar_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python upload_to_dropbox.py <changed_files>")
        sys.exit(1)
        
    access_token = os.getenv('DROPBOX_ACCESS_TOKEN')
    if not access_token:
        print("ERROR: Dropbox access token not found in environment variables")
        sys.exit(1)
        
    changed_files = sys.argv[1]
    process_files(changed_files, access_token)
