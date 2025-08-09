import os
import dropbox
import glob
import tarfile
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get Dropbox access token from environment variable
ACCESS_TOKEN = os.environ.get("DROPBOX_ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise EnvironmentError("DROPBOX_ACCESS_TOKEN not found in environment variables.")

def find_latest_tar_gz_file():
    """Find the most recently modified .tar.gz file in the repository."""
    files = glob.glob("*.tar.gz")
    if not files:
        logging.error("No .tar.gz files found in the repository.")
        raise FileNotFoundError("No .tar.gz files found.")
    return max(files, key=os.path.getmtime)

def convert_tar_gz_to_tgz(file_path):
    """Convert .tar.gz file to .tgz file."""
    new_file_path = file_path.replace('.tar.gz', '.tgz')
    with tarfile.open(file_path, 'r:gz') as tar:
        with tarfile.open(new_file_path, 'w:gz') as tgz:
            tgz.add(tar.getnames())
    logging.info(f"Converted {file_path} to {new_file_path}.")
    return new_file_path

def upload_file_to_dropbox(file_path, dropbox_path):
    """Upload the file to Dropbox, replacing if it already exists."""
    dbx = dropbox.Dropbox(ACCESS_TOKEN)

    try:
        # Check if the file already exists in Dropbox
        dbx.files_get_metadata(dropbox_path)
        logging.info(f"File {dropbox_path} already exists. Replacing it.")
        dbx.files_delete(dropbox_path)
    except dropbox.exceptions.ApiError as e:
        if e.error.is_path() and e.error.get_path().is_conflict():
            logging.info("File already exists, replacing it.")
        else:
            logging.error("Error checking file existence: %s", e)
            raise

    with open(file_path, "rb") as f:
        dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
    logging.info(f"Uploaded {file_path} to Dropbox at {dropbox_path}.")

if __name__ == "__main__":
    try:
        # Find the latest .tar.gz file
        tar_gz_file = find_latest_tar_gz_file()
        
        # Convert to .tgz
        tgz_file = convert_tar_gz_to_tgz(tar_gz_file)
        
        # Define the Dropbox path
        dropbox_path = f"/specific_folder/{os.path.basename(tgz_file)}"  # Change to your desired folder path
        
        # Upload the .tgz file to Dropbox
        upload_file_to_dropbox(tgz_file, dropbox_path)
    except Exception as e:
        logging.error("An error occurred: %s", e)
