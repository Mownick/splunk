import os
import dropbox
import glob

ACCESS_TOKEN = os.environ.get("DROPBOX_ACCESS_TOKEN")

if not ACCESS_TOKEN:
    raise EnvironmentError("DROPBOX_ACCESS_TOKEN not found in environment variables.")

def find_file():
    # Find the latest .tgz or .tar.gz file in repo
    files = glob.glob("*.tgz") + glob.glob("*.tar.gz")
    if not files:
        raise FileNotFoundError("No .tgz or .tar.gz files found in repository.")
    # Pick the most recently modified file
    return max(files, key=os.path.getmtime)

def upload_file(file_path):
    dbx = dropbox.Dropbox(ACCESS_TOKEN)
    with open(file_path, "rb") as f:
        dbx.files_upload(f.read(), f"/{os.path.basename(file_path)}",
                         mode=dropbox.files.WriteMode.overwrite)
    print(f"✅ Uploaded {file_path} to Dropbox")

if __name__ == "__main__":
    file_to_upload = find_file()
    print(f"Found file: {file_to_upload}")
    upload_file(file_to_upload)
