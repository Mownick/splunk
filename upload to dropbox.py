import os
import tarfile
import dropbox
import shutil
import sys
from datetime import datetime

# Dropbox settings
DROPBOX_MASTER_PATH = "/Bots_V3_splunkapps.tar"
TEMP_DIR = "temp_extract"

def log(message):
    """Print log message with timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def convert_to_tgz(input_file):
    """Convert .tar.gz → .tgz"""
    base_name = os.path.splitext(os.path.splitext(input_file)[0])[0]
    output_file = f"{base_name}.tgz"
    with tarfile.open(input_file, "r:gz") as tar:
        with tarfile.open(output_file, "w:gz") as out_tar:
            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f:
                    out_tar.addfile(member, f)
    log(f"Converted {input_file} → {output_file}")
    return output_file

def main():
    try:
        # 1️⃣ Detect pushed files
        pushed_files = []
        for root, _, files in os.walk("."):
            for file in files:
                if file.endswith((".tar.gz", ".tgz")) and file != "Bots_V3_splunkapps.tar":
                    pushed_files.append(os.path.join(root, file))

        if not pushed_files:
            log("❌ No .tar.gz or .tgz file found in the repository.")
            sys.exit(1)

        log(f"Found {len(pushed_files)} file(s) to process: {pushed_files}")

        processed_files = []
        for new_file_path in pushed_files:
            if new_file_path.endswith(".tar.gz"):
                new_file_path = convert_to_tgz(new_file_path)

            processed_files.append(new_file_path)

        # 2️⃣ Connect to Dropbox
        dbx = dropbox.Dropbox(os.environ["DROPBOX_TOKEN"])
        log("Connected to Dropbox.")

        # 3️⃣ Download master TAR
        log("Downloading master TAR from Dropbox...")
        with open("Bots_V3_splunkapps.tar", "wb") as f:
            metadata, res = dbx.files_download(DROPBOX_MASTER_PATH)
            f.write(res.content)
        log("Master TAR downloaded.")

        # 4️⃣ Extract master TAR
        os.makedirs(TEMP_DIR, exist_ok=True)
        with tarfile.open("Bots_V3_splunkapps.tar", "r") as tar:
            tar.extractall(TEMP_DIR)
        log("Master TAR extracted.")

        # 5️⃣ Replace/Add new files
        for new_file_path in processed_files:
            new_file_name = os.path.basename(new_file_path)
            target_path = os.path.join(TEMP_DIR, new_file_name)
            shutil.copy(new_file_path, target_path)
            log(f"✅ Added/Replaced {new_file_name} in master TAR.")

        # 6️⃣ Repack TAR
        with tarfile.open("Bots_V3_splunkapps.tar", "w") as tar:
            tar.add(TEMP_DIR, arcname="")
        log("Master TAR repacked.")

        # 7️⃣ Upload back to Dropbox
        with open("Bots_V3_splunkapps.tar", "rb") as f:
            dbx.files_upload(f.read(), DROPBOX_MASTER_PATH, mode=dropbox.files.WriteMode.overwrite)
        log("✅ Uploaded updated TAR to Dropbox.")

    except Exception as e:
        log(f"❌ ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
