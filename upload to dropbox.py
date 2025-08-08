import os
import tarfile
import dropbox
import shutil

# ✅ CHANGE THIS TO YOUR PUSHED FILE NAME (if you want fixed file)
# If you want to auto-detect any pushed file, leave it as None
FIXED_FILE_NAME = None  # Example: "splunklab-PROD.tar.gz"

# Dropbox settings
DROPBOX_MASTER_PATH = "/Bots_V3_splunkapps.tar"
TEMP_DIR = "temp_extract"

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
    print(f"Converted {input_file} → {output_file}")
    return output_file

def main():
    # 1️⃣ Detect pushed file
    pushed_files = []
    for root, _, files in os.walk("."):
        for file in files:
            if file.endswith((".tar.gz", ".tgz")) and file != "Bots_V3_splunkapps.tar":
                pushed_files.append(os.path.join(root, file))

    if not pushed_files:
        print("No .tar.gz or .tgz file found.")
        return

    if FIXED_FILE_NAME:
        new_file_path = [f for f in pushed_files if FIXED_FILE_NAME in f][0]
    else:
        new_file_path = pushed_files[0]

    # 2️⃣ Convert if .tar.gz
    if new_file_path.endswith(".tar.gz"):
        new_file_path = convert_to_tgz(new_file_path)

    new_file_name = os.path.basename(new_file_path)

    # 3️⃣ Connect to Dropbox
    dbx = dropbox.Dropbox(os.environ["DROPBOX_TOKEN"])

    # 4️⃣ Download master TAR
    print("Downloading master TAR from Dropbox...")
    with open("Bots_V3_splunkapps.tar", "wb") as f:
        metadata, res = dbx.files_download(DROPBOX_MASTER_PATH)
        f.write(res.content)

    # 5️⃣ Extract master TAR
    os.makedirs(TEMP_DIR, exist_ok=True)
    with tarfile.open("Bots_V3_splunkapps.tar", "r") as tar:
        tar.extractall(TEMP_DIR)

    # 6️⃣ Replace/Add new file
    target_path = os.path.join(TEMP_DIR, new_file_name)
    shutil.copy(new_file_path, target_path)
    print(f"Added/Replaced {new_file_name} in master TAR.")

    # 7️⃣ Repack TAR
    with tarfile.open("Bots_V3_splunkapps.tar", "w") as tar:
        tar.add(TEMP_DIR, arcname="")

    # 8️⃣ Upload back to Dropbox
    with open("Bots_V3_splunkapps.tar", "rb") as f:
        dbx.files_upload(f.read(), DROPBOX_MASTER_PATH, mode=dropbox.files.WriteMode.overwrite)
    print("Uploaded updated TAR to Dropbox.")

if __name__ == "__main__":
    main()
