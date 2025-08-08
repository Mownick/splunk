# upload_to_dropbox.py
import os
import glob
import dropbox
from pathlib import Path

TOKEN = os.getenv("DROPBOX_ACCESS")
if not TOKEN:
    print("ERROR: DROPBOX_ACCESS not found in env.")
    raise SystemExit(1)

dbx = dropbox.Dropbox(TOKEN)
print("Connected to Dropbox account.")

# find all .tar.gz files in repo (recursive)
tar_files = [p for p in glob.glob("**/*.tar.gz", recursive=True)]

if not tar_files:
    print("No .tar.gz files found. Exiting.")
    raise SystemExit(0)

for f in tar_files:
    p = Path(f)
    if p.name.endswith(".tar.gz"):
        new_name = p.with_name(p.name[:-7] + ".tgz")  # convert name to .tgz
        # Rename in runner workspace (this does not change your GitHub repo)
        p.rename(new_name)
        print(f"Converted: {p} -> {new_name}")

        # Upload to Dropbox into folder /splunk_apps/ (create if not exists)
        dropbox_path = f"/splunk_apps/{new_name.name}"
        with open(new_name, "rb") as fh:
            data = fh.read()
        try:
            dbx.files_upload(data, dropbox_path, mode=dropbox.files.WriteMode.overwrite)
            print(f"Uploaded {new_name.name} to Dropbox at {dropbox_path}")
        except dropbox.exceptions.ApiError as e:
            print("Dropbox API error:", e)
            raise
