from flask import Flask, render_template, request, send_file
import os
from process_excel import process_excel
from github import Github

app = Flask(__name__, template_folder="../templates")

# Temporary folders — only /tmp is writable on Vercel
UPLOAD_FOLDER = "/tmp/uploads"
OUTPUT_FOLDER = "/tmp/outputs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ============ GitHub Release Setup ============
GITHUB_TOKEN = os.getenv("GITHUB_PAT")  # Set in Vercel Environment Variables
REPO_NAME = "diocletian53/EAP3"
RELEASE_TAG = "v1.0"

def get_release():
    """Get or create a GitHub release."""
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(REPO_NAME)
    try:
        return repo.get_release(RELEASE_TAG)
    except:
        return repo.create_git_release(tag=RELEASE_TAG, name=RELEASE_TAG, message="Initial release")

def get_github_asset_url(release, filename):
    for asset in release.get_assets():
        if asset.name == filename:
            return asset.browser_download_url
    return None

# ============ Routes ============
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        main_file = request.files.get("main_file")
        master_file = request.files.get("master_file")

        if not main_file or not master_file:
            return render_template("index.html", error="Please upload both files.")

        main_path = os.path.join(UPLOAD_FOLDER, main_file.filename)
        master_path = os.path.join(UPLOAD_FOLDER, master_file.filename)
        output_filename = f"Processed_{main_file.filename}"
        output_path = os.path.join(OUTPUT_FOLDER, output_filename)

        main_file.save(main_path)
        master_file.save(master_path)

        # Process Excel
        process_excel(main_path, master_path, output_path)

        # Upload to GitHub
        release = get_release()
        with open(output_path, "rb") as f:
            for asset in release.get_assets():
                if asset.name == output_filename:
                    asset.delete_asset()
            release.upload_asset(output_path)

        github_url = get_github_asset_url(release, output_filename)

        return render_template(
            "index.html",
            download_file=output_filename,
            github_message=f"✅ File uploaded to GitHub Release {RELEASE_TAG}!",
            github_url=github_url
        )

    return render_template("index.html")

@app.route("/download/<filename>")
def download_file(filename):
    path = os.path.join(OUTPUT_FOLDER, filename)
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    return "File not found!", 404
