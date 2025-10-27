from flask import Flask, render_template, request, send_file
import os
from process_excel import process_excel
from github import Github

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# =========================
# GITHUB CONFIG
# =========================
GITHUB_TOKEN = os.getenv("GITHUB_PAT")  # Set your PAT as environment variable
REPO_NAME = "diocletian53/EAP"
RELEASE_TAG = "v1.0"

g = Github(GITHUB_TOKEN)
repo = g.get_repo(REPO_NAME)

try:
    release = repo.get_release(RELEASE_TAG)
except:
    release = repo.create_git_release(tag=RELEASE_TAG, name=RELEASE_TAG, message="Initial release")

# -------------------------
# Helper: Get GitHub asset URL
# -------------------------
def get_github_asset_url(filename):
    for asset in release.get_assets():
        if asset.name == filename:
            return asset.browser_download_url
    return None

# =========================
# ROUTES
# =========================
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        main_file = request.files.get("main_file")
        master_file = request.files.get("master_file")

        if not main_file or main_file.filename == "":
            return render_template("index.html", error="Please upload the main file!")
        if not master_file or master_file.filename == "":
            return render_template("index.html", error="Please upload the master file!")

        main_path = os.path.join(UPLOAD_FOLDER, main_file.filename)
        master_path = os.path.join(UPLOAD_FOLDER, master_file.filename)
        output_filename = "Processed_" + main_file.filename
        output_path = os.path.join(OUTPUT_FOLDER, output_filename)

        main_file.save(main_path)
        master_file.save(master_path)

        # Process Excel
        process_excel(main_path, master_path, output_path)

        # Upload to GitHub
        with open(output_path, "rb") as f:
            for asset in release.get_assets():
                if asset.name == os.path.basename(output_path):
                    asset.delete_asset()
            release.upload_asset(output_path)

        github_message = f"✅ File uploaded to GitHub Release {RELEASE_TAG}!"
        github_url = get_github_asset_url(output_filename)

        return render_template(
            "index.html",
            download_file=output_filename,
            github_message=github_message,
            github_url=github_url
        )

    return render_template("index.html")

@app.route("/download/<filename>")
def download_file(filename):
    path = os.path.join(OUTPUT_FOLDER, filename)
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    return "File not found!", 404

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
