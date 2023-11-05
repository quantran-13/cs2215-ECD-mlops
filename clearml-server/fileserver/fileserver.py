""" A Simple file server for uploading and downloading files """
import json
import mimetypes
import os
import shutil
import urllib.parse
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path

from boltons.iterutils import first
from flask import Flask, request, send_from_directory, abort, Response
from flask_compress import Compress
from flask_cors import CORS
from werkzeug.exceptions import NotFound
from werkzeug.security import safe_join

from config import config
from utils import get_env_bool

log = config.logger(__file__)
DEFAULT_UPLOAD_FOLDER = "/mnt/fileserver"

app = Flask(__name__)
CORS(app, **config.get("fileserver.cors"))

if get_env_bool("CLEARML_COMPRESS_RESP", default=True):
    Compress(app)

app.config["UPLOAD_FOLDER"] = first(
    (os.environ.get(f"{prefix}_UPLOAD_FOLDER") for prefix in ("CLEARML", "TRAINS")),
    default=DEFAULT_UPLOAD_FOLDER,
)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = config.get(
    "fileserver.download.cache_timeout_sec", 5 * 60
)


@app.route("/", methods=["GET"])
def ping():
    return "OK", 200


@app.before_request
def before_request():
    if request.content_encoding:
        return f"Content encoding is not supported ({request.content_encoding})", 415


@app.after_request
def after_request(response):
    response.headers["server"] = config.get(
        "fileserver.response.headers.server", "clearml"
    )
    return response


@app.route("/", methods=["POST"])
def upload():
    results = []
    for filename, file in request.files.items():
        if not filename:
            continue
        file_path = filename.lstrip(os.sep)
        safe_path = safe_join(app.config["UPLOAD_FOLDER"], file_path)
        if safe_path is None:
            raise NotFound()
        target = Path(safe_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        file.save(str(target))
        results.append(file_path)

    log.info(f"Uploaded {len(results)} files")
    return json.dumps(results), 200


@app.route("/<path:path>", methods=["GET"])
def download(path):
    as_attachment = "download" in request.args

    _, encoding = mimetypes.guess_type(os.path.basename(path))
    mimetype = "application/octet-stream" if encoding == "gzip" else None

    response = send_from_directory(
        app.config["UPLOAD_FOLDER"],
        path,
        as_attachment=as_attachment,
        mimetype=mimetype,
    )
    if config.get("fileserver.download.disable_browser_caching", False):
        headers = response.headers
        headers["Pragma-directive"] = "no-cache"
        headers["Cache-directive"] = "no-cache"
        headers["Cache-control"] = "no-cache"
        headers["Pragma"] = "no-cache"
        headers["Expires"] = "0"

    log.info(f"Downloaded file {str(path)}")
    return response


def _get_full_path(path: str) -> Path:
    return Path(safe_join(os.fspath(app.config["UPLOAD_FOLDER"]), os.fspath(path)))


@app.route("/<path:path>", methods=["DELETE"])
def delete(path):
    real_path = _get_full_path(path)
    if not real_path.exists() or not real_path.is_file():
        log.error(f"Error deleting file {str(real_path)}. Not found or not a file")
        abort(Response(f"File {str(real_path)} not found", 404))

    real_path.unlink()

    log.info(f"Deleted file {str(real_path)}")
    return json.dumps(str(path)), 200


def batch_delete():
    body = request.get_json(force=True, silent=False)
    if not body:
        abort(Response("Json payload is missing", 400))
    files = body.get("files")
    if not files:
        abort(Response("files are missing", 400))

    deleted = {}
    errors = defaultdict(list)
    log_errors = defaultdict(list)

    def record_error(msg: str, file_, path_):
        errors[msg].append(str(file_))
        log_errors[msg].append(str(path_))

    for file in files:
        path = urllib.parse.unquote_plus(file)
        if not path or not path.strip("/"):
            # empty path may result in deleting all company data. Too dangerous
            record_error("Empty path not allowed", file, path)
            continue

        path = _get_full_path(path)

        if not path.exists():
            record_error("Not found", file, path)
            continue

        try:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                shutil.rmtree(path)
            else:
                record_error("Not a file or folder", file, path)
                continue
        except OSError as ex:
            record_error(ex.strerror, file, path)
            continue
        except Exception as ex:
            record_error(str(ex).replace(str(path), ""), file, path)
            continue

        deleted[file] = str(path)

    for error, paths in log_errors.items():
        log.error(f"{len(paths)} files/folders cannot be deleted due to the {error}")

    log.info(f"Deleted {len(deleted)} files/folders")
    return json.dumps({"deleted": deleted, "errors": errors}), 200


if config.get("fileserver.delete.allow_batch"):
    app.route("/delete_many", methods=["POST"])(batch_delete)


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--port", "-p", type=int, default=8081, help="Port (default %(default)d)"
    )
    parser.add_argument(
        "--ip", "-i", type=str, default="0.0.0.0", help="Address (default %(default)s)"
    )
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument(
        "--upload-folder",
        "-u",
        help=f"Upload folder (default {DEFAULT_UPLOAD_FOLDER})",
    )
    args = parser.parse_args()

    if args.upload_folder is not None:
        app.config["UPLOAD_FOLDER"] = args.upload_folder

    app.run(debug=args.debug, host=args.ip, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
