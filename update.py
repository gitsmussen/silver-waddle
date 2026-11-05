#!/usr/bin/env python3

import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import Message
from email.utils import parsedate_to_datetime
from enum import Enum
from http.client import HTTPException
from pathlib import Path
from typing import TypeAlias, cast
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener
from urllib.response import addinfourl

# JSON types used by the NOAA feeds and the local JSONL archives.
JSONValue: TypeAlias = (
    str | int | float | bool | None | list["JSONValue"] | dict[str, "JSONValue"]
)
JSONRecord: TypeAlias = dict[str, JSONValue]

# --------------------------------------------------
# Configuration
# --------------------------------------------------

USER_AGENT = (
    "silver-waddle/1.0 "
    "(+https://github.com/gitsmussen/silver-waddle; "
    "contact: git@smussen.no)"
)

DATA_DIR = Path(
    os.environ.get(
        "DATA_DIR",
        Path(__file__).resolve().parent / "data",
    )
)

REQUEST_TIMEOUT = 30
MAX_ATTEMPTS = 2        # the first try plus a single retry
RETRY_DELAY = 10        # seconds to wait before the retry
MAX_RETRY_WAIT = 60     # give up if the server asks us to wait longer than this
REQUEST_GAP_SECONDS = 1

# Remembers ETag / Last-Modified per URL so unchanged files are not downloaded again.
STATE_FILE = DATA_DIR / "fetch-state.json"

ALERTS_ARCHIVE = "alerts.jsonl"
ALERTS_URL = "https://services.swpc.noaa.gov/products/alerts.json"
MAX_NEW_ALERTS = 500

KP_INDEX_FILENAME = "noaa-planetary-k-index.jsonl"
KP_INDEX_URL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
KP_INDEX_LEGACY_FILENAME = "noaa-planetary-k-index.json"

FILES: dict[str, str] = {
    "sgarf.txt":
        "https://services.swpc.noaa.gov/text/sgarf.txt",
    "weekly.txt":
        "https://services.swpc.noaa.gov/text/weekly.txt",
    "3-day-forecast.txt":
        "https://services.swpc.noaa.gov/text/3-day-forecast.txt",
    "3-day-geomag-forecast.txt":
        "https://services.swpc.noaa.gov/text/3-day-geomag-forecast.txt",
    "daily-geomagnetic-indices.txt":
        "https://services.swpc.noaa.gov/text/daily-geomagnetic-indices.txt",
    "discussion.txt":
        "https://services.swpc.noaa.gov/text/discussion.txt",
}

FILE_SIZE_LIMITS: dict[str, int] = {
    "sgarf.txt": 64 * 1024,
    "weekly.txt": 64 * 1024,
    "3-day-forecast.txt": 64 * 1024,
    "3-day-geomag-forecast.txt": 64 * 1024,
    "daily-geomagnetic-indices.txt": 64 * 1024,
    "discussion.txt": 64 * 1024,
    "noaa-planetary-k-index.json": 128 * 1024,
    "alerts.json": 256 * 1024,
}

# Result words written to log.txt for each file.
STATUS_UPDATED = "success"
STATUS_UNCHANGED = "unchanged"
STATUS_FAILED = "fail"


def updated_status(new_records: int | None = None) -> str:
    """'success', or 'success (+N new)' when we know how many records were added."""
    if new_records is None:
        return STATUS_UPDATED
    return f"{STATUS_UPDATED} (+{new_records} new)"


# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Redirect handling
# --------------------------------------------------

class NoRedirectHandler(HTTPRedirectHandler):
    """Reject every HTTP redirect instead of following it."""

    def redirect_request(
        self,
        req: Request,
        fp: addinfourl,
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> None:
        return None


HTTP_OPENER = build_opener(NoRedirectHandler())
REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}

# --------------------------------------------------
# Backup paths
# --------------------------------------------------

def backup_paths(filename: str) -> tuple[Path, Path, Path]:
    current = DATA_DIR / filename
    old = current.with_name(f"{current.stem}.old{current.suffix}")
    older = current.with_name(f"{current.stem}.older{current.suffix}")
    return current, old, older

# --------------------------------------------------
# Fetch state (ETag / Last-Modified)
# --------------------------------------------------

# Keyed by URL: {"etag": "...", "last_modified": "..."}
FETCH_STATE: dict[str, dict[str, str]] = {}


def load_fetch_state() -> dict[str, dict[str, str]]:
    """Read saved validators; a missing or broken file just means 'fetch everything'."""
    try:
        with STATE_FILE.open("r", encoding="utf-8") as file:
            raw: object = json.load(file)
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, UnicodeDecodeError, OSError) as error:
        logger.warning(
            "Could not read %s (%s); fetching everything fresh",
            STATE_FILE.name,
            error,
        )
        return {}

    if not isinstance(raw, dict):
        return {}

    state: dict[str, dict[str, str]] = {}
    for url, saved in raw.items():
        if isinstance(url, str) and isinstance(saved, dict):
            state[url] = {
                key: value
                for key, value in saved.items()
                if isinstance(key, str) and isinstance(value, str)
            }
    return state


def save_fetch_state() -> None:
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=STATE_FILE.parent,
            prefix=f".{STATE_FILE.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            json.dump(FETCH_STATE, temp_file, indent=2, sort_keys=True)
            temp_file.write("\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, STATE_FILE)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def conditional_headers(url: str, local_copy: Path) -> dict[str, str]:
    """Headers asking the server to answer 304 if nothing changed.

    Only used when we really have the data locally, so a deleted file or
    archive is always downloaded again.
    """
    if not local_copy.exists() or local_copy.stat().st_size == 0:
        return {}

    saved = FETCH_STATE.get(url, {})
    headers: dict[str, str] = {}
    if "etag" in saved:
        headers["If-None-Match"] = saved["etag"]
    if "last_modified" in saved:
        headers["If-Modified-Since"] = saved["last_modified"]
    return headers


def extract_validators(headers: Message) -> dict[str, str]:
    validators: dict[str, str] = {}
    etag = headers.get("ETag")
    if etag:
        validators["etag"] = etag
    last_modified = headers.get("Last-Modified")
    if last_modified:
        validators["last_modified"] = last_modified
    return validators


def remember_validators(url: str, validators: dict[str, str] | None) -> None:
    """Call this only after the downloaded data was processed successfully."""
    if validators:
        FETCH_STATE[url] = validators
    else:
        FETCH_STATE.pop(url, None)

# --------------------------------------------------
# Retry helpers
# --------------------------------------------------

def get_retry_after(error: HTTPError) -> float | None:
    headers = getattr(error, "headers", None)
    value = headers.get("Retry-After") if headers is not None else None

    if not value:
        return None

    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        retry_date = parsedate_to_datetime(value)
        if retry_date.tzinfo is None:
            retry_date = retry_date.replace(tzinfo=timezone.utc)
        return max(
            0.0,
            (retry_date - datetime.now(timezone.utc)).total_seconds(),
        )
    except (TypeError, ValueError, OverflowError):
        return None


def is_retryable(error: Exception) -> bool:
    """Only errors that may go away on their own are worth a retry."""
    if isinstance(error, HTTPError):
        # 429 and 5xx are temporary; 404, 403, redirects and so on are not.
        return error.code == 429 or error.code >= 500
    # Timeouts, dropped connections and cut-off responses.
    return isinstance(error, (OSError, HTTPException))

# --------------------------------------------------
# Hash comparison
# --------------------------------------------------

def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def files_identical(first: Path, second: Path) -> bool:
    if not first.exists() or not second.exists():
        return False
    if first.stat().st_size != second.stat().st_size:
        return False
    return file_hash(first) == file_hash(second)

# --------------------------------------------------
# Download and validation
# --------------------------------------------------

def validate_file(filename: str, temp_path: Path, max_size: int) -> bool:
    """Validate downloaded content before touching existing files."""
    if not temp_path.exists():
        logger.error("%s: Temporary file missing", filename)
        return False

    size = temp_path.stat().st_size

    if size == 0:
        logger.error("%s: Empty download rejected", filename)
        return False

    if size > max_size:
        logger.error(
            "%s: Download too large (%d bytes > %d bytes)",
            filename,
            size,
            max_size,
        )
        return False

    if filename.endswith(".json"):
        try:
            with temp_path.open("r", encoding="utf-8-sig") as file:
                json.load(file)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            logger.error("%s: Invalid JSON rejected: %s", filename, error)
            return False

    if filename.endswith(".txt"):
        try:
            content = temp_path.read_text(encoding="utf-8-sig")
        except UnicodeDecodeError as error:
            logger.error(
                "%s: Non-text or invalid UTF-8 download rejected: %s",
                filename,
                error,
            )
            return False

        if "\x00" in content:
            logger.error("%s: Binary content rejected", filename)
            return False

        prefix = content.lstrip()[:512].lower()
        if prefix.startswith("<!doctype html") or prefix.startswith("<html"):
            logger.error("%s: HTML response rejected", filename)
            return False

    return True


class FetchStatus(Enum):
    DOWNLOADED = "downloaded"
    NOT_MODIFIED = "not modified"
    FAILED = "failed"


@dataclass
class FetchResult:
    status: FetchStatus
    path: Path | None = None                     # temp file when DOWNLOADED
    validators: dict[str, str] | None = None     # ETag / Last-Modified from the server


def download_file(
    url: str,
    destination: Path,
    max_size: int,
    local_copy: Path,
) -> FetchResult:
    """Download to a temporary file without following redirects.

    destination is only used for naming the temp file and log lines.
    local_copy is what we already hold for this URL (the current file or
    the archive); conditional headers are only sent if it exists.
    """
    request_headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    request_headers.update(conditional_headers(url, local_copy))

    for attempt in range(1, MAX_ATTEMPTS + 1):
        temp_path: Path | None = None

        try:
            logger.info(
                "Downloading %s from %s (attempt %d/%d)",
                destination.name,
                url,
                attempt,
                MAX_ATTEMPTS,
            )

            request = Request(url, headers=request_headers)

            with HTTP_OPENER.open(request, timeout=REQUEST_TIMEOUT) as response:
                if not 200 <= response.status < 300:
                    raise RuntimeError(
                        f"Unexpected HTTP status: {response.status}"
                    )

                validators = extract_validators(response.headers)

                with tempfile.NamedTemporaryFile(
                    mode="wb",
                    dir=DATA_DIR,
                    prefix=f".{destination.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as temp_file:
                    temp_path = Path(temp_file.name)
                    bytes_written = 0

                    while True:
                        chunk = response.read(65536)
                        if not chunk:
                            break

                        bytes_written += len(chunk)
                        if bytes_written > max_size:
                            raise ValueError(
                                f"Downloaded file exceeds {max_size} bytes"
                            )

                        temp_file.write(chunk)

                    temp_file.flush()
                    os.fsync(temp_file.fileno())

            if not validate_file(destination.name, temp_path, max_size):
                raise ValueError("Downloaded file failed validation")

            logger.info(
                "%s downloaded and validated (%d bytes)",
                destination.name,
                temp_path.stat().st_size,
            )
            return FetchResult(FetchStatus.DOWNLOADED, temp_path, validators)

        except Exception as error:
            # urllib raises HTTPError for 304, but it is good news for us.
            if isinstance(error, HTTPError) and error.code == 304:
                logger.info("%s: Not modified (HTTP 304)", destination.name)
                return FetchResult(FetchStatus.NOT_MODIFIED)

            logger.warning(
                "%s download from %s failed (%s): %s",
                destination.name,
                url,
                type(error).__name__,
                error,
            )

            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

            if isinstance(error, HTTPError) and error.code in REDIRECT_STATUS_CODES:
                logger.error(
                    "%s: Redirect refused; redirects are not allowed",
                    destination.name,
                )

            # Most errors will not fix themselves, so stop right away.
            if not is_retryable(error):
                break

            if attempt >= MAX_ATTEMPTS:
                break

            retry_after = get_retry_after(error) if isinstance(error, HTTPError) else None

            if retry_after is not None and retry_after > MAX_RETRY_WAIT:
                logger.warning(
                    "%s: Retry-After is %.0f seconds (limit %d); giving up",
                    destination.name,
                    retry_after,
                    MAX_RETRY_WAIT,
                )
                break

            delay = max(RETRY_DELAY, retry_after or 0.0)
            logger.warning("Retrying %s in %.0f seconds", destination.name, delay)
            time.sleep(delay)

    logger.error("%s: Download failed", destination.name)
    return FetchResult(FetchStatus.FAILED)

# --------------------------------------------------
# JSONL archive helpers
# --------------------------------------------------

def load_json_records(path: Path) -> list[JSONRecord]:
    """Read and validate a JSON array containing object records."""
    with path.open("r", encoding="utf-8-sig") as file:
        raw_data: object = json.load(file)

    if not isinstance(raw_data, list):
        raise ValueError(f"{path.name} must contain a JSON array")

    records: list[JSONRecord] = []
    for item in raw_data:
        if not isinstance(item, dict) or not all(
            isinstance(key, str) for key in item
        ):
            raise ValueError(f"{path.name} contains a non-object record")
        records.append(cast(JSONRecord, item))

    return records


def read_jsonl_archive(path: Path) -> list[JSONRecord]:
    """Read an oldest-to-newest JSONL archive, rejecting malformed rows."""
    if not path.exists():
        return []

    records: list[JSONRecord] = []

    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                item: object = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"{path.name}, line {line_number}: invalid JSON"
                ) from error

            if not isinstance(item, dict) or not all(
                isinstance(key, str) for key in item
            ):
                raise ValueError(
                    f"{path.name}, line {line_number}: record must be an object"
                )

            records.append(cast(JSONRecord, item))

    return records


def append_jsonl_records(path: Path, records: list[JSONRecord]) -> None:
    """Append a complete batch; roll back the file length if writing fails."""
    if not records:
        return

    payload = "".join(
        json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
        for record in records
    ).encode("utf-8")

    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and path.stat().st_size:
        with path.open("rb") as existing:
            existing.seek(-1, os.SEEK_END)
            if existing.read(1) != b"\n":
                raise ValueError(f"{path.name} does not end with a newline")

    with path.open("a+b") as archive:
        original_size = archive.tell()
        try:
            archive.write(payload)
            archive.flush()
            os.fsync(archive.fileno())
        except OSError:
            archive.truncate(original_size)
            archive.flush()
            os.fsync(archive.fileno())
            raise


def write_jsonl_atomically(path: Path, records: list[JSONRecord]) -> None:
    """Create or replace a JSONL archive through a same-directory temp file."""
    temp_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)

            for record in records:
                temp_file.write(
                    json.dumps(record, separators=(",", ":"), ensure_ascii=False)
                    + "\n"
                )

            temp_file.flush()
            os.fsync(temp_file.fileno())

        os.replace(temp_path, path)

    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

# --------------------------------------------------
# Alerts archive handling
# --------------------------------------------------

def parse_issue_datetime(entry: JSONRecord) -> datetime:
    value = entry.get("issue_datetime")

    if not isinstance(value, str) or not value.strip():
        raise ValueError("Alert is missing a valid issue_datetime")

    try:
        issue_time = datetime.fromisoformat(value.strip())
    except ValueError as error:
        raise ValueError(f"Invalid issue_datetime: {value!r}") from error

    if issue_time.tzinfo is None:
        issue_time = issue_time.replace(tzinfo=timezone.utc)

    return issue_time.astimezone(timezone.utc)


def update_alerts_archive() -> str:
    """Append new NOAA alerts to the oldest-to-newest JSONL archive."""
    logger.info("Processing %s", ALERTS_ARCHIVE)

    archive_path = DATA_DIR / ALERTS_ARCHIVE

    result = download_file(
        ALERTS_URL,
        DATA_DIR / "alerts.json",
        FILE_SIZE_LIMITS["alerts.json"],
        archive_path,
    )

    if result.status is FetchStatus.FAILED:
        logger.error("%s: Existing archive left untouched", ALERTS_ARCHIVE)
        return STATUS_FAILED

    if result.status is FetchStatus.NOT_MODIFIED:
        logger.info("%s: Unchanged at the server", ALERTS_ARCHIVE)
        return STATUS_UNCHANGED

    temp_path = result.path
    if temp_path is None:
        return STATUS_FAILED

    try:
        remote_entries = load_json_records(temp_path)

        remote_with_times = [
            (parse_issue_datetime(entry), index, entry)
            for index, entry in enumerate(remote_entries)
        ]

        archive_entries = read_jsonl_archive(archive_path)

        if not archive_entries:
            if len(remote_entries) > MAX_NEW_ALERTS:
                logger.error(
                    "%s: Initial snapshot has %d alerts (maximum %d); skipping",
                    ALERTS_ARCHIVE,
                    len(remote_entries),
                    MAX_NEW_ALERTS,
                )
                return STATUS_FAILED

            append_jsonl_records(archive_path, list(reversed(remote_entries)))
            remember_validators(ALERTS_URL, result.validators)
            logger.info(
                "Created %s from remote snapshot (%d alerts)",
                ALERTS_ARCHIVE,
                len(remote_entries),
            )
            return updated_status(len(remote_entries))

        latest_archive_time = max(
            parse_issue_datetime(entry) for entry in archive_entries
        )

        new_entries = [
            (issue_time, entry)
            for issue_time, _index, entry in remote_with_times
            if issue_time > latest_archive_time
        ]

        new_entries.reverse()
        new_entries.sort(key=lambda item: item[0])

        if len(new_entries) > MAX_NEW_ALERTS:
            logger.error(
                "%s: Detected %d new alerts (maximum %d); skipping archive update",
                ALERTS_ARCHIVE,
                len(new_entries),
                MAX_NEW_ALERTS,
            )
            return STATUS_FAILED

        if not new_entries:
            remember_validators(ALERTS_URL, result.validators)
            logger.info("%s: No new alerts to append", ALERTS_ARCHIVE)
            return STATUS_UNCHANGED

        append_jsonl_records(
            archive_path,
            [entry for _issue_time, entry in new_entries],
        )
        remember_validators(ALERTS_URL, result.validators)

        logger.info("%s: Appended %d new alerts", ALERTS_ARCHIVE, len(new_entries))
        return updated_status(len(new_entries))

    except (json.JSONDecodeError, ValueError, TypeError, OSError) as error:
        logger.error("%s: Archive update failed: %s", ALERTS_ARCHIVE, error)
        return STATUS_FAILED

    finally:
        temp_path.unlink(missing_ok=True)

# --------------------------------------------------
# Planetary K-index archive handling
# --------------------------------------------------

def parse_k_index_time_tag(entry: JSONRecord) -> datetime:
    value = entry.get("time_tag")

    if not isinstance(value, str) or not value.strip():
        raise ValueError("K-index record is missing a valid time_tag")

    try:
        timestamp = datetime.fromisoformat(value.strip())
    except ValueError as error:
        raise ValueError(f"Invalid time_tag: {value!r}") from error

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)

    timestamp = timestamp.astimezone(timezone.utc)
    return timestamp.replace(minute=0, second=0, microsecond=0)


def migrate_k_index_archive(archive_path: Path) -> bool:
    """Seed JSONL from the legacy JSON snapshot once, if present."""
    if archive_path.exists():
        return True

    legacy_path = DATA_DIR / KP_INDEX_LEGACY_FILENAME
    if not legacy_path.exists():
        return True

    try:
        records = load_json_records(legacy_path)

        for record in records:
            parse_k_index_time_tag(record)

        write_jsonl_atomically(archive_path, records)
        logger.info("Migrated %s to %s", legacy_path.name, archive_path.name)
        return True

    except (json.JSONDecodeError, UnicodeDecodeError, ValueError, OSError) as error:
        logger.error("Could not migrate K-index archive: %s", error)
        return False


def update_k_index_archive() -> str:
    """Append K-index rows with a later date/hour to the JSONL archive."""
    archive_path = DATA_DIR / KP_INDEX_FILENAME

    if not migrate_k_index_archive(archive_path):
        return STATUS_FAILED

    logger.info("Processing %s", KP_INDEX_FILENAME)

    result = download_file(
        KP_INDEX_URL,
        DATA_DIR / KP_INDEX_LEGACY_FILENAME,
        FILE_SIZE_LIMITS[KP_INDEX_LEGACY_FILENAME],
        archive_path,
    )

    if result.status is FetchStatus.FAILED:
        logger.error("%s: Existing archive left untouched", KP_INDEX_FILENAME)
        return STATUS_FAILED

    if result.status is FetchStatus.NOT_MODIFIED:
        logger.info("%s: Unchanged at the server", KP_INDEX_FILENAME)
        return STATUS_UNCHANGED

    temp_path = result.path
    if temp_path is None:
        return STATUS_FAILED

    try:
        remote_entries = load_json_records(temp_path)

        remote_with_times = [
            (parse_k_index_time_tag(entry), index, entry)
            for index, entry in enumerate(remote_entries)
        ]

        archive_entries = read_jsonl_archive(archive_path)

        if not archive_entries:
            # Keep source order (oldest to newest) for an initial archive.
            append_jsonl_records(archive_path, remote_entries)
            remember_validators(KP_INDEX_URL, result.validators)
            logger.info(
                "Created %s from remote snapshot (%d records)",
                KP_INDEX_FILENAME,
                len(remote_entries),
            )
            return updated_status(len(remote_entries))

        archived_times = [
            parse_k_index_time_tag(entry) for entry in archive_entries
        ]

        if any(
            earlier > later
            for earlier, later in zip(archived_times, archived_times[1:])
        ):
            raise ValueError("K-index archive is not ordered oldest to newest")

        latest_archive_time = archived_times[-1]

        new_entries = [
            (timestamp, index, entry)
            for timestamp, index, entry in remote_with_times
            if timestamp > latest_archive_time
        ]

        new_entries.sort(key=lambda item: (item[0], item[1]))
        records_to_append = [entry for _timestamp, _index, entry in new_entries]

        if not records_to_append:
            remember_validators(KP_INDEX_URL, result.validators)
            logger.info("%s: No new records to append", KP_INDEX_FILENAME)
            return STATUS_UNCHANGED

        append_jsonl_records(archive_path, records_to_append)
        remember_validators(KP_INDEX_URL, result.validators)

        logger.info(
            "%s: Appended %d records",
            KP_INDEX_FILENAME,
            len(records_to_append),
        )
        return updated_status(len(records_to_append))

    except (json.JSONDecodeError, ValueError, TypeError, OSError) as error:
        logger.error("%s: Archive update failed: %s", KP_INDEX_FILENAME, error)
        return STATUS_FAILED

    finally:
        temp_path.unlink(missing_ok=True)

# --------------------------------------------------
# Backup rotation and installation
# --------------------------------------------------

def install_file(filename: str, temp_path: Path) -> bool:
    """Rotate backups and install a validated temporary file."""
    current, old, older = backup_paths(filename)

    try:
        if old.exists():
            logger.info("%s -> %s", old.name, older.name)
            os.replace(old, older)

        if current.exists():
            logger.info("%s -> %s", current.name, old.name)
            os.replace(current, old)

        os.replace(temp_path, current)
        logger.info("%s successfully updated", filename)
        return True

    except OSError as error:
        logger.error("%s: Installation failed: %s", filename, error)

        if not current.exists() and old.exists():
            try:
                shutil.copy2(old, current)
                logger.warning("%s: Restored previous current file", filename)
            except OSError as restore_error:
                logger.critical("%s: Restoration failed: %s", filename, restore_error)

        return False

    finally:
        temp_path.unlink(missing_ok=True)

# --------------------------------------------------
# Per-file update
# --------------------------------------------------

def update_file(filename: str, url: str, max_size: int) -> str:
    destination = DATA_DIR / filename

    logger.info("Processing %s", filename)

    result = download_file(url, destination, max_size, destination)

    if result.status is FetchStatus.FAILED:
        logger.error("%s: Existing files left untouched", filename)
        return STATUS_FAILED

    if result.status is FetchStatus.NOT_MODIFIED:
        logger.info("%s: Unchanged at the server", filename)
        return STATUS_UNCHANGED

    temp_path = result.path
    if temp_path is None:
        return STATUS_FAILED

    # Same content as what we already have: leave the files and backups alone.
    if files_identical(temp_path, destination):
        temp_path.unlink(missing_ok=True)
        remember_validators(url, result.validators)
        logger.info("%s: Content identical to current file; left unchanged", filename)
        return STATUS_UNCHANGED

    if install_file(filename, temp_path):
        remember_validators(url, result.validators)
        return STATUS_UPDATED

    return STATUS_FAILED

# --------------------------------------------------
# Update log
# --------------------------------------------------

def write_update_log(results: Mapping[str, str]) -> None:
    """Append one UTC-dated update summary to log.txt."""
    log_file = DATA_DIR / "log.txt"
    timestamp = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M:%S UTC")

    entries = [f"{filename}:{status}" for filename, status in results.items()]

    log_entry = f"{timestamp} - " + " - ".join(entries) + "\n"

    with log_file.open("a", encoding="utf-8") as file:
        file.write(log_entry)

    logger.info("Update log appended to %s", log_file)

# --------------------------------------------------
# Main
# --------------------------------------------------

def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Starting NOAA dataset update")
    logger.info("Data directory: %s", DATA_DIR)

    FETCH_STATE.clear()
    FETCH_STATE.update(load_fetch_state())

    results: dict[str, str] = {}
    requests_made = 0

    for filename, url in FILES.items():
        if requests_made:
            time.sleep(REQUEST_GAP_SECONDS)

        results[filename] = update_file(
            filename,
            url,
            FILE_SIZE_LIMITS[filename],
        )
        requests_made += 1

    for archive_name, update_archive in (
        (KP_INDEX_FILENAME, update_k_index_archive),
        (ALERTS_ARCHIVE, update_alerts_archive),
    ):
        if requests_made:
            time.sleep(REQUEST_GAP_SECONDS)

        results[archive_name] = update_archive()
        requests_made += 1

    # Validators are only stored for successfully processed data,
    # so this is safe to save even when some files failed.
    try:
        save_fetch_state()
    except OSError as error:
        logger.error("Could not save %s: %s", STATE_FILE.name, error)

    logger.info("=" * 50)
    logger.info("Weather data update summary")

    for filename, status in results.items():
        if status == STATUS_FAILED:
            logger.info("%s: FAILED - OLD DATA PRESERVED", filename)
        else:
            logger.info("%s: %s", filename, status.upper())

    try:
        write_update_log(results)
    except OSError as error:
        logger.error("Could not write update log: %s", error)

    logger.info("=" * 50)

    failed = [name for name, status in results.items() if status == STATUS_FAILED]

    if failed:
        logger.warning("One or more files were not updated: %s", ", ".join(failed))
        return 1

    logger.info("All NOAA spaceweather files are up to date.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

# Space weather data from NOAA SWPC.
# Local copies of the files are used when a live fetch fails.
