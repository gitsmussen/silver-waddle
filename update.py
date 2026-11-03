#!/usr/bin/env python3

import json
import logging
import os
import shutil
import tempfile
import time

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


# --------------------------------------------------
# Configuration
# --------------------------------------------------

USER_AGENT = (
    "silver-waddle/1.0 "
    "(+https://github.com/gitsmussen/silver-waddle; "
    "contact: git@smussen.no)"
)

DATA_DIR = Path(
    os.environ.get("DATA_DIR", Path(__file__).resolve().parent)
)

REQUEST_TIMEOUT = 30
MAX_ATTEMPTS = 4
INITIAL_BACKOFF = 60
MAX_BACKOFF = 300
MAX_RETRY_WAIT = 300
REQUEST_GAP_SECONDS = 1

ALLOWED_REDIRECT_HOSTS = {"services.swpc.noaa.gov"}
ALERTS_ARCHIVE = "alerts.jsonl"
ALERTS_URL = "https://services.swpc.noaa.gov/products/alerts.json"
MAX_NEW_ALERTS = 10

FILES = {
    "sgarf.txt":
        "https://services.swpc.noaa.gov/text/sgarf.txt",

    "weekly.txt":
        "https://services.swpc.noaa.gov/text/weekly.txt",

    "3-dayforecast.txt":
        "https://services.swpc.noaa.gov/text/3-day-forecast.txt",

    "3-day-geomag-forecast.txt":
        "https://services.swpc.noaa.gov/text/3-day-geomag-forecast.txt",

    "daily-geomagnetic-indices.txt":
        "https://services.swpc.noaa.gov/text/daily-geomagnetic-indices.txt",

    "discussion.txt":
        "https://services.swpc.noaa.gov/text/discussion.txt",

    "noaa-planetary-k-index.json":
        "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json",
}

FILE_SIZE_LIMITS = {
    "sgarf.txt": 64 * 1024,
    "weekly.txt": 64 * 1024,
    "3-dayforecast.txt": 64 * 1024,
    "3-day-geomag-forecast.txt": 64 * 1024,
    "daily-geomagnetic-indices.txt": 64 * 1024,
    "discussion.txt": 64 * 1024,
    "noaa-planetary-k-index.json": 128 * 1024,
    "alerts.jsonl": 256 * 1024,
}


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
# Backup paths
# --------------------------------------------------

def backup_paths(filename: str):
    current = DATA_DIR / filename

    old = current.with_name(
        f"{current.stem}.old{current.suffix}"
    )

    older = current.with_name(
        f"{current.stem}.older{current.suffix}"
    )

    return current, old, older


# --------------------------------------------------
# Redirect and retry helpers
# --------------------------------------------------

def is_allowed_redirect(url: str) -> bool:
    host = urlparse(url).hostname
    if host is None:
        return False
    return host in ALLOWED_REDIRECT_HOSTS


def get_retry_after(error: HTTPError) -> float | None:
    headers = getattr(error, "headers", None)
    value = headers.get("Retry-After") if headers is not None else None

    if not value:
        return None

    try:
        return max(0, float(value))
    except ValueError:
        pass

    try:
        retry_date = parsedate_to_datetime(value)

        if retry_date.tzinfo is None:
            retry_date = retry_date.replace(
                tzinfo=timezone.utc
            )

        return max(
            0,
            (retry_date - datetime.now(timezone.utc)).total_seconds(),
        )

    except (TypeError, ValueError, OverflowError):
        return None


# --------------------------------------------------
# Download and validation
# --------------------------------------------------

def validate_file(filename: str, temp_path: Path, max_size: int) -> bool:
    """
    Validate downloaded content before touching existing files.
    """

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
            logger.error(
                "%s: Invalid JSON rejected: %s",
                filename,
                error,
            )
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


def download_file(url: str, destination: Path, max_size: int) -> Path | None:
    """
    Download to a temporary file.

    Returns the temporary file path only after a successful
    download and validation.
    """

    for attempt in range(1, MAX_ATTEMPTS + 1):
        temp_path = None

        try:
            logger.info(
                "Downloading %s from %s (attempt %d/%d)",
                destination.name,
                url,
                attempt,
                MAX_ATTEMPTS,
            )

            request = Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "*/*",
                },
            )

            with urlopen(
                request,
                timeout=REQUEST_TIMEOUT,
            ) as response:
                final_url = response.geturl()
                if not is_allowed_redirect(final_url):
                    raise ValueError(
                        f"Unexpected redirect target: {final_url}"
                    )

                if not 200 <= response.status < 300:
                    raise RuntimeError(
                        f"Unexpected HTTP status: {response.status}"
                    )

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
            return temp_path

        except Exception as error:
            logger.warning(
                "%s download from %s failed (%s): %s",
                destination.name,
                url,
                type(error).__name__,
                error,
            )

            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

            retry_after = (
                get_retry_after(error)
                if isinstance(error, HTTPError)
                else None
            )

            if retry_after is not None and retry_after > MAX_RETRY_WAIT:
                logger.warning(
                    "%s: Retry-After exceeds 5 minutes; treating as failure",
                    destination.name,
                )
                break

            if attempt < MAX_ATTEMPTS:
                backoff = min(
                    INITIAL_BACKOFF * (2 ** (attempt - 1)),
                    MAX_BACKOFF,
                )
                delay = min(max(backoff, retry_after or 0), MAX_RETRY_WAIT)

                logger.warning(
                    "Retrying %s in %.0f seconds",
                    destination.name,
                    delay,
                )
                time.sleep(delay)

    logger.error(
        "%s: All download attempts failed",
        destination.name,
    )
    return None


# --------------------------------------------------
# Alerts archive handling
# --------------------------------------------------

def read_alerts_archive() -> list[dict]:
    archive_path = DATA_DIR / ALERTS_ARCHIVE
    if not archive_path.exists():
        return []

    entries = []
    with archive_path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError as error:
                logger.error(
                    "%s: Invalid JSONL record rejected: %s",
                    ALERTS_ARCHIVE,
                    error,
                )
                raise

    return entries


def update_alerts_archive(url: str, max_size: int) -> bool:
    """
    Maintain an append-only alerts archive in JSONL format.

    The archive is ordered oldest to newest, so we append only newly
    observed alerts in reverse order and keep a fail-safe cap.
    """

    logger.info("Processing %s", ALERTS_ARCHIVE)

    temp_path = download_file(
        url,
        DATA_DIR / "alerts.json",
        max_size,
    )
    if temp_path is None:
        logger.error("%s: Existing archive left untouched", ALERTS_ARCHIVE)
        return False

    try:
        with temp_path.open("r", encoding="utf-8-sig") as file:
            remote_entries = json.load(file)

        if not isinstance(remote_entries, list):
            raise ValueError("Alert snapshot is not a JSON array")

        archive_path = DATA_DIR / ALERTS_ARCHIVE
        archive_entries = read_alerts_archive()

        if not archive_entries:
            ordered = list(reversed(remote_entries))
            with archive_path.open("w", encoding="utf-8") as file:
                for item in ordered:
                    file.write(json.dumps(item, separators=(",", ":")) + "\n")
            logger.info("Created %s from remote snapshot", ALERTS_ARCHIVE)
            return True

        latest_local = archive_entries[-1]
        latest_local_key = (
            latest_local.get("issue_datetime"),
            latest_local.get("product_id"),
        )

        new_entries = []
        for entry in remote_entries:
            entry_key = (
                entry.get("issue_datetime"),
                entry.get("product_id"),
            )
            if entry_key == latest_local_key:
                break
            new_entries.append(entry)

        if len(new_entries) > MAX_NEW_ALERTS:
            logger.error(
                "%s: Detected %d new alerts; skipping archive update to avoid runaway append",
                ALERTS_ARCHIVE,
                len(new_entries),
            )
            return False

        if not new_entries:
            logger.info("%s: No new alerts to append", ALERTS_ARCHIVE)
            return True

        ordered_new_entries = list(reversed(new_entries))
        with archive_path.open("a", encoding="utf-8") as file:
            for item in ordered_new_entries:
                file.write(json.dumps(item, separators=(",", ":")) + "\n")

        logger.info(
            "%s: Appended %d alerts",
            ALERTS_ARCHIVE,
            len(ordered_new_entries),
        )
        return True

    except (json.JSONDecodeError, ValueError, TypeError) as error:
        logger.error("%s: Invalid alert snapshot: %s", ALERTS_ARCHIVE, error)
        return False

    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)


# --------------------------------------------------
# Backup rotation and installation
# --------------------------------------------------

def install_file(filename: str, temp_path: Path) -> bool:
    """
    Rotate backups and install the validated temporary file.

    Existing files are not touched until validation succeeds.
    """

    current, old, older = backup_paths(filename)

    try:
        if old.exists():
            logger.info("%s -> %s", old.name, older.name)
            os.replace(old, older)

        if current.exists():
            logger.info("%s -> %s", current.name, old.name)
            os.replace(current, old)

        os.replace(temp_path, current)

        logger.info(
            "%s successfully updated",
            filename,
        )

        return True

    except OSError as error:
        logger.error(
            "%s: Installation failed: %s",
            filename,
            error,
        )

        if not current.exists() and old.exists():
            try:
                shutil.copy2(old, current)
                logger.warning(
                    "%s: Restored previous current file",
                    filename,
                )
            except OSError as restore_error:
                logger.critical(
                    "%s: Restoration failed: %s",
                    filename,
                    restore_error,
                )

        return False

    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


# --------------------------------------------------
# Per-file update
# --------------------------------------------------

def update_file(filename: str, url: str, max_size: int) -> bool:
    destination = DATA_DIR / filename

    logger.info("Processing %s", filename)

    temp_path = download_file(url, destination, max_size)

    if temp_path is None:
        logger.error(
            "%s: Existing files left untouched",
            filename,
        )
        return False

    time.sleep(REQUEST_GAP_SECONDS)
    return install_file(filename, temp_path)


# --------------------------------------------------
# Update log
# --------------------------------------------------

def write_update_log(results: dict) -> None:
    """
    Append one UTC-dated update summary to log.md.
    """

    log_file = DATA_DIR / "log.md"

    timestamp = datetime.now(timezone.utc).strftime(
        "%d.%m.%Y %H:%M:%S UTC"
    )

    entries = []
    for filename, success in results.items():
        status = "success" if success else "fail"
        entries.append(f"{filename}:{status}")

    log_entry = (
        f"{timestamp} - "
        + " - ".join(entries)
        + "\n"
    )

    with log_file.open("a", encoding="utf-8") as file:
        file.write(log_entry)

    logger.info("Update log appended to %s", log_file)


# --------------------------------------------------
# Main
# --------------------------------------------------

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Starting weather data update")
    logger.info("Data directory: %s", DATA_DIR)

    results = {}

    for filename, url in FILES.items():
        results[filename] = update_file(
            filename,
            url,
            FILE_SIZE_LIMITS.get(filename, 64 * 1024),
        )

    results[ALERTS_ARCHIVE] = update_alerts_archive(
        ALERTS_URL,
        FILE_SIZE_LIMITS.get(ALERTS_ARCHIVE, 256 * 1024),
    )

    logger.info("=" * 50)
    logger.info("Weather data update summary")

    for filename, success in results.items():
        logger.info(
            "%s: %s",
            filename,
            "UPDATED" if success else "FAILED - OLD DATA PRESERVED",
        )

    try:
        write_update_log(results)
    except OSError as error:
        logger.error("Could not write update log: %s", error)

    logger.info("=" * 50)

    if not all(results.values()):
        logger.warning("One or more files were not updated.")
    else:
        logger.info("All weather files successfully updated.")


if __name__ == "__main__":
    main()


# Weather data from MET Norway (CC BY 4.0).
# Space weather data from NOAA SWPC.
# Local copies of the files are used when a live fetch fails.
# allright
