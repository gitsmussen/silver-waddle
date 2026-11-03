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
MAX_BACKOFF = 900

FILES = {
    "sgarf.txt":
        "https://services.swpc.noaa.gov/text/sgarf.txt",

    "weekly.txt":
        "https://services.swpc.noaa.gov/text/weekly.txt",

    "3-dayforecast.txt":
        "https://services.swpc.noaa.gov/text/3-day-forecast.txt",

    "alerts.json":
        "https://services.swpc.noaa.gov/products/alerts.json",
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
# Download and validation
# --------------------------------------------------

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
            (retry_date - datetime.now(timezone.utc)).total_seconds()
        )

    except (TypeError, ValueError, OverflowError):
        return None


def validate_file(filename: str, temp_path: Path) -> bool:
    """
    Validate downloaded content before touching existing files.

    Text downloads must be UTF-8 text and not an obvious HTML error page.
    """

    if not temp_path.exists():
        logger.error("%s: Temporary file missing", filename)
        return False

    if temp_path.stat().st_size == 0:
        logger.error("%s: Empty download rejected", filename)
        return False

    if filename == "alerts.json":
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

    elif filename.endswith(".txt"):
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


def download_file(url: str, destination: Path) -> Path | None:
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
                timeout=REQUEST_TIMEOUT
            ) as response:

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

                    shutil.copyfileobj(response, temp_file)

                    temp_file.flush()
                    os.fsync(temp_file.fileno())

            if not validate_file(destination.name, temp_path):
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

            if attempt < MAX_ATTEMPTS:

                backoff = min(
                    INITIAL_BACKOFF * (2 ** (attempt - 1)),
                    MAX_BACKOFF,
                )

                # Keep the total delay within the configured maximum,
                # even if the server sends a longer Retry-After value.
                delay = min(max(backoff, retry_after or 0), MAX_BACKOFF)

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
# Backup rotation and installation
# --------------------------------------------------

def install_file(filename: str, temp_path: Path) -> bool:
    """
    Rotate backups and install the validated temporary file.

    Existing files are not touched until validation succeeds.
    """

    current, old, older = backup_paths(filename)

    try:
        # Rotate older backup first.
        if old.exists():
            logger.info("%s -> %s", old.name, older.name)
            os.replace(old, older)

        # Rotate current file.
        if current.exists():
            logger.info("%s -> %s", current.name, old.name)
            os.replace(current, old)

        # Install newly downloaded file.
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

        # Attempt to restore the original current file.
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

def update_file(filename: str, url: str) -> bool:

    destination = DATA_DIR / filename

    logger.info("Processing %s", filename)

    # First download and validate.
    temp_path = download_file(url, destination)

    if temp_path is None:
        logger.error(
            "%s: Existing files left untouched",
            filename,
        )
        return False

    # Only now rotate backups and install.
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

        results[filename] = update_file(filename, url)

    logger.info("=" * 50)
    logger.info("Weather data update summary")

    for filename, success in results.items():
        logger.info(
            "%s: %s",
            filename,
            "UPDATED" if success else "FAILED - OLD DATA PRESERVED",
        )

    # Append execution results to log.md
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
# Local copies of the files are only used if a live fetch fails.
# allright
