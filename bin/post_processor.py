#!/usr/bin/env python3
"""satpi – post_processor

Postprocessing nach Pass-Aufzeichnung: Copy, Notify, DB-Import, Plotting

Ein oder mehrere bereits aufgezeichnete Pässe verarbeiten.
Abhängig von Execution-Flags:
  --copy   → rclone copy zum Remote
  --notify → Mail-Benachrichtigung senden
  --db     → Import zu SQLite reception.db
  --plots  → Erzeuge Plots via plot_reception.py

Kann manuell aufgerufen oder vom receive_orchestrator.py gesteuert werden.

Author: Andreas Horvath
Project: satpi
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))
from read_config import read_config, ConfigError


logger = logging.getLogger("satpi.post_processor")


# --- Constants ---------------------------------------------------------------

COPY_TIMEOUT_SECONDS = 30 * 60
MAIL_TIMEOUT_SECONDS = 60
DB_IMPORT_TIMEOUT_SECONDS = 120
PLOT_TIMEOUT_SECONDS = 180
RCLONE_LINK_TIMEOUT_SECONDS = 60


# --- Helpers -----------------------------------------------------------------

def setup_logger(log_file: Optional[str] = None) -> None:
    """Setup logging to stderr and optionally to file."""
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        logger.addHandler(fh)


def _run_with_timeout(
    cmd: List[str],
    *,
    timeout: int,
    log_path: Optional[str] = None,
    cwd: Optional[str] = None,
) -> int:
    """Run a command with timeout, log output if requested."""
    logger.info("Running: %s", " ".join(cmd))

    try:
        with open(log_path, "w", encoding="utf-8") if log_path else open(os.devnull, "w") as lf:
            proc = subprocess.run(
                cmd,
                stdout=lf if log_path else subprocess.PIPE,
                stderr=subprocess.STDOUT if log_path else subprocess.PIPE,
                text=True,
                timeout=timeout,
                cwd=cwd,
            )
    except subprocess.TimeoutExpired:
        logger.error("Command timed out after %ds: %s", timeout, " ".join(cmd))
        return 124

    if proc.returncode != 0:
        logger.error("Command failed with rc=%s", proc.returncode)
        if proc.stderr and proc.stderr.strip():
            logger.error("stderr: %s", proc.stderr.strip())
    else:
        logger.info("Command succeeded")

    return proc.returncode


# --- Pass Finding ------------------------------------------------------------

def find_pass_dirs(output_dir: str, count: int) -> List[Tuple[str, str]]:
    """Find the last N pass directories sorted by timestamp.

    Returns: [(pass_name, pass_dir), ...]
    """
    if not os.path.isdir(output_dir):
        logger.error("output_dir does not exist: %s", output_dir)
        return []

    entries = []
    for entry in os.listdir(output_dir):
        entry_path = os.path.join(output_dir, entry)
        if os.path.isdir(entry_path):
            entries.append((entry, entry_path))

    # Sort by name (contains timestamp: YYYY-MM-DD_HH-MM-SS_...)
    entries.sort(reverse=True)
    return entries[:count]


def load_pass_file(pass_dir: str) -> Optional[Dict[str, Any]]:
    """Load reception.json from pass directory."""
    reception_json = os.path.join(pass_dir, "reception.json")
    if not os.path.exists(reception_json):
        logger.error("reception.json not found: %s", reception_json)
        return None

    try:
        with open(reception_json, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load reception.json: %s", e)
        return None


# --- Postprocessing Steps ---------------------------------------------------

def copy_output(
    config: Dict[str, Any],
    pass_id: str,
    pass_output_dir: str,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """Copy pass output via rclone, optionally generate link.

    Returns: (success: bool, target_path: Optional[str], link: Optional[str])
    """
    cfg = config.get("copytarget", {})
    if not cfg.get("enabled"):
        logger.info("Copy target disabled")
        return False, None, None

    if cfg.get("type") != "rclone":
        logger.error("Unsupported copy type: %s", cfg.get("type"))
        return False, None, None

    remote = str(cfg.get("rclone_remote", "")).strip()
    rclone_dir = str(cfg.get("rclone_dir", "")).strip()

    if not remote or not rclone_dir:
        logger.error("rclone target not fully configured (remote=%s, dir=%s)", remote, rclone_dir)
        return False, None, None

    pass_name = os.path.basename(pass_output_dir)
    target = f"{remote}:{rclone_dir}/{pass_name}"

    log_dir = config.get("paths", {}).get("log_dir", "/tmp")
    upload_log = os.path.join(log_dir, f"{pass_id}-upload.log")

    rc = _run_with_timeout(
        ["rclone", "copy", pass_output_dir, target],
        timeout=COPY_TIMEOUT_SECONDS,
        log_path=upload_log,
    )

    if rc != 0:
        logger.error("rclone copy failed (rc=%s)", rc)
        return False, target, None

    # Try to generate link if enabled
    link = None
    if cfg.get("create_link"):
        try:
            result = subprocess.run(
                ["rclone", "link", target],
                capture_output=True,
                text=True,
                check=True,
                timeout=RCLONE_LINK_TIMEOUT_SECONDS,
            )
            link = result.stdout.strip() or None
            if link:
                logger.info("Generated link: %s", link)
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as e:
            logger.warning("Link generation failed: %s", e)

    return True, target, link


def send_notification(
    config: Dict[str, Any],
    reception_payload: Dict[str, Any],
    copy_ok: bool,
    target: Optional[str],
    link: Optional[str],
) -> bool:
    """Send mail notification with pass summary.

    Returns: success: bool
    """
    ncfg = config.get("notify", {})
    if not ncfg.get("enabled"):
        logger.info("Notifications disabled")
        return True

    mail_to = ncfg.get("mail_to", "").strip()
    if not mail_to:
        logger.error("notify.mail_to not configured")
        return False

    mail_bin = config.get("paths", {}).get("mail_bin")
    if not mail_bin or not os.path.exists(mail_bin):
        logger.error("mail binary not found: %s", mail_bin)
        return False

    # Build message
    subject_prefix = ncfg.get("mail_subject_prefix", "[SATPI]")
    satellite = reception_payload.get("satellite", "UNKNOWN")
    subject = f"{subject_prefix} {satellite}"

    body_parts = [
        f"Satellite: {satellite}",
        f"Pass ID: {reception_payload.get('pass_id', '?')}",
        f"Samples: {len(reception_payload.get('samples', []))}",
    ]

    if link:
        body_parts.append(f"Link: {link}")
    elif copy_ok and target:
        body_parts.append(f"Target: {target}")
    else:
        body_parts.append("Copy failed — files local only")

    body = "\n".join(body_parts) + "\n"
    mail_data = f"Subject: {subject}\n\n{body}"

    try:
        proc = subprocess.run(
            [mail_bin, mail_to],
            input=mail_data,
            text=True,
            capture_output=True,
            timeout=MAIL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        logger.error("Mail send timed out")
        return False

    if proc.returncode != 0:
        logger.error("Mail failed (rc=%s)", proc.returncode)
        if proc.stderr.strip():
            logger.error("mail stderr: %s", proc.stderr.strip())
        return False

    logger.info("Notification sent to %s", mail_to)
    return True


def import_to_db(config: Dict[str, Any], reception_json_path: str) -> bool:
    """Import reception data to SQLite database.

    Returns: success: bool
    """
    base_dir = str(Path(__file__).resolve().parent.parent)
    script = os.path.join(base_dir, "bin", "import_to_db.py")

    if not os.path.exists(script):
        logger.warning("import_to_db.py not found: %s", script)
        return False

    log_dir = config.get("paths", {}).get("log_dir", "/tmp")
    python_bin = config.get("paths", {}).get("python_bin", "python3")

    rc = _run_with_timeout(
        [python_bin, script, reception_json_path],
        timeout=DB_IMPORT_TIMEOUT_SECONDS,
        cwd=base_dir,
    )

    return rc == 0


def generate_plots(config: Dict[str, Any], pass_id: str) -> bool:
    """Generate plots for the pass using composite key (date, start_time, satellite).

    Returns: success: bool
    """
    base_dir = str(Path(__file__).resolve().parent.parent)
    script = os.path.join(base_dir, "bin", "plot_reception.py")

    if not os.path.exists(script):
        logger.warning("plot_reception.py not found: %s", script)
        return False

    python_bin = config.get("paths", {}).get("python_bin", "python3")

    # Extract date, start_time, satellite from pass_id: "2026-05-05_05-41-50_METEOR-M2_4"
    parts = pass_id.split("_", 2)
    if len(parts) < 3:
        logger.error("Invalid pass_id format: %s", pass_id)
        return False

    pass_date = parts[0]  # 2026-05-05
    pass_start_time = parts[1].replace("-", ":")  # 05-41-50 → 05:41:50
    satellite = parts[2].replace("_", " ")  # METEOR-M2_4 → METEOR-M2 4

    rc = _run_with_timeout(
        [python_bin, script, "--date", pass_date, "--start-time", pass_start_time, "--satellite", satellite],
        timeout=PLOT_TIMEOUT_SECONDS,
        cwd=base_dir,
    )

    return rc == 0


# --- CLI & Main --------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Post-process recorded satellite passes"
    )

    # Input: mutually exclusive
    input_group = p.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--pass-name",
        help="Pass name (e.g., 2026-05-04_13-45-30_METEOR_M2-X)",
    )
    input_group.add_argument(
        "--reception-json",
        help="Path to reception.json file",
    )
    input_group.add_argument(
        "--pass-output-dir",
        help="Path to pass output directory",
    )
    input_group.add_argument(
        "--last",
        type=int,
        metavar="N",
        help="Process last N passes",
    )

    # Execution flags
    p.add_argument("--copy", action="store_true", help="Copy output via rclone")
    p.add_argument("--notify", action="store_true", help="Send mail notification")
    p.add_argument("--db", action="store_true", help="Import to database")
    p.add_argument("--plots", action="store_true", help="Generate plots")

    # Config
    p.add_argument(
        "--config",
        help="Path to config.ini (default: ~/satpi/config/config.ini)",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()

    # Load config
    if args.config:
        config_path = args.config
    else:
        base_dir = str(Path(__file__).resolve().parent.parent)
        config_path = os.path.join(base_dir, "config", "config.ini")

    try:
        config = read_config(config_path)
    except ConfigError as e:
        print(f"[post_processor] CONFIG ERROR: {e}", file=sys.stderr)
        return 2

    output_dir = config.get("paths", {}).get("output_dir")
    if not output_dir:
        print("[post_processor] output_dir not configured in config.ini", file=sys.stderr)
        return 2

    output_dir = os.path.expanduser(output_dir)

    setup_logger()

    logger.info("post_processor started")

    # Determine which passes to process
    passes_to_process: List[Tuple[str, str]] = []

    if args.last:
        logger.info("Processing last %d passes", args.last)
        passes_to_process = find_pass_dirs(output_dir, args.last)
        if not passes_to_process:
            logger.error("No passes found in %s", output_dir)
            return 1

    elif args.pass_name:
        pass_dir = os.path.join(output_dir, args.pass_name)
        if not os.path.isdir(pass_dir):
            logger.error("Pass directory not found: %s", pass_dir)
            return 1
        passes_to_process = [(args.pass_name, pass_dir)]

    elif args.reception_json:
        if not os.path.exists(args.reception_json):
            logger.error("reception.json not found: %s", args.reception_json)
            return 1
        pass_dir = os.path.dirname(args.reception_json)
        pass_name = os.path.basename(pass_dir)
        passes_to_process = [(pass_name, pass_dir)]

    elif args.pass_output_dir:
        if not os.path.isdir(args.pass_output_dir):
            logger.error("pass_output_dir not found: %s", args.pass_output_dir)
            return 1
        pass_dir = args.pass_output_dir
        pass_name = os.path.basename(pass_dir)
        passes_to_process = [(pass_name, pass_dir)]

    # Process passes
    logger.info("Processing %d pass(es)", len(passes_to_process))

    failed_count = 0
    for pass_name, pass_dir in passes_to_process:
        logger.info("─" * 60)
        logger.info("Processing: %s", pass_name)

        reception_payload = load_pass_file(pass_dir)
        if not reception_payload:
            logger.error("Failed to load reception data for %s", pass_name)
            failed_count += 1
            continue

        # Execute enabled steps
        copy_ok = False
        target = None
        link = None

        if args.copy:
            logger.info("Step: Copy")
            copy_ok, target, link = copy_output(config, pass_name, pass_dir)

        if args.db:
            logger.info("Step: DB Import")
            reception_json = os.path.join(pass_dir, "reception.json")
            import_to_db(config, reception_json)

        if args.plots:
            logger.info("Step: Plots")
            pass_id = pass_name
            generate_plots(config, pass_id)

        if args.notify:
            logger.info("Step: Notify")
            send_notification(config, reception_payload, copy_ok, target, link)

    logger.info("Pass %s completed", pass_name)

    logger.info("─" * 60)
    if failed_count > 0:
        logger.error("Completed with %d failure(s)", failed_count)
        return 1

    logger.info("All passes processed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
