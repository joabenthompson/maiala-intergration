"""
Maiala Park Lodge - Daily Housekeeping Job Creator
Runs at 12:01am AEST each day via Render Cron Job.

Flow:
  1. Query Checkfront for today's checkouts (end_date = today)
  2. For each active booking, create a Flip Checkout job in Operandio
     for each cabin in the booking, titled
     "Flip [Cabin] - Checkout [D Month YYYY] ([Guest Name])"
"""

import os
import logging
import requests
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Checkfront config
CHECKFRONT_BASE_URL = "https://maiala-park-lodge.checkfront.com/api/3.0"
CHECKFRONT_API_KEY = os.environ.get("CHECKFRONT_API_KEY", "")
CHECKFRONT_API_SECRET = os.environ.get("CHECKFRONT_API_SECRET", "")

# Operandio config
OPERANDIO_USERNAME = os.environ.get("OPERANDIO_USERNAME")
OPERANDIO_PASSWORD = os.environ.get("OPERANDIO_PASSWORD")
OPERANDIO_AUTH_URL = "https://api.operandio.com/auth/oauth2/token"
OPERANDIO_GRAPHQL_URL = "https://api.operandio.com/graphql"
OPERANDIO_LOCATION_ID = "6875e8527e4d972e36fe8073"  # Maiala Park Lodge

# Protect /run-daily endpoint with a secret
CRON_SECRET = os.environ.get("CRON_SECRET", "")

AEST = ZoneInfo("Australia/Brisbane")

# Bookings with these statuses are considered active
# PAID = fully paid, PART = deposit paid, HOLD = on hold, PEND = pending
# OTACC = OTA takes payment, OTAPP = OTA pending payment
ACTIVE_STATUSES = {"PAID", "PART", "HOLD", "PEND", "OTACC", "OTAPP", "OTAAB"}

# Map cabin names (lowercase) to Operandio process + schedule IDs
CABIN_MAP = {
    "kookaburra":        {"process": "68fff4c0b8d923384331984e", "schedule": "68fff4c0b8d9233843319835", "label": "Kookaburra Suite"},
    "kookaburra suite":  {"process": "68fff4c0b8d923384331984e", "schedule": "68fff4c0b8d9233843319835", "label": "Kookaburra Suite"},
    "pademelon":         {"process": "68fff4ecf0cdee421c13570a", "schedule": "68fff4ecf0cdee421c1356f1", "label": "Pademelon Suite"},
    "pademelon suite":   {"process": "68fff4ecf0cdee421c13570a", "schedule": "68fff4ecf0cdee421c1356f1", "label": "Pademelon Suite"},
    "echidna":           {"process": "68fff515c1c4d200fadf3b0d", "schedule": "68fff515c1c4d200fadf3af4", "label": "Echidna Suite"},
    "echidna suite":     {"process": "68fff515c1c4d200fadf3b0d", "schedule": "68fff515c1c4d200fadf3af4", "label": "Echidna Suite"},
    "cockatoo":          {"process": "688f3858ae37bb646c829bf6", "schedule": "688f3858ae37bb646c829bde", "label": "Cockatoo Suite"},
    "cockatoo suite":    {"process": "688f3858ae37bb646c829bf6", "schedule": "688f3858ae37bb646c829bde", "label": "Cockatoo Suite"},
    "bowerbird":         {"process": "68fff5390c79dcfdc46f0cc1", "schedule": "68fff5390c79dcfdc46f0ca8", "label": "Bowerbird Cottage"},
    "bowerbird cottage": {"process": "68fff5390c79dcfdc46f0cc1", "schedule": "68fff5390c79dcfdc46f0ca8", "label": "Bowerbird Cottage"},
}

# Cabins that form the "Main House" (used for twin share scope matching)
MAIN_HOUSE_LABELS = {"Kookaburra Suite", "Pademelon Suite", "Echidna Suite", "Cockatoo Suite"}

# Items to explicitly ignore (non-cabin items that appear in booking summaries)
IGNORE_ITEMS = [
    "full property hire",
    "lodge group booking",
    "gather & feast",
    "gather and feast",
]


def get_cabin_configs_from_summary(summary):
    """
    Parse a Checkfront booking summary (possibly a comma-separated list of items)
    and return a list of cabin configs for all recognised cabins.
    """
    if not summary:
        return []

    configs = []
    seen_processes = set()  # dedupe in case the same cabin appears twice

    items = [item.strip() for item in summary.split(",")]

    for item in items:
        item_lower = item.lower()

        # Skip non-cabin items
        if any(ignore in item_lower for ignore in IGNORE_ITEMS):
            logger.info(f"Ignoring non-cabin item: '{item}'")
            continue

        # Try exact match first
        if item_lower in CABIN_MAP:
            config = CABIN_MAP[item_lower]
            if config["process"] not in seen_processes:
                configs.append(config)
                seen_processes.add(config["process"])
            continue

        # Try partial match (e.g. "Kookaburra Suite (Queen)" contains "kookaburra")
        matched = False
        for cabin_key, config in CABIN_MAP.items():
            if cabin_key in item_lower:
                if config["process"] not in seen_processes:
                    logger.info(f"Partial match: '{item}' → '{cabin_key}'")
                    configs.append(config)
                    seen_processes.add(config["process"])
                matched = True
                break

        if not matched:
            logger.warning(f"No cabin match for item: '{item}'")

    return configs


# ---------------------------------------------------------------------------
# Checkfront
# ---------------------------------------------------------------------------

def get_checkfront_checkouts(date_str, filter_status=True):
    """
    Query Checkfront for bookings where end_date = date_str (checkouts).
    Returns list of active booking dicts (or all bookings if filter_status=False).
    """
    logger.info(f"Querying Checkfront for checkouts on {date_str}")
    response = requests.get(
        f"{CHECKFRONT_BASE_URL}/booking",
        auth=(CHECKFRONT_API_KEY, CHECKFRONT_API_SECRET),
        params={"end_date": date_str, "limit": 100},
        timeout=30
    )
    response.raise_for_status()
    data = response.json()

    bookings_raw = data.get("booking/index", data.get("booking", {}))
    if not bookings_raw:
        logger.info(f"No bookings found for checkouts on {date_str}")
        return []

    bookings = list(bookings_raw.values()) if isinstance(bookings_raw, dict) else bookings_raw
    if not filter_status:
        return bookings
    active = [b for b in bookings if b.get("status_id", "") in ACTIVE_STATUSES]
    logger.info(f"Found {len(active)} active checkout bookings (filtered from {len(bookings)} total)")
    return active


def extract_cabin_summary(booking):
    """Extract the summary string (which may list multiple cabins) from a booking."""
    summary = booking.get("summary", "")
    if summary:
        return summary
    return booking.get("item_name", "")


def get_checkfront_booking_detail(booking_id):
    """Fetch full booking detail including individual items."""
    logger.info(f"Fetching full booking detail for {booking_id}")
    response = requests.get(
        f"{CHECKFRONT_BASE_URL}/booking/{booking_id}",
        auth=(CHECKFRONT_API_KEY, CHECKFRONT_API_SECRET),
        timeout=30
    )
    response.raise_for_status()
    return response.json().get("booking", {})


def get_cabin_configs_from_booking_detail(detail):
    """
    Parse cabin configs from a full booking's items dict.
    Used as a fallback when the booking-list summary doesn't yield a cabin match
    (common with OTA bookings where the summary is the channel name).
    """
    items = detail.get("items", {})
    if not items:
        return []

    all_configs = []
    seen = set()
    item_list = items.values() if isinstance(items, dict) else items

    for item in item_list:
        item_summary = item.get("summary", "")
        if item_summary:
            for config in get_cabin_configs_from_summary(item_summary):
                if config["process"] not in seen:
                    all_configs.append(config)
                    seen.add(config["process"])

    return all_configs


def has_twin_share_for_cabin(list_summary, cabin_label):
    """
    Return True if list_summary contains a Twin Share Configuration that applies
    to cabin_label, respecting booking scope:
      - "Twin Share Configuration - Echidna"       → Echidna Suite only
      - "Twin Share Configuration - Main House"     → Echidna, Pademelon, Cockatoo, Kookaburra
      - "All Twin Share Configuration (Full Property)" → all 5 cabins including Bowerbird
    """
    cabin_key = cabin_label.lower().split()[0]  # e.g. "echidna" from "Echidna Suite"

    for raw_item in list_summary.split(","):
        item = raw_item.strip().lower()
        if "twin share configuration" not in item:
            continue
        # Full property → applies to every cabin
        if "full property" in item or item.startswith("all twin"):
            return True
        # Main house → applies only to the four main house cabins
        if "main house" in item:
            return cabin_label in MAIN_HOUSE_LABELS
        # Specific cabin qualifier e.g. "twin share configuration - echidna"
        if " - " in item:
            qualifier = item.split(" - ", 1)[1].strip()
            if cabin_key in qualifier:
                return True
            continue  # qualifier names a different cabin — skip
        # No qualifier → plain "Twin Share Configuration", assume it applies
        return True
    return False


def get_checkfront_future_bookings(cabin_config, after_date_str):
    """
    Query Checkfront for active bookings in a specific cabin that start after
    after_date_str (YYYY-MM-DD). Uses a 365-day window. Returns a list of booking
    dicts sorted by start_date ascending.
    """
    after_date = datetime.strptime(after_date_str, "%Y-%m-%d")
    next_day = (after_date + timedelta(days=1)).strftime("%Y-%m-%d")
    # Use a 60-day window to stay well under Checkfront's 100-result cap.
    # The full-year window was returning 100 results sorted by booking_id ascending,
    # which cut off recently-created (high-ID) bookings for near-future stays.
    window_end = (after_date + timedelta(days=60)).strftime("%Y-%m-%d")

    logger.info(
        f"Querying Checkfront for future {cabin_config['label']} bookings after {after_date_str}"
    )
    response = requests.get(
        f"{CHECKFRONT_BASE_URL}/booking",
        auth=(CHECKFRONT_API_KEY, CHECKFRONT_API_SECRET),
        params={"start_date": next_day, "end_date": window_end, "limit": 100},
        timeout=30
    )
    response.raise_for_status()
    data = response.json()

    bookings_raw = data.get("booking/index", data.get("booking", {}))
    if not bookings_raw:
        return []

    bookings = list(bookings_raw.values()) if isinstance(bookings_raw, dict) else bookings_raw
    active = [b for b in bookings if b.get("status_id", "") in ACTIVE_STATUSES]

    target_process = cabin_config["process"]
    return [
        b for b in active
        if any(c["process"] == target_process
               for c in get_cabin_configs_from_summary(extract_cabin_summary(b)))
    ]


def _parse_checkfront_date(raw):
    """Parse a Checkfront date value: unix int, string-encoded unix, YYYY-MM-DD, or YYYYMMDD."""
    if not raw:
        return None
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw)
    s = str(raw).strip()
    if s.isdigit():
        return datetime.fromtimestamp(int(s))
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def get_bed_note_for_next_booking(cabin_config, checkout_date_str):
    """
    Look up the next active booking for cabin_config after checkout_date_str.
    If it includes a 'Twin Share Configuration' add-on, return the appropriate
    bed note string. Returns None if no note is needed.

    Checkfront's booking list API does not return stay dates, so full detail is
    fetched for each candidate to find the actual earliest check-in after checkout.
    """
    future_bookings = get_checkfront_future_bookings(cabin_config, checkout_date_str)
    if not future_bookings:
        return None

    checkout_date = datetime.strptime(checkout_date_str, "%Y-%m-%d")

    next_checkin_date = None
    next_detail = None
    next_booking = None

    for booking in future_bookings:
        bid = booking.get("booking_id") or booking.get("code", "")
        try:
            detail = get_checkfront_booking_detail(bid)
            raw = detail.get("start_date", "")
            if not raw:
                continue
            # start_date may be an int, a string-encoded unix timestamp, or YYYY-MM-DD / YYYYMMDD
            checkin = _parse_checkfront_date(raw)
            if checkin is None:
                logger.warning(f"Unrecognised start_date format for booking {bid}: {raw!r}")
                continue
            if checkin > checkout_date:
                if next_checkin_date is None or checkin < next_checkin_date:
                    next_checkin_date = checkin
                    next_detail = detail
                    next_booking = booking
        except Exception as e:
            logger.warning(f"Skipping booking {bid} in next-booking search: {e}")

    if not next_detail:
        logger.info(
            f"No confirmed future bookings for {cabin_config['label']} after {checkout_date_str}"
        )
        return None

    logger.info(
        f"Next booking for {cabin_config['label']}: check-in {next_checkin_date.date()}"
    )

    # Twin share add-ons appear in the list-level booking summary but NOT in the
    # detail's items dict (which only contains cabin/package line items). Use the
    # list-level summary that was already returned by the future bookings query,
    # and check scope (individual cabin / main house / full property) so that a
    # "Main House" twin share doesn't incorrectly trigger a note for Bowerbird.
    list_summary = extract_cabin_summary(next_booking)
    if not has_twin_share_for_cabin(list_summary, cabin_config["label"]):
        logger.info(f"No applicable twin share configuration for next {cabin_config['label']} booking")
        return None

    days_until = (next_checkin_date - checkout_date).days
    checkin_label = next_checkin_date.strftime("%d/%m/%Y")

    if days_until <= 5:
        return f"Split beds required - {checkin_label}"
    else:
        return f"Do not make beds, split beds required - {checkin_label}"


# ---------------------------------------------------------------------------
# Operandio
# ---------------------------------------------------------------------------

def get_operandio_token():
    """Get Operandio OAuth bearer token."""
    response = requests.post(
        OPERANDIO_AUTH_URL,
        data={"grant_type": "client_credentials"},
        auth=(OPERANDIO_USERNAME, OPERANDIO_PASSWORD),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30
    )
    response.raise_for_status()
    return response.json().get("access_token")


def graphql(token, query, variables=None):
    """Execute a GraphQL mutation/query against Operandio."""
    response = requests.post(
        OPERANDIO_GRAPHQL_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        },
        json={"query": query, "variables": variables or {}},
        timeout=30
    )
    response.raise_for_status()
    result = response.json()
    if "errors" in result:
        raise Exception(f"GraphQL errors: {result['errors']}")
    return result["data"]


def run_process(token, process_id, schedule_id):
    """Run a process adhoc to create a job instance. Returns job ID."""
    data = graphql(token, """
        mutation RunProcess($processId: ID!, $scheduleId: ID!, $locationId: ID!) {
            process(id: $processId) {
                run(schedule: $scheduleId, location: $locationId) {
                    id
                    processName
                }
            }
        }
    """, {
        "processId": process_id,
        "scheduleId": schedule_id,
        "locationId": OPERANDIO_LOCATION_ID
    })
    job_id = data["process"]["run"]["id"]
    logger.info(f"Created job instance: {job_id} ({data['process']['run']['processName']})")
    return job_id


def update_job_title(token, job_id, title):
    """Update the display title of a job instance."""
    graphql(token, """
        mutation UpdateJobTitle($jobId: ID!, $title: String!) {
            job(id: $jobId) {
                updateTitle(title: $title) {
                    id
                    title
                }
            }
        }
    """, {"jobId": job_id, "title": title})
    logger.info(f"Set job title: '{title}'")


def update_job_description(token, job_id, description):
    """Update the description field of a job instance."""
    graphql(token, """
        mutation UpdateJobDescription($jobId: ID!, $description: String!) {
            job(id: $jobId) {
                updateDescription(description: $description) {
                    id
                    description
                }
            }
        }
    """, {"jobId": job_id, "description": description})
    logger.info(f"Set job description: '{description}'")


def create_flip_checkout_job(token, cabin_config, date_str, guest_name="", bed_note=None):
    """
    Create a Flip Checkout job for a cabin, dated for the checkout day.
    Optionally sets a job description for bed configuration.
    Returns (job_id, title, bed_note).
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        date_label = date_obj.strftime("%-d %B %Y")
    except Exception:
        date_label = date_str

    guest_part = f" ({guest_name})" if guest_name else ""
    title = f"Flip {cabin_config['label']} - Checkout {date_label}{guest_part}"

    job_id = run_process(token, cabin_config["process"], cabin_config["schedule"])
    update_job_title(token, job_id, title)

    if bed_note:
        update_job_description(token, job_id, bed_note)

    logger.info(f"Flip checkout job ready: '{title}'" + (f" | Note: '{bed_note}'" if bed_note else ""))
    return job_id, title, bed_note


# ---------------------------------------------------------------------------
# Core daily logic
# ---------------------------------------------------------------------------

def run_daily_jobs(today_str):
    """
    Query Checkfront for today's checkouts and create a Flip Checkout job in
    Operandio for each cabin in each active booking.
    """
    results = {
        "date": today_str,
        "checkouts": [],
        "errors": []
    }

    token = get_operandio_token()
    logger.info(f"Running daily checkout jobs for {today_str}")

    try:
        checkouts = get_checkfront_checkouts(today_str)
        for booking in checkouts:
            try:
                booking_id = booking.get("booking_id") or booking.get("code", "")
                guest_name = booking.get("customer_name", "")
                summary = extract_cabin_summary(booking)
                cabin_configs = get_cabin_configs_from_summary(summary)

                if not cabin_configs:
                    # OTA bookings (e.g. Airbnb) often have the channel name as
                    # the summary rather than the cabin name. Fall back to fetching
                    # the full booking detail and reading the items list.
                    logger.info(
                        f"No cabin match from summary '{summary}' for booking "
                        f"{booking_id} — fetching full booking detail"
                    )
                    raw_id = booking.get("booking_id")
                    if raw_id:
                        try:
                            detail = get_checkfront_booking_detail(raw_id)
                            cabin_configs = get_cabin_configs_from_booking_detail(detail)
                        except Exception as e:
                            logger.error(f"Failed to fetch detail for booking {booking_id}: {e}")

                if not cabin_configs:
                    logger.warning(
                        f"Skipping booking {booking_id} - no recognised cabins in "
                        f"summary or items: '{summary}'"
                    )
                    results["errors"].append(
                        f"No cabins matched for booking {booking_id}: '{summary}'"
                    )
                    continue

                for cabin_config in cabin_configs:
                    try:
                        bed_note = None
                        try:
                            bed_note = get_bed_note_for_next_booking(cabin_config, today_str)
                        except Exception as bed_err:
                            logger.warning(
                                f"Bed note lookup failed for {cabin_config['label']} "
                                f"(booking {booking_id}): {bed_err} — creating job without note"
                            )

                        job_id, title, bed_note = create_flip_checkout_job(
                            token=token,
                            cabin_config=cabin_config,
                            date_str=today_str,
                            guest_name=guest_name,
                            bed_note=bed_note
                        )
                        results["checkouts"].append({
                            "booking_id": booking_id,
                            "cabin": cabin_config["label"],
                            "guest": guest_name,
                            "job_id": job_id,
                            "title": title,
                            "bed_note": bed_note
                        })
                    except Exception as e:
                        logger.error(
                            f"Error creating job for booking {booking_id}, "
                            f"cabin {cabin_config['label']}: {e}"
                        )
                        results["errors"].append(
                            f"Job creation failed for {cabin_config['label']} ({booking_id}): {e}"
                        )

            except Exception as e:
                logger.error(f"Error processing booking {booking.get('booking_id')}: {e}")
                results["errors"].append(str(e))
    except Exception as e:
        logger.error(f"Error fetching checkouts: {e}")
        results["errors"].append(f"Checkout fetch error: {e}")

    return results


# ---------------------------------------------------------------------------
# Flask endpoints
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "Maiala Park Lodge Integration"})


@app.route("/run-daily", methods=["GET", "POST"])
def run_daily_endpoint():
    """
    Called by Render Cron Job at 2:01pm UTC (= 12:01am AEST).
    Protected by CRON_SECRET header.
    """
    if CRON_SECRET:
        auth = request.headers.get("X-Cron-Secret", "")
        if auth != CRON_SECRET:
            return jsonify({"error": "Unauthorized"}), 401

    now_aest = datetime.now(AEST)
    today_str = now_aest.strftime("%Y-%m-%d")

    try:
        results = run_daily_jobs(today_str)
        total = len(results["checkouts"])
        logger.info(
            f"Daily run complete: {total} checkout jobs created, "
            f"{len(results['errors'])} errors"
        )
        return jsonify({"success": True, **results}), 200
    except Exception as e:
        logger.error(f"Daily run failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/test", methods=["GET", "POST"])
def test_endpoint():
    """
    Test endpoint - pass ?date=YYYY-MM-DD to simulate a specific day,
    or leave blank to use today AEST.
    Pass ?dry_run=true to query Checkfront without creating Operandio jobs.
    """
    date_param = request.args.get("date")
    dry_run = request.args.get("dry_run", "false").lower() == "true"
    show_all = request.args.get("all", "false").lower() == "true"

    if date_param:
        try:
            today = datetime.strptime(date_param, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": "Invalid date format, use YYYY-MM-DD"}), 400
    else:
        today = datetime.now(AEST)

    today_str = today.strftime("%Y-%m-%d")

    if dry_run:
        try:
            checkouts = get_checkfront_checkouts(today_str, filter_status=not show_all)
            preview = []
            for b in checkouts:
                summary = extract_cabin_summary(b)
                configs = get_cabin_configs_from_summary(summary)
                fallback_used = False
                if not configs:
                    raw_id = b.get("booking_id")
                    if raw_id:
                        try:
                            detail = get_checkfront_booking_detail(raw_id)
                            configs = get_cabin_configs_from_booking_detail(detail)
                            fallback_used = bool(configs)
                        except Exception:
                            pass
                bed_notes_preview = []
                for cabin_cfg in configs:
                    note = None
                    try:
                        note = get_bed_note_for_next_booking(cabin_cfg, today_str)
                    except Exception:
                        note = "lookup_failed"
                    bed_notes_preview.append({"cabin": cabin_cfg["label"], "bed_note": note})

                preview.append({
                    "booking_id": b.get("booking_id"),
                    "code": b.get("code"),
                    "guest": b.get("customer_name"),
                    "status": b.get("status_id"),
                    "summary": summary,
                    "cabins_matched": [c["label"] for c in configs],
                    "detail_fallback_used": fallback_used,
                    "bed_notes": bed_notes_preview
                })
            return jsonify({
                "dry_run": True,
                "date": today_str,
                "checkouts_found": len(checkouts),
                "bookings": preview
            }), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    try:
        results = run_daily_jobs(today_str)
        return jsonify({"success": True, **results}), 200
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/debug-future", methods=["GET"])
def debug_future_endpoint():
    """
    Diagnostic: shows what Checkfront returns for the future bookings query.
    Usage: /debug-future?date=YYYY-MM-DD&cabin=echidna
    """
    date_param = request.args.get("date")
    cabin_param = request.args.get("cabin", "").lower()

    if date_param:
        try:
            datetime.strptime(date_param, "%Y-%m-%d")
            date_str = date_param
        except ValueError:
            return jsonify({"error": "Invalid date format, use YYYY-MM-DD"}), 400
    else:
        date_str = datetime.now(AEST).strftime("%Y-%m-%d")

    after_date = datetime.strptime(date_str, "%Y-%m-%d")
    next_day = (after_date + timedelta(days=1)).strftime("%Y-%m-%d")
    window_end = (after_date + timedelta(days=60)).strftime("%Y-%m-%d")

    try:
        response = requests.get(
            f"{CHECKFRONT_BASE_URL}/booking",
            auth=(CHECKFRONT_API_KEY, CHECKFRONT_API_SECRET),
            params={"start_date": next_day, "end_date": window_end, "limit": 100},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    bookings_raw = data.get("booking/index", data.get("booking", {}))
    bookings = list(bookings_raw.values()) if isinstance(bookings_raw, dict) else (bookings_raw or [])

    result = []
    for b in bookings:
        bsummary = extract_cabin_summary(b)
        configs = get_cabin_configs_from_summary(bsummary)
        result.append({
            "booking_id": b.get("booking_id"),
            "code": b.get("code"),
            "status": b.get("status_id"),
            "start_date": b.get("start_date"),
            "end_date": b.get("end_date"),
            "summary": bsummary,
            "cabins_matched": [c["label"] for c in configs],
            "active": b.get("status_id", "") in ACTIVE_STATUSES
        })

    cabin_filtered = []
    if cabin_param and cabin_param in CABIN_MAP:
        target = CABIN_MAP[cabin_param]["process"]
        cabin_filtered = [r for r in result if any(
            c["process"] == target
            for c in get_cabin_configs_from_summary(r["summary"])
        )]

    # Fetch detail for each ACTIVE cabin-matched booking to show real check-in dates
    cabin_detail_dates = []
    for b in cabin_filtered:
        if not b.get("active"):
            continue
        bid = b.get("booking_id")
        try:
            d = get_checkfront_booking_detail(bid)
            raw_start = d.get("start_date", "")
            parsed_start = _parse_checkfront_date(raw_start)
            # Twin share is in the list-level summary, not in detail items
            list_summary = b.get("summary", "")
            has_twin_share = "twin share configuration" in list_summary.lower()
            cabin_detail_dates.append({
                "booking_id": bid,
                "code": b.get("code"),
                "status": b.get("status"),
                "checkin": str(parsed_start) if parsed_start else None,
                "summary": list_summary,
                "has_twin_share": has_twin_share,
            })
        except Exception as e:
            cabin_detail_dates.append({"booking_id": bid, "error": str(e)})

    return jsonify({
        "query_params": {"start_date": next_day, "end_date": window_end},
        "total_returned": len(bookings),
        "cabin_filter": cabin_param or "none — add ?cabin=echidna to filter",
        "cabin_active_detail": cabin_detail_dates
    })


@app.route("/debug-items", methods=["GET"])
def debug_items_endpoint():
    """
    Temporary diagnostic: lists Checkfront catalog items, optionally filtered
    by category_id, to confirm exact bed-configuration item naming.
    Usage: /debug-items or /debug-items?category_id=19
    """
    category_id = request.args.get("category_id")
    params = {"limit": 100}
    if category_id:
        params["category_id"] = category_id
    try:
        response = requests.get(
            f"{CHECKFRONT_BASE_URL}/item",
            auth=(CHECKFRONT_API_KEY, CHECKFRONT_API_SECRET),
            params=params,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if request.args.get("raw") == "true":
        return jsonify(data)

    items_raw = data.get("items", data.get("item/index", data.get("item", {})))
    items = list(items_raw.values()) if isinstance(items_raw, dict) else (items_raw or [])

    match = request.args.get("match", "").lower()
    if match:
        items = [i for i in items if match in (i.get("name") or "").lower()]

    summary = [
        {
            "id": i.get("item_id") or i.get("id"),
            "name": i.get("name"),
            "sku": i.get("sku"),
            "category_id": i.get("category_id"),
            "category": i.get("category"),
        }
        for i in items
    ]
    return jsonify({"count": len(summary), "items": summary})


@app.route("/debug-raw", methods=["GET"])
def debug_raw_endpoint():
    """
    Temporary diagnostic: returns the raw Checkfront booking detail JSON
    for a given booking_id, to confirm exact item naming/format.
    Usage: /debug-raw?booking_id=1397
    """
    booking_id = request.args.get("booking_id")
    if not booking_id:
        return jsonify({"error": "booking_id required"}), 400
    try:
        detail = get_checkfront_booking_detail(booking_id)
        return jsonify(detail)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
