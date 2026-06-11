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


def get_checkfront_future_bookings(cabin_config, after_date_str):
    """
    Query Checkfront for active bookings in a specific cabin that start after
    after_date_str (YYYY-MM-DD). Uses a 365-day window. Returns a list of booking
    dicts sorted by start_date ascending.
    """
    after_date = datetime.strptime(after_date_str, "%Y-%m-%d")
    next_day = (after_date + timedelta(days=1)).strftime("%Y-%m-%d")
    window_end = (after_date + timedelta(days=365)).strftime("%Y-%m-%d")

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
    cabin_bookings = []
    for b in active:
        summary = extract_cabin_summary(b)
        configs = get_cabin_configs_from_summary(summary)
        if any(c["process"] == target_process for c in configs):
            cabin_bookings.append(b)

    def _sort_key(b):
        raw = b.get("start_date", "")
        if isinstance(raw, int):
            return raw
        try:
            return datetime.strptime(raw, "%Y-%m-%d").timestamp()
        except Exception:
            return 0

    cabin_bookings.sort(key=_sort_key)
    return cabin_bookings


def get_bed_note_for_next_booking(cabin_config, checkout_date_str):
    """
    Look up the next active booking for cabin_config after checkout_date_str.
    If it includes a 'Twin Share Configuration' add-on, return the appropriate
    bed note string. Returns None if no note is needed.
    """
    future_bookings = get_checkfront_future_bookings(cabin_config, checkout_date_str)
    if not future_bookings:
        return None

    next_booking = future_bookings[0]
    next_booking_id = next_booking.get("booking_id") or next_booking.get("code", "")
    logger.info(f"Next booking for {cabin_config['label']}: {next_booking_id}")

    detail = get_checkfront_booking_detail(next_booking_id)
    items = detail.get("items", {})
    item_list = items.values() if isinstance(items, dict) else (items or [])

    has_twin_share = any(
        "twin share configuration" in (item.get("summary", "") or "").lower()
        for item in item_list
    )
    if not has_twin_share:
        logger.info(f"No twin share configuration in next booking {next_booking_id}")
        return None

    raw_checkin = detail.get("start_date") or next_booking.get("start_date", "")
    if isinstance(raw_checkin, int):
        next_checkin_date = datetime.fromtimestamp(raw_checkin)
    else:
        next_checkin_date = datetime.strptime(raw_checkin, "%Y-%m-%d")

    checkout_date = datetime.strptime(checkout_date_str, "%Y-%m-%d")
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
    window_end = (after_date + timedelta(days=365)).strftime("%Y-%m-%d")

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

    return jsonify({
        "query_params": {"start_date": next_day, "end_date": window_end},
        "total_returned": len(bookings),
        "bookings": result[:30],
        "cabin_filter": cabin_param or "none — add ?cabin=echidna to filter",
        "cabin_matched": cabin_filtered
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
