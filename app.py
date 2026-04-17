"""
Maiala Park Lodge - Daily Housekeeping Job Creator
Runs at 12:01am AEST each day via Render Cron Job.

Flow:
  1. Query Checkfront for today's checkouts → create Flip (Checkout) jobs in Operandio
  2. Query Checkfront for tomorrow's check-ins → create Flip (Pre-Arrival) jobs in Operandio
"""

import os
import json
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

# Protect the /run-daily endpoint with a secret
CRON_SECRET = os.environ.get("CRON_SECRET", "")

AEST = ZoneInfo("Australia/Brisbane")

# Bookings with these statuses are considered active
ACTIVE_STATUSES = {"PAID", "PART", "HOLD", "PEND"}

# Map Checkfront item names (lowercase) to Operandio process + schedule IDs
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

DEFAULT_CABIN = CABIN_MAP["bowerbird cottage"]


def get_cabin_config(item_name):
    """Match a Checkfront item name to an Operandio cabin config."""
    if not item_name:
        return None
    key = item_name.strip().lower()
    if key in CABIN_MAP:
        return CABIN_MAP[key]
    for cabin_key, config in CABIN_MAP.items():
        if cabin_key in key:
            logger.info(f"Partial match: '{item_name}' → '{cabin_key}'")
            return config
    logger.warning(f"No cabin match for item '{item_name}'")
    return None


# ---------------------------------------------------------------------------
# Checkfront
# ---------------------------------------------------------------------------

def get_checkfront_bookings(date_str, date_field):
    """
    Query Checkfront for bookings on a specific date.
    date_field: 'start_date' (check-in) or 'end_date' (check-out)
    Returns list of active booking dicts.
    """
    logger.info(f"Querying Checkfront: {date_field}={date_str}")
    response = requests.get(
        f"{CHECKFRONT_BASE_URL}/booking",
        auth=(CHECKFRONT_API_KEY, CHECKFRONT_API_SECRET),
        params={
            date_field: date_str,
            "status_id": ",".join(ACTIVE_STATUSES),
            "limit": 100
        },
        timeout=30
    )
    response.raise_for_status()
    data = response.json()

    bookings_raw = data.get("booking/index", data.get("booking", {}))
    if not bookings_raw:
        logger.info(f"No bookings found for {date_field}={date_str}")
        return []

    bookings = list(bookings_raw.values()) if isinstance(bookings_raw, dict) else bookings_raw
    active = [b for b in bookings if b.get("status_id", "") in ACTIVE_STATUSES]
    logger.info(f"Found {len(active)} active bookings for {date_field}={date_str}")
    return active


def get_booking_detail(booking_id):
    """Get full booking detail including items/cabin name."""
    response = requests.get(
        f"{CHECKFRONT_BASE_URL}/booking/{booking_id}",
        auth=(CHECKFRONT_API_KEY, CHECKFRONT_API_SECRET),
        timeout=30
    )
    response.raise_for_status()
    data = response.json()
    return data.get("booking", {})


def extract_cabin_from_booking(booking):
    """Extract cabin name from a Checkfront booking dict."""
    # Try summary field first
    summary = booking.get("summary", "")
    if summary:
        return summary.split(",")[0].strip()
    return booking.get("item_name", "")


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


def create_flip_job(token, cabin_config, date_str, job_type, guest_name=""):
    """
    Create a Flip job for a cabin.
    job_type: 'Checkout' or 'Pre-Arrival'
    Returns (job_id, title).
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        date_label = date_obj.strftime("%-d %B %Y")
    except Exception:
        date_label = date_str

    guest_part = f" ({guest_name})" if guest_name else ""
    title = f"Flip {cabin_config['label']} - {job_type} {date_label}{guest_part}"

    job_id = run_process(token, cabin_config["process"], cabin_config["schedule"])
    update_job_title(token, job_id, title)
    logger.info(f"Flip job ready: '{title}'")
    return job_id, title


# ---------------------------------------------------------------------------
# Core daily logic
# ---------------------------------------------------------------------------

def run_daily_jobs(today_str, tomorrow_str):
    """
    Main logic: query Checkfront and create Operandio jobs.
    Returns a summary dict.
    """
    results = {
        "date": today_str,
        "checkouts": [],
        "pre_arrivals": [],
        "errors": []
    }

    token = get_operandio_token()
    logger.info(f"Running daily jobs for {today_str} (checkouts) / {tomorrow_str} (pre-arrivals)")

    # --- Checkouts (end_date = today) ---
    try:
        checkouts = get_checkfront_bookings(today_str, "end_date")
        for booking in checkouts:
            try:
                booking_id = booking.get("booking_id") or booking.get("code", "")
                guest_name = booking.get("customer_name", "")
                cabin_name = extract_cabin_from_booking(booking)
                cabin_config = get_cabin_config(cabin_name)

                if not cabin_config:
                    logger.warning(f"Skipping booking {booking_id} - unknown cabin: '{cabin_name}'")
                    results["errors"].append(f"Unknown cabin '{cabin_name}' for booking {booking_id}")
                    continue

                job_id, title = create_flip_job(
                    token=token,
                    cabin_config=cabin_config,
                    date_str=today_str,
                    job_type="Checkout",
                    guest_name=guest_name
                )
                results["checkouts"].append({
                    "booking_id": booking_id,
                    "cabin": cabin_name,
                    "guest": guest_name,
                    "job_id": job_id,
                    "title": title
                })
            except Exception as e:
                logger.error(f"Error processing checkout booking {booking.get('booking_id')}: {e}")
                results["errors"].append(str(e))
    except Exception as e:
        logger.error(f"Error fetching checkouts: {e}")
        results["errors"].append(f"Checkout fetch error: {e}")

    # --- Pre-arrivals (start_date = tomorrow) ---
    try:
        pre_arrivals = get_checkfront_bookings(tomorrow_str, "start_date")
        for booking in pre_arrivals:
            try:
                booking_id = booking.get("booking_id") or booking.get("code", "")
                guest_name = booking.get("customer_name", "")
                cabin_name = extract_cabin_from_booking(booking)
                cabin_config = get_cabin_config(cabin_name)

                if not cabin_config:
                    logger.warning(f"Skipping booking {booking_id} - unknown cabin: '{cabin_name}'")
                    results["errors"].append(f"Unknown cabin '{cabin_name}' for booking {booking_id}")
                    continue

                job_id, title = create_flip_job(
                    token=token,
                    cabin_config=cabin_config,
                    date_str=tomorrow_str,
                    job_type="Pre-Arrival",
                    guest_name=guest_name
                )
                results["pre_arrivals"].append({
                    "booking_id": booking_id,
                    "cabin": cabin_name,
                    "guest": guest_name,
                    "job_id": job_id,
                    "title": title
                })
            except Exception as e:
                logger.error(f"Error processing pre-arrival booking {booking.get('booking_id')}: {e}")
                results["errors"].append(str(e))
    except Exception as e:
        logger.error(f"Error fetching pre-arrivals: {e}")
        results["errors"].append(f"Pre-arrival fetch error: {e}")

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
    Called by Render Cron Job at 2:01am UTC (= 12:01am AEST).
    Protected by CRON_SECRET header.
    """
    # Verify secret if configured
    if CRON_SECRET:
        auth = request.headers.get("X-Cron-Secret", "")
        if auth != CRON_SECRET:
            return jsonify({"error": "Unauthorized"}), 401

    now_aest = datetime.now(AEST)
    today_str = now_aest.strftime("%Y-%m-%d")
    tomorrow_str = (now_aest + timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        results = run_daily_jobs(today_str, tomorrow_str)
        total = len(results["checkouts"]) + len(results["pre_arrivals"])
        logger.info(f"Daily run complete: {total} jobs created, {len(results['errors'])} errors")
        return jsonify({"success": True, **results}), 200
    except Exception as e:
        logger.error(f"Daily run failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/test", methods=["GET", "POST"])
def test_endpoint():
    """
    Test endpoint — pass ?date=YYYY-MM-DD to simulate a specific day,
    or leave blank to use today AEST.
    Pass ?dry_run=true to query Checkfront without creating Operandio jobs.
    """
    date_param = request.args.get("date")
    dry_run = request.args.get("dry_run", "false").lower() == "true"

    if date_param:
        try:
            today = datetime.strptime(date_param, "%Y-%m-%d")
        except ValueError:
            return jsonify({"error": "Invalid date format, use YYYY-MM-DD"}), 400
    else:
        today = datetime.now(AEST)

    today_str = today.strftime("%Y-%m-%d")
    tomorrow_str = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    if dry_run:
        # Just show what Checkfront returns without touching Operandio
        try:
            checkouts = get_checkfront_bookings(today_str, "end_date")
            pre_arrivals = get_checkfront_bookings(tomorrow_str, "start_date")
            return jsonify({
                "dry_run": True,
                "today": today_str,
                "tomorrow": tomorrow_str,
                "checkouts_found": len(checkouts),
                "pre_arrivals_found": len(pre_arrivals),
                "checkouts": checkouts,
                "pre_arrivals": pre_arrivals
            }), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    try:
        results = run_daily_jobs(today_str, tomorrow_str)
        return jsonify({"success": True, **results}), 200
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
