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
from datetime import datetime
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
ACTIVE_STATUSES = {"PAID", "PART", "HOLD", "PEND", "OTACC", "OTAPP"}

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

def get_checkfront_checkouts(date_str):
    """
    Query Checkfront for bookings where end_date = date_str (checkouts).
    Returns list of active booking dicts.
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
    active = [b for b in bookings if b.get("status_id", "") in ACTIVE_STATUSES]
    logger.info(f"Found {len(active)} active checkout bookings (filtered from {len(bookings)} total)")
    return active


def extract_cabin_summary(booking):
    """Extract the summary string (which may list multiple cabins) from a booking."""
    summary = booking.get("summary", "")
    if summary:
        return summary
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


def create_flip_checkout_job(token, cabin_config, date_str, guest_name=""):
    """
    Create a Flip Checkout job for a cabin, dated for the checkout day.
    Returns (job_id, title).
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
    logger.info(f"Flip checkout job ready: '{title}'")
    return job_id, title


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
                    logger.warning(
                        f"Skipping booking {booking_id} - no recognised cabins in summary: '{summary}'"
                    )
                    results["errors"].append(
                        f"No cabins matched for booking {booking_id}: '{summary}'"
                    )
                    continue

                for cabin_config in cabin_configs:
                    try:
                        job_id, title = create_flip_checkout_job(
                            token=token,
                            cabin_config=cabin_config,
                            date_str=today_str,
                            guest_name=guest_name
                        )
                        results["checkouts"].append({
                            "booking_id": booking_id,
                            "cabin": cabin_config["label"],
                            "guest": guest_name,
                            "job_id": job_id,
                            "title": title
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
            checkouts = get_checkfront_checkouts(today_str)
            preview = []
            for b in checkouts:
                summary = extract_cabin_summary(b)
                configs = get_cabin_configs_from_summary(summary)
                preview.append({
                    "booking_id": b.get("booking_id"),
                    "code": b.get("code"),
                    "guest": b.get("customer_name"),
                    "status": b.get("status_id"),
                    "summary": summary,
                    "cabins_matched": [c["label"] for c in configs]
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
