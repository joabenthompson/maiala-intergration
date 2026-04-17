"""
Maiala Park Lodge - Checkfront to Operandio Housekeeping Integration
Receives Checkfront booking webhooks, uses Claude to generate housekeeping jobs,
then creates them in Operandio via GraphQL API.

Flow:
  1. Receive Checkfront webhook
  2. Claude generates job titles/content/dates
  3. For each job: process.run() → get job instance ID
  4. createJobAction() on that job instance
"""

import os
import json
import logging
import requests
from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
OPERANDIO_USERNAME = os.environ.get("OPERANDIO_USERNAME")
OPERANDIO_PASSWORD = os.environ.get("OPERANDIO_PASSWORD")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
OPERANDIO_AUTH_URL = "https://api.operandio.com/auth/oauth2/token"
OPERANDIO_GRAPHQL_URL = "https://api.operandio.com/graphql"
OPERANDIO_LOCATION_ID = "6875e8527e4d972e36fe8073"  # Maiala Park Lodge

# Map cabin names to their Operandio process ID and schedule ID
CABIN_MAP = {
    "kookaburra":         {"process": "68fff4c0b8d923384331984e", "schedule": "68fff4c0b8d9233843319835"},
    "kookaburra suite":   {"process": "68fff4c0b8d923384331984e", "schedule": "68fff4c0b8d9233843319835"},
    "pademelon":          {"process": "68fff4ecf0cdee421c13570a", "schedule": "68fff4ecf0cdee421c1356f1"},
    "pademelon suite":    {"process": "68fff4ecf0cdee421c13570a", "schedule": "68fff4ecf0cdee421c1356f1"},
    "echidna":            {"process": "68fff515c1c4d200fadf3b0d", "schedule": "68fff515c1c4d200fadf3af4"},
    "echidna suite":      {"process": "68fff515c1c4d200fadf3b0d", "schedule": "68fff515c1c4d200fadf3af4"},
    "cockatoo":           {"process": "688f3858ae37bb646c829bf6", "schedule": "688f3858ae37bb646c829bde"},
    "cockatoo suite":     {"process": "688f3858ae37bb646c829bf6", "schedule": "688f3858ae37bb646c829bde"},
    "bowerbird":          {"process": "68fff5390c79dcfdc46f0cc1", "schedule": "68fff5390c79dcfdc46f0ca8"},
    "bowerbird cottage":  {"process": "68fff5390c79dcfdc46f0cc1", "schedule": "68fff5390c79dcfdc46f0ca8"},
}

DEFAULT_CABIN = CABIN_MAP["bowerbird cottage"]


def get_cabin_config(cabin_name):
    """Return process/schedule IDs for the given cabin name."""
    if not cabin_name:
        logger.warning("No cabin name provided, using default")
        return DEFAULT_CABIN
    key = cabin_name.strip().lower()
    if key in CABIN_MAP:
        logger.info(f"Exact match: '{cabin_name}'")
        return CABIN_MAP[key]
    for cabin_key, config in CABIN_MAP.items():
        if cabin_key in key:
            logger.info(f"Partial match: '{cabin_name}' matched '{cabin_key}'")
            return config
    logger.warning(f"No match for cabin '{cabin_name}', using default")
    return DEFAULT_CABIN


SYSTEM_PROMPT = """You are a housekeeping operations assistant for Maiala Park Lodge in Queensland, Australia.
When given a Checkfront booking, generate two housekeeping jobs.
Return ONLY a single flat JSON object with no markdown, no code blocks, no arrays.
Use exactly these fields:
- title: checkout clean job title including cabin name e.g. "Checkout Clean - Kookaburra Suite"
- content: detailed checkout housekeeping instructions based on length of stay and guest count
- priority: exactly one of: low, medium, high (use high if 4+ nights or 4+ guests)
- dueAt: checkout date in YYYY-MM-DD format
- preArrivalTitle: pre-arrival clean job title e.g. "Pre-Arrival Clean - Kookaburra Suite"
- preArrivalContent: detailed pre-arrival clean instructions to prepare cabin for incoming guests
- preArrivalDueAt: the day before checkin date in YYYY-MM-DD format"""


def get_operandio_token():
    """Get Operandio OAuth bearer token."""
    logger.info("Getting Operandio auth token...")
    response = requests.post(
        OPERANDIO_AUTH_URL,
        data={"grant_type": "client_credentials"},
        auth=(OPERANDIO_USERNAME, OPERANDIO_PASSWORD),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    logger.info("Operandio token obtained successfully")
    return token


def generate_jobs_with_claude(booking_data):
    """Use Claude API to generate housekeeping job details from booking data."""
    logger.info(f"Generating jobs with Claude for booking: {booking_data.get('code', 'unknown')}")

    guest_first = booking_data.get("fields", {}).get("customer_first_name", "")
    guest_last = booking_data.get("fields", {}).get("customer_last_name", "")
    guest_name = f"{guest_first} {guest_last}".strip() or booking_data.get("customer_name", "Guest")

    items = booking_data.get("items", [])
    if isinstance(items, list) and items:
        cabin = items[0].get("name", "Lodge")
    else:
        cabin = booking_data.get("item_name", "Lodge")

    check_in = booking_data.get("checkIn", booking_data.get("start_date", ""))
    check_out = booking_data.get("checkOut", booking_data.get("end_date", ""))
    booking_ref = booking_data.get("code", "")
    guest_count = booking_data.get("slots", booking_data.get("guest_count", ""))
    notes = booking_data.get("notes", "")

    user_message = f"""New booking received:
Guest: {guest_name}
Cabin: {cabin}
Check-in: {check_in}
Check-out: {check_out}
Booking ref: {booking_ref}
Guest count: {guest_count}
Guest notes: {notes}

Generate the housekeeping jobs for this booking."""

    response = requests.post(
        ANTHROPIC_API_URL,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01"
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 800,
            "temperature": 0.3,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_message}]
        },
        timeout=60
    )
    response.raise_for_status()

    content = response.json()["content"][0]["text"]
    logger.info(f"Claude response: {content}")

    content = content.strip()
    if content.startswith("```"):
        content = content.split(chr(10), 1)[1]
    if content.strip().endswith("```"):
        content = content.rsplit("```", 1)[0]
    content = content.strip()
    if not content.startswith("{"):
        start = content.find("{")
        end = content.rfind("}") + 1
        if start != -1 and end > start:
            content = content[start:end]

    jobs = json.loads(content)
    logger.info(f"Successfully parsed Claude response: {jobs}")
    return jobs


def format_date_for_operandio(date_str):
    """Return date in YYYY-MM-DD format for Operandio."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except (ValueError, TypeError):
        logger.warning(f"Could not parse date: {date_str}, using as-is")
        return date_str


def run_process(token, process_id, schedule_id):
    """Step 1: Run a process to create a job instance. Returns job instance ID."""
    logger.info(f"Running process {process_id} with schedule {schedule_id}")

    mutation = """
    mutation RunProcess($processId: ID!, $scheduleId: ID!, $locationId: ID!) {
        process(id: $processId) {
            run(schedule: $scheduleId, location: $locationId) {
                id
                processName
            }
        }
    }
    """

    variables = {
        "processId": process_id,
        "scheduleId": schedule_id,
        "locationId": OPERANDIO_LOCATION_ID
    }

    response = requests.post(
        OPERANDIO_GRAPHQL_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        },
        json={"query": mutation, "variables": variables},
        timeout=30
    )
    response.raise_for_status()
    result = response.json()

    if "errors" in result:
        raise Exception(f"GraphQL errors running process: {result['errors']}")

    job_id = result["data"]["process"]["run"]["id"]
    process_name = result["data"]["process"]["run"]["processName"]
    logger.info(f"Created job instance: {job_id} ({process_name})")
    return job_id


def create_operandio_job_action(token, job_instance_id, title, content, priority, due_at):
    """Step 2: Create a job action on an existing job instance."""
    logger.info(f"Creating job action '{title}' on job {job_instance_id} due {due_at}")

    formatted_date = format_date_for_operandio(due_at)

    priority_map = {
        "low": "low",
        "medium": "normal",
        "normal": "normal",
        "high": "high",
        "critical": "critical"
    }
    mapped_priority = priority_map.get(priority.lower(), "normal")

    mutation = """
    mutation CreateJobAction($input: JobActionInput!) {
        createJobAction(input: $input) {
            id
            title
        }
    }
    """

    variables = {
        "input": {
            "title": title,
            "content": content,
            "job": job_instance_id,
            "priority": mapped_priority,
            "dueAt": formatted_date
        }
    }

    response = requests.post(
        OPERANDIO_GRAPHQL_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        },
        json={"query": mutation, "variables": variables},
        timeout=30
    )
    if not response.ok:
        logger.error(f"Operandio error {response.status_code}: {response.text}")
    response.raise_for_status()
    result = response.json()

    if "errors" in result:
        raise Exception(f"GraphQL errors creating job action: {result['errors']}")

    action_id = result["data"]["createJobAction"]["id"]
    logger.info(f"Created job action: {action_id} - {title}")
    return action_id


def create_housekeeping_job(token, cabin_config, title, content, priority, due_at):
    """Full two-step flow: run process → create job action. Returns action ID."""
    job_instance_id = run_process(
        token=token,
        process_id=cabin_config["process"],
        schedule_id=cabin_config["schedule"]
    )
    action_id = create_operandio_job_action(
        token=token,
        job_instance_id=job_instance_id,
        title=title,
        content=content,
        priority=priority,
        due_at=due_at
    )
    return action_id


def extract_cabin_name(booking_data):
    """Extract cabin name from booking data."""
    items = booking_data.get("items", [])
    if isinstance(items, list) and items:
        return items[0].get("name", "")
    return booking_data.get("item_name", "")


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "Maiala Park Lodge Integration"})


@app.route("/webhook/checkfront", methods=["POST"])
def handle_checkfront_webhook():
    """Main webhook handler for Checkfront bookings."""
    logger.info("Received Checkfront webhook")

    try:
        if request.content_type and "application/json" in request.content_type:
            booking_data = request.get_json()
        else:
            raw_data = request.form.get("payload") or request.data.decode("utf-8")
            booking_data = json.loads(raw_data)
    except Exception as e:
        logger.error(f"Failed to parse webhook payload: {e}")
        return jsonify({"error": "Invalid payload"}), 400

    logger.info(f"Booking data received: {json.dumps(booking_data, indent=2)[:500]}")

    try:
        cabin_name = extract_cabin_name(booking_data)
        cabin_config = get_cabin_config(cabin_name)
        logger.info(f"Cabin '{cabin_name}' → process {cabin_config['process']}")

        jobs = generate_jobs_with_claude(booking_data)
        token = get_operandio_token()

        checkout_action_id = create_housekeeping_job(
            token=token,
            cabin_config=cabin_config,
            title=jobs["title"],
            content=jobs["content"],
            priority=jobs["priority"],
            due_at=jobs["dueAt"]
        )

        prearrival_action_id = create_housekeeping_job(
            token=token,
            cabin_config=cabin_config,
            title=jobs["preArrivalTitle"],
            content=jobs["preArrivalContent"],
            priority=jobs["priority"],
            due_at=jobs["preArrivalDueAt"]
        )

        logger.info(f"Success! checkout={checkout_action_id}, prearrival={prearrival_action_id}")

        return jsonify({
            "success": True,
            "cabin": cabin_name,
            "checkout_action_id": checkout_action_id,
            "prearrival_action_id": prearrival_action_id,
            "checkout_title": jobs["title"],
            "prearrival_title": jobs["preArrivalTitle"]
        }), 200

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Claude response as JSON: {e}")
        return jsonify({"error": "Claude returned invalid JSON", "detail": str(e)}), 500
    except requests.HTTPError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
        return jsonify({"error": "API request failed", "detail": str(e)}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return jsonify({"error": "Internal server error", "detail": str(e)}), 500


@app.route("/test", methods=["GET", "POST"])
def test_with_sample():
    """Test endpoint with sample booking data."""
    cabin_override = request.args.get("cabin", "Kookaburra Suite")

    sample_booking = {
        "code": "TEST-001",
        "fields": {
            "customer_first_name": "Jane",
            "customer_last_name": "Smith"
        },
        "items": [{"name": cabin_override}],
        "checkIn": "2026-05-29",
        "checkOut": "2026-05-31",
        "slots": 2,
        "notes": "Celebrating anniversary"
    }

    try:
        cabin_name = extract_cabin_name(sample_booking)
        cabin_config = get_cabin_config(cabin_name)

        jobs = generate_jobs_with_claude(sample_booking)
        token = get_operandio_token()

        checkout_action_id = create_housekeeping_job(
            token=token,
            cabin_config=cabin_config,
            title=jobs["title"],
            content=jobs["content"],
            priority=jobs["priority"],
            due_at=jobs["dueAt"]
        )

        prearrival_action_id = create_housekeeping_job(
            token=token,
            cabin_config=cabin_config,
            title=jobs["preArrivalTitle"],
            content=jobs["preArrivalContent"],
            priority=jobs["priority"],
            due_at=jobs["preArrivalDueAt"]
        )

        return jsonify({
            "success": True,
            "cabin": cabin_name,
            "process_id": cabin_config["process"],
            "test_booking": sample_booking,
            "claude_output": jobs,
            "checkout_action_id": checkout_action_id,
            "prearrival_action_id": prearrival_action_id
        }), 200

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
