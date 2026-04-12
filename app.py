"""
Maiala Park Lodge - Checkfront to Operandio Housekeeping Integration
Receives Checkfront booking webhooks, uses Claude to generate housekeeping jobs,
then creates them in Operandio via GraphQL API.
"""

import os
import json
import logging
import requests
from flask import Flask, request, jsonify
from datetime import datetime, timedelta

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
OPERANDIO_USERNAME = os.environ.get("OPERANDIO_USERNAME")
OPERANDIO_PASSWORD = os.environ.get("OPERANDIO_PASSWORD")
OPERANDIO_JOB_TEMPLATE_ID = os.environ.get("OPERANDIO_JOB_TEMPLATE_ID", "68fff5390c79dcfdc46f0cc1")
OPERANDIO_GROUP_ID = os.environ.get("OPERANDIO_GROUP_ID", "6875e9afe3b2fa6732104a84")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
OPERANDIO_AUTH_URL = "https://api.operandio.com/auth/oauth2/token"
OPERANDIO_GRAPHQL_URL = "https://api.operandio.com/graphql"

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

    # Extract booking fields safely
    guest_first = booking_data.get("fields", {}).get("customer_first_name", "")
    guest_last = booking_data.get("fields", {}).get("customer_last_name", "")
    guest_name = f"{guest_first} {guest_last}".strip() or booking_data.get("customer_name", "Guest")
    
    # Handle items/cabin name
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
            "model": "claude-sonnet-4-5-20250929",
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

    # Strip markdown code fences and extract JSON robustly
    content = content.strip()
    # Remove opening fence (e.g. ```json or ```)
    if content.startswith("```"):
        content = content.split(chr(10), 1)[1]
    # Remove closing fence
    if content.strip().endswith("```"):
        content = content.rsplit("```", 1)[0]
    content = content.strip()

    # Find JSON object boundaries as fallback
    if not content.startswith("{"):
        start = content.find("{")
        end = content.rfind("}") + 1
        if start != -1 and end > start:
            content = content[start:end]

    # Parse JSON response
    jobs = json.loads(content)
    logger.info(f"Successfully parsed Claude response: {jobs}")
    return jobs


def format_date_for_operandio(date_str):
    """Convert YYYY-MM-DD to ISO 8601 with timezone for Operandio."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        # Use Australian Eastern time (UTC+10)
        return dt.strftime("%Y-%m-%dT00:00:00+10:00")
    except (ValueError, TypeError):
        logger.warning(f"Could not parse date: {date_str}, using as-is")
        return date_str


def create_operandio_job(token, title, content, priority, due_at):
    """Create a single job in Operandio via GraphQL."""
    logger.info(f"Creating Operandio job: {title} due {due_at}")

    formatted_date = format_date_for_operandio(due_at)

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
            "job": OPERANDIO_JOB_TEMPLATE_ID,
            "priority": priority,
            "status": "unresolved",
            "dueAt": formatted_date,
            "groups": [OPERANDIO_GROUP_ID],
            "requirePhotoWhenResolving": True
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
        logger.error(f"Request payload: job={OPERANDIO_JOB_TEMPLATE_ID}, group={OPERANDIO_GROUP_ID}, dueAt={formatted_date}")
    response.raise_for_status()
    result = response.json()

    if "errors" in result:
        raise Exception(f"GraphQL errors: {result['errors']}")

    job_id = result["data"]["createJobAction"]["id"]
    logger.info(f"Created Operandio job: {job_id} - {title}")
    return job_id


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "service": "Maiala Park Lodge Integration"})


@app.route("/webhook/checkfront", methods=["POST"])
def handle_checkfront_webhook():
    """Main webhook handler for Checkfront bookings."""
    logger.info("Received Checkfront webhook")

    # Parse payload
    try:
        if request.content_type and "application/json" in request.content_type:
            booking_data = request.get_json()
        else:
            # Checkfront may send form-encoded data
            raw_data = request.form.get("payload") or request.data.decode("utf-8")
            booking_data = json.loads(raw_data)
    except Exception as e:
        logger.error(f"Failed to parse webhook payload: {e}")
        return jsonify({"error": "Invalid payload"}), 400

    logger.info(f"Booking data received: {json.dumps(booking_data, indent=2)[:500]}")

    try:
        # Step 1: Generate jobs with Claude
        jobs = generate_jobs_with_claude(booking_data)

        # Step 2: Get Operandio auth token
        token = get_operandio_token()

        # Step 3: Create checkout clean job
        checkout_job_id = create_operandio_job(
            token=token,
            title=jobs["title"],
            content=jobs["content"],
            priority=jobs["priority"],
            due_at=jobs["dueAt"]
        )

        # Step 4: Create pre-arrival clean job
        prearrival_job_id = create_operandio_job(
            token=token,
            title=jobs["preArrivalTitle"],
            content=jobs["preArrivalContent"],
            priority=jobs["priority"],
            due_at=jobs["preArrivalDueAt"]
        )

        logger.info(f"Successfully created both jobs: checkout={checkout_job_id}, prearrival={prearrival_job_id}")

        return jsonify({
            "success": True,
            "checkout_job_id": checkout_job_id,
            "prearrival_job_id": prearrival_job_id,
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
    sample_booking = {
        "code": "TEST-001",
        "fields": {
            "customer_first_name": "Jane",
            "customer_last_name": "Smith"
        },
        "items": [{"name": "Kookaburra Suite"}],
        "checkIn": "2026-05-29",
        "checkOut": "2026-05-31",
        "slots": 2,
        "notes": "Celebrating anniversary"
    }

    try:
        jobs = generate_jobs_with_claude(sample_booking)
        token = get_operandio_token()

        checkout_job_id = create_operandio_job(
            token=token,
            title=jobs["title"],
            content=jobs["content"],
            priority=jobs["priority"],
            due_at=jobs["dueAt"]
        )

        prearrival_job_id = create_operandio_job(
            token=token,
            title=jobs["preArrivalTitle"],
            content=jobs["preArrivalContent"],
            priority=jobs["priority"],
            due_at=jobs["preArrivalDueAt"]
        )

        return jsonify({
            "success": True,
            "test_booking": sample_booking,
            "claude_output": jobs,
            "checkout_job_id": checkout_job_id,
            "prearrival_job_id": prearrival_job_id
        }), 200

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
