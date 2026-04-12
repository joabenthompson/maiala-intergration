# Maiala Park Lodge - Checkfront to Operandio Integration

Automatically creates housekeeping jobs in Operandio when bookings are confirmed in Checkfront, using Claude AI to generate intelligent job descriptions.

## How It Works

1. Checkfront sends a webhook when a new booking is created
2. Claude analyses the booking and generates two housekeeping jobs (checkout clean + pre-arrival clean)
3. The script creates both jobs in Operandio via the GraphQL API

## Deployment on Render.com (Free)

### Step 1 - Create a GitHub repository
1. Go to github.com and create a new repository called `maiala-integration`
2. Upload all three files: `app.py`, `requirements.txt`, `render.yaml`

### Step 2 - Deploy on Render
1. Go to render.com and sign up for a free account
2. Click **New** → **Web Service**
3. Connect your GitHub repository
4. Render will auto-detect the `render.yaml` configuration
5. Click **Create Web Service**

### Step 3 - Add environment variables
In your Render dashboard, go to **Environment** and add:

| Variable | Value |
|----------|-------|
| `ANTHROPIC_API_KEY` | Your Anthropic API key (sk-ant-...) |
| `OPERANDIO_USERNAME` | Your Operandio login email |
| `OPERANDIO_PASSWORD` | Your Operandio password |
| `OPERANDIO_JOB_TEMPLATE_ID` | `/templates/active/68fff5390c79dcfdc46f0cc1` |
| `OPERANDIO_GROUP_ID` | `/account/groups/6875e9afe3b2fa6732104a84` |

### Step 4 - Test the integration
Once deployed, your service URL will be something like:
`https://maiala-housekeeping-integration.onrender.com`

Test it by calling the test endpoint:
```
POST https://maiala-housekeeping-integration.onrender.com/test
```

This will create two real test jobs in Operandio using sample booking data.

### Step 5 - Configure Checkfront webhook
1. Log into Checkfront
2. Go to **Manage → Developer → Webhooks**
3. Add a new webhook with:
   - URL: `https://maiala-housekeeping-integration.onrender.com/webhook/checkfront`
   - Trigger: **Booking Created**
4. Save

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/webhook/checkfront` | POST | Main webhook receiver |
| `/test` | POST | Test with sample booking data |

## Job Types Created

For every booking, the integration creates:
- **Checkout Clean** — scheduled for the checkout date
- **Pre-Arrival Clean** — scheduled for the day before check-in

Both jobs are assigned to your housekeeping staff group in Operandio with photo required on completion.

## Costs

- **Render.com** free tier: $0/month (750 hours free)
- **Anthropic API**: ~$0.003 per booking (less than half a cent)
- **Total annual cost** at 200 bookings/year: ~$0.60
