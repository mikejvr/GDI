"""
web_app.py – Paywalled recommendation server with Stripe webhook + email delivery.
"""

import os
import json
import uuid
import hashlib
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template_string
from apscheduler.schedulers.background import BackgroundScheduler
import stripe
import requests

# ----------------------------------------------------------------------
# Import your recommendation engine
# ----------------------------------------------------------------------
import recommend  # must be in the same directory or accessible

# ----------------------------------------------------------------------
# Flask app initialization
# ----------------------------------------------------------------------
app = Flask(__name__)

# ----------------------------------------------------------------------
# Environment variables (set these on Render)
# ----------------------------------------------------------------------
STRIPE_PAYMENT_LINK = os.environ.get("STRIPE_PAYMENT_LINK", "https://buy.stripe.com/your-link")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "changeme")

# Stripe webhook & email
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET")
MAILGUN_API_KEY = os.environ.get("MAILGUN_API_KEY")
MAILGUN_DOMAIN = os.environ.get("MAILGUN_DOMAIN")
FROM_EMAIL = os.environ.get("FROM_EMAIL", f"welcome@{MAILGUN_DOMAIN}" if MAILGUN_DOMAIN else "welcome@example.com")

stripe.api_key = STRIPE_SECRET_KEY

# ----------------------------------------------------------------------
# Persistent token storage (file‑based, survives restarts)
# ----------------------------------------------------------------------
TOKEN_FILE = Path("tokens.json")

def load_tokens():
    if TOKEN_FILE.exists():
        with open(TOKEN_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_tokens(tokens):
    with open(TOKEN_FILE, "w") as f:
        json.dump(list(tokens), f)

ACTIVE_TOKENS = load_tokens()

def add_token(token):
    ACTIVE_TOKENS.add(token)
    save_tokens(ACTIVE_TOKENS)

def remove_token(token):
    ACTIVE_TOKENS.discard(token)
    save_tokens(ACTIVE_TOKENS)

# ----------------------------------------------------------------------
# Git pull helper (keeps data/ updated from GitHub)
# ----------------------------------------------------------------------
def git_pull():
    """Pull the latest commits from the main branch using a PAT."""
    print("🔄 Running git pull...")
    repo_path = Path(__file__).parent
    token = os.environ.get("GIT_PAT")
    if not token:
        print("⚠️ GIT_PAT not set – cannot pull.")
        return

    try:
        remote_url = subprocess.run(
            ["git", "config", "--get", "remote.origin.url"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True
        ).stdout.strip()
        if remote_url.startswith("https://"):
            new_url = remote_url.replace("https://", f"https://{token}@")
        else:
            print("⚠️ Non-https remote – skipping pull.")
            return
        subprocess.run(["git", "remote", "set-url", "origin", new_url], cwd=repo_path, check=True)
        subprocess.run(["git", "pull", "--ff-only"], cwd=repo_path, check=True)
        print("✅ Git pull successful.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git pull failed: {e}")
    finally:
        if remote_url:
            subprocess.run(["git", "remote", "set-url", "origin", remote_url], cwd=repo_path, check=False)

# ----------------------------------------------------------------------
# Scheduler: run git pull every hour
# ----------------------------------------------------------------------
scheduler = BackgroundScheduler()
scheduler.add_job(git_pull, 'interval', hours=1)
scheduler.start()

# ----------------------------------------------------------------------
# Email sending function (Mailgun)
# ----------------------------------------------------------------------
def send_welcome_email(to_email, customer_name, token):
    """Send an email with the access token using Mailgun."""
    app_url = os.environ.get("RENDER_EXTERNAL_URL", "https://your-app.onrender.com")
    magic_link = f"{app_url}/?token={token}"
    
    html_content = f"
    '''<html>
      <body>
        <h2>Welcome, {customer_name}!</h2>
        <p>Your subscription to <strong>Gig Driver Intelligence</strong> is now active.</p>
        <p><b>Your Access Token:</b> <code>{token}</code></p>
        <p>👉 <a href="{magic_link}">Click here to get your first recommendation</a></p>
        <p>Or paste the token on our website.</p>
        <p>This token is valid for 30 days (your subscription period).</p>
        <p>Thank you for subscribing!</p>
        <p>– Gig Driver Intelligence Team</p>
      </body>
    </html>'''
    "
    
    if not MAILGUN_API_KEY or not MAILGUN_DOMAIN:
        print("⚠️ Mailgun credentials missing – email not sent.")
        return False
    
    resp = requests.post(
        f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
        auth=("api", MAILGUN_API_KEY),
        data={
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": "Your Gig Driver Intelligence access token",
            "html": html_content
        }
    )
    return resp.status_code == 200

# ----------------------------------------------------------------------
# HTML landing page
# ----------------------------------------------------------------------
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>Gig Driver Earnings Maximizer</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: Arial, sans-serif; margin: 2em; max-width: 800px; margin: auto; }
        button { background: #635bff; color: white; border: none; padding: 12px 24px; font-size: 18px; border-radius: 8px; cursor: pointer; }
        #rec { background: #f5f5f5; padding: 1em; border-radius: 8px; margin-top: 2em; white-space: pre-wrap; }
        .error { color: red; }
    </style>
</head>
<body>
    <h1>🚗 Gig Driver Intelligence</h1>
    <p>Real‑time recommendation: when to drive, which apps to stack, expected hourly earnings.</p>
    <p><strong>$9.99/month</strong> – cancel anytime.</p>

    {% if not token %}
    <p><a href="{{ stripe_link }}" target="_blank"><button>Subscribe Now</button></a></p>
    <p>After subscribing, enter your token:</p>
    <form method="GET">
        <input type="text" name="token" placeholder="Your token" style="padding: 8px; width: 300px;">
        <button type="submit">Get Recommendation</button>
    </form>
    {% else %}
    <div id="rec">
        <strong>Your recommendation:</strong>
        <pre id="rec-content">Loading...</pre>
    </div>
    <button onclick="fetchRecommendation()">Refresh</button>
    <script>
        async function fetchRecommendation() {
            const token = '{{ token }}';
            const response = await fetch('/api/recommend', {
                headers: { 'Authorization': 'Bearer ' + token }
            });
            const data = await response.json();
            document.getElementById('rec-content').textContent = JSON.stringify(data, null, 2);
        }
        fetchRecommendation();
    </script>
    {% endif %}
</body>
</html>
"""

# ----------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------
@app.route('/')
def index():
    token = request.args.get('token')
    if token and token in ACTIVE_TOKENS:
        return render_template_string(HTML_PAGE, token=token, stripe_link=STRIPE_PAYMENT_LINK)
    else:
        return render_template_string(HTML_PAGE, token=None, stripe_link=STRIPE_PAYMENT_LINK)

@app.route('/api/recommend')
def api_recommend():
    auth = request.headers.get('Authorization')
    if not auth or not auth.startswith('Bearer '):
        return jsonify({"error": "Missing token"}), 401
    token = auth.split(' ')[1]
    if token not in ACTIVE_TOKENS:
        return jsonify({"error": "Invalid or expired token"}), 403

    shard = recommend.load_latest_shard()
    if not shard:
        return jsonify({"error": "No demand data available"}), 503
    rec = recommend.compute_recommendation(shard)
    return jsonify(rec)

@app.route('/admin/add_token', methods=['POST'])
def add_token_endpoint():
    secret = request.headers.get('X-Admin-Secret')
    if secret != ADMIN_SECRET:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()
    token = data.get('token')
    if token:
        add_token(token)
        return jsonify({"status": "added", "token": token})
    return jsonify({"error": "No token provided"}), 400

# ----------------------------------------------------------------------
# Stripe webhook
# ----------------------------------------------------------------------
@app.route('/stripe-webhook', methods=['POST'])
def stripe_webhook():
    if not STRIPE_WEBHOOK_SECRET:
        return jsonify({"error": "Webhook secret not configured"}), 500

    payload = request.get_data(as_text=True)
    sig_header = request.headers.get('Stripe-Signature')
    
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except ValueError:
        return jsonify({"error": "Invalid payload"}), 400
    except stripe.error.SignatureVerificationError:
        return jsonify({"error": "Invalid signature"}), 400

    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        customer_email = session['customer_details']['email']
        customer_name = session['customer_details'].get('name', 'Driver')
        
        # Generate a unique token from subscription ID or session ID
        sub_id = session.get('subscription')
        if sub_id:
            token_base = f"{sub_id}:{customer_email}"
        else:
            token_base = f"{session['id']}:{customer_email}"
        token = hashlib.sha256(token_base.encode()).hexdigest()[:16]
        
        add_token(token)
        
        # Send email using Mailgun (if configured)
        if MAILGUN_API_KEY and MAILGUN_DOMAIN:
            success = send_welcome_email(customer_email, customer_name, token)
            if not success:
                print(f"⚠️ Failed to send email to {customer_email}")
        else:
            print(f"ℹ️ No Mailgun config – token {token} for {customer_email} saved but not emailed.")
    
    return jsonify({"status": "success"}), 200

# ----------------------------------------------------------------------
# Startup
# ----------------------------------------------------------------------
if __name__ == '__main__':
    # Pull latest data once at startup
    git_pull()
    app.run(host='0.0.0.0', port=5000, debug=False)
