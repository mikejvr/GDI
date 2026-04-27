import os
import json
import uuid
import subprocess
from pathlib import Path
from flask import Flask, request, jsonify, render_template_string
from apscheduler.schedulers.background import BackgroundScheduler
import recommend  # your existing recommendation module

app = Flask(__name__)

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

    # Rewrite remote URL to include the token for authentication
    try:
        # Get current remote URL
        remote_url = subprocess.run(
            ["git", "config", "--get", "remote.origin.url"],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True
        ).stdout.strip()
        # Convert https://github.com/user/repo -> https://TOKEN@github.com/user/repo
        if remote_url.startswith("https://"):
            new_url = remote_url.replace("https://", f"https://{token}@")
        else:
            # For ssh, you'd need a different approach – but most use https
            print("⚠️ Non-https remote – skipping pull.")
            return
        # Set the temporary URL for this pull
        subprocess.run(["git", "remote", "set-url", "origin", new_url], cwd=repo_path, check=True)
        # Pull
        subprocess.run(["git", "pull", "--ff-only"], cwd=repo_path, check=True)
        print("✅ Git pull successful.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git pull failed: {e}")
    finally:
        # Restore original URL (without token)
        if remote_url:
            subprocess.run(["git", "remote", "set-url", "origin", remote_url], cwd=repo_path, check=False)

# ----------------------------------------------------------------------
# Scheduler: run git pull every hour
# ----------------------------------------------------------------------
scheduler = BackgroundScheduler()
scheduler.add_job(git_pull, 'interval', hours=1)
scheduler.start()

# ----------------------------------------------------------------------
# In-memory token store (for MVP – use a database later)
# ----------------------------------------------------------------------
ACTIVE_TOKENS = set()
STRIPE_PAYMENT_LINK = os.environ.get("STRIPE_PAYMENT_LINK", "https://buy.stripe.com/your-link-here")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "changeme")

# ----------------------------------------------------------------------
# HTML landing page (same as before, but with token auto‑store)
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
    </style>
</head>
<body>
    <h1>🚗 Gig Driver Intelligence</h1>
    <p>Real‑time recommendation: when to drive, which apps to stack, expected hourly earnings.</p>
    <p><strong>$9.99/month</strong> – cancel anytime.</p>

    {% if not token %}
    <p><a href="{{ stripe_link }}" target="_blank"><button>Subscribe Now</button></a></p>
    <p>After subscribing, you'll receive a token. Paste it below:</p>
    <form method="GET">
        <input type="text" name="token" placeholder="Your token" style="padding: 8px; width: 300px;">
        <button type="submit">Get Recommendation</button>
    </form>
    {% else %}
    <div id="rec"><strong>Your recommendation:</strong><br><pre id="rec-content">Loading...</pre></div>
    <button onclick="fetchRec()">Refresh</button>
    <script>
        async function fetchRec() {
            const response = await fetch('/api/recommend', {
                headers: { 'Authorization': 'Bearer {{ token }}' }
            });
            const data = await response.json();
            document.getElementById('rec-content').textContent = JSON.stringify(data, null, 2);
        }
        fetchRec();
    </script>
    {% endif %}
</body>
</html>
"""

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

    # Ensure we have the latest shard (run git pull if needed? Already done every hour)
    shard = recommend.load_latest_shard()
    if not shard:
        return jsonify({"error": "No demand data available"}), 503
    rec = recommend.compute_recommendation(shard)
    return jsonify(rec)

@app.route('/admin/add_token', methods=['POST'])
def add_token():
    secret = request.headers.get('X-Admin-Secret')
    if secret != ADMIN_SECRET:
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json()
    token = data.get('token')
    if token:
        ACTIVE_TOKENS.add(token)
        return jsonify({"status": "added", "token": token})
    return jsonify({"error": "No token provided"}), 400

if __name__ == '__main__':
    # Run a git pull at startup to get the latest data
    git_pull()
    app.run(host='0.0.0.0', port=5000, debug=False)
