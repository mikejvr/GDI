#!/usr/bin/env python3
"""
web_app.py – Flask server for paywalled recommendation API.
Uses Stripe Payment Links for subscription.
"""

import os
import json
from flask import Flask, request, jsonify, render_template_string
from recommend import load_latest_shard, compute_recommendation

app = Flask(__name__)

# Stripe keys – set these as environment variables
STRIPE_PUBLIC_KEY = os.environ.get("STRIPE_PUBLIC_KEY", "pk_test_...")
STRIPE_PRICE_ID = os.environ.get("STRIPE_PRICE_ID", "price_...")  # $9.99/month product

# For MVP, we'll use a simple in-memory token store.
# In production, use a database (sqlite, postgres) to store active subscriptions.
active_tokens = set()  # tokens granted after payment
active_tokens.add("test_token_123")

# ----------------------------------------------------------------------
# Simple payment flow: Stripe Checkout (hosted by Stripe)
# You create a Payment Link on Stripe Dashboard, then redirect users there.
# After payment, Stripe sends a webhook to /webhook to grant access.
# ----------------------------------------------------------------------

HTML_LANDING = """
<!DOCTYPE html>
<html>
<head>
    <title>Gig Driver Intelligence</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: Arial, sans-serif; margin: 2em; line-height: 1.5; }
        .container { max-width: 600px; margin: auto; }
        button { background: #635bff; color: white; border: none; padding: 12px 24px; font-size: 18px; cursor: pointer; border-radius: 5px; }
        button:hover { background: #4a43c9; }
        #recommendation { margin-top: 2em; padding: 1em; border: 1px solid #ccc; background: #f9f9f9; display: none; }
    </style>
</head>
<body>
<div class="container">
    <h1>🚗 Gig Driver Earnings Maximizer</h1>
    <p>Get real‑time recommendations on when, where, and which apps to stack – based on surge, events, and weather.</p>
    <p><strong>$9.99/month</strong> – cancel anytime.</p>
    <button id="subscribeBtn">Subscribe Now</button>
    
    <div id="recommendation">
        <h2>Your Recommendation</h2>
        <pre id="recOutput"></pre>
    </div>

    <script>
        const token = localStorage.getItem('access_token');
        if (token) {
            fetchRecommendation(token);
        }
        document.getElementById('subscribeBtn').onclick = function() {
            // Redirect to Stripe Payment Link (create one in dashboard)
            window.location.href = "{{ stripe_payment_link }}";
        };
        async function fetchRecommendation(token) {
            const response = await fetch('/api/recommend', {
                headers: { 'Authorization': 'Bearer ' + token }
            });
            const data = await response.json();
            if (response.ok) {
                document.getElementById('recOutput').textContent = JSON.stringify(data, null, 2);
                document.getElementById('recommendation').style.display = 'block';
            } else {
                alert('Subscription required. Please subscribe.');
                localStorage.removeItem('access_token');
            }
        }
        // Check if URL contains ?token=... (after webhook redirect)
        const urlParams = new URLSearchParams(window.location.search);
        const urlToken = urlParams.get('token');
        if (urlToken) {
            localStorage.setItem('access_token', urlToken);
            window.history.replaceState({}, '', '/');
            fetchRecommendation(urlToken);
        }
    </script>
</div>
</body>
</html>
"""

@app.route('/')
def landing():
    """Show landing page with subscribe button."""
    # You must create a Stripe Payment Link manually and set it here or via env var
    stripe_payment_link = os.environ.get("STRIPE_PAYMENT_LINK", "https://buy.stripe.com/your-link")
    return render_template_string(HTML_LANDING, stripe_payment_link=stripe_payment_link)

@app.route('/api/recommend')
def api_recommend():
    """Protected endpoint: returns recommendation JSON if token valid."""
    auth = request.headers.get('Authorization')
    if not auth or not auth.startswith('Bearer '):
        return jsonify({"error": "Missing or invalid token"}), 401
    token = auth.split(' ')[1]
    if token not in active_tokens:
        return jsonify({"error": "Subscription required"}), 402
    shard = load_latest_shard()
    if not shard:
        return jsonify({"error": "No demand data available"}), 503
    if shard.get("shard_type") != "gig_demand_signal":
        return jsonify({"error": "Invalid data"}), 500
    rec = compute_recommendation(shard)
    return jsonify(rec)

@app.route('/webhook', methods=['POST'])
def stripe_webhook():
    """Stripe webhook to grant access after successful payment."""
    payload = request.get_data(as_text=True)
    sig_header = request.headers.get('Stripe-Signature')
    # Verify webhook signature (optional for MVP, but recommended)
    # webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET")
    # event = stripe.Webhook.construct_event(payload, sig_header, webhook_secret)
    event = json.loads(payload)  # For testing without verification
    if event.get('type') == 'checkout.session.completed':
        session = event['data']['object']
        customer_email = session.get('customer_details', {}).get('email')
        # Generate a unique token for this customer
        import uuid
        token = str(uuid.uuid4())
        active_tokens.add(token)
        # In production, store token in DB with expiry (e.g., 30 days)
        # For MVP, we'll include token in redirect URL
        return jsonify({"token": token}), 200
    return jsonify({"status": "ignored"}), 200

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
