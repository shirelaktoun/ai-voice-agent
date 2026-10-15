import os
import json
import asyncio
import threading
import websockets
import requests
from flask import Flask, request, Response
from openai import OpenAI, InvalidWebhookSignatureError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Load credentials from environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_WEBHOOK_SECRET = os.getenv("OPENAI_WEBHOOK_SECRET")
OPENAI_PROMPT_ID = os.getenv("OPENAI_PROMPT_ID")

if not OPENAI_API_KEY or not OPENAI_WEBHOOK_SECRET or not OPENAI_PROMPT_ID:
    raise ValueError("Missing required environment variables. Please create a .env file with OPENAI_API_KEY, OPENAI_WEBHOOK_SECRET, and OPENAI_PROMPT_ID.")

# --- Globals ---
app = Flask(__name__)
client = OpenAI(webhook_secret=OPENAI_WEBHOOK_SECRET)
AUTH_HEADER = {"Authorization": f"Bearer {OPENAI_API_KEY}"}

# This is the payload to configure the agent using the user's prompt_id
CALL_ACCEPT_PAYLOAD = {
    "type": "realtime",
    "prompt": {
        "id": OPENAI_PROMPT_ID
    }
}

# Call IDs with a live WebSocket handler, so ended calls can be cleaned up
active_calls = set()
active_calls_lock = threading.Lock()

# --- Call Control ---
def hangup_call(call_id):
    """
    Ends a call by asking OpenAI to hang up the SIP leg.
    Returns True if the hangup request was accepted.
    """
    hangup_url = f"https://api.openai.com/v1/realtime/calls/{call_id}/hangup"
    try:
        response = requests.post(hangup_url, headers=AUTH_HEADER, timeout=10)
        response.raise_for_status()
        print(f"Hung up call {call_id}. Status: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to hang up call {call_id}: {e}")
        return False

# --- WebSocket Task ---
async def websocket_task(call_id):
    """
    Handles the WebSocket connection for a given call.
    Exits cleanly when the call ends (the server closes the socket), and
    hangs up the call if the handler fails unexpectedly so it isn't left open.
    """
    uri = f"wss://api.openai.com/v1/realtime?call_id={call_id}"
    print(f"Connecting to WebSocket at {uri}")

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}

    with active_calls_lock:
        active_calls.add(call_id)

    try:
        async with websockets.connect(uri, additional_headers=headers) as websocket:
            print(f"WebSocket connected for call_id: {call_id}")

            # Listen for messages until the call ends and the socket closes
            async for message in websocket:
                print(f"Received from WebSocket: {message}")

        print(f"Call {call_id} ended (WebSocket closed).")

    except websockets.exceptions.ConnectionClosed as e:
        print(f"Call {call_id} ended (connection closed: {e}).")
    except Exception as e:
        print(f"WebSocket error for call_id {call_id}: {e}")
        # The handler can no longer serve this call, so end it
        await asyncio.to_thread(hangup_call, call_id)
    finally:
        with active_calls_lock:
            active_calls.discard(call_id)

# --- Webhook Handler ---
@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Handles incoming webhooks from OpenAI SIP service.
    """
    try:
        # Verify webhook signature
        event = client.webhooks.unwrap(payload=request.data, sig_header=request.headers.get("openai-signature"))

        if event.type == "realtime.call.incoming":
            call_id = event.data.call_id
            print(f"Received incoming call webhook for call_id: {call_id}")

            # Make the API call to accept the call
            accept_url = f"https://api.openai.com/v1/realtime/calls/{call_id}/accept"
            accept_headers = {**AUTH_HEADER, "Content-Type": "application/json"}

            try:
                response = requests.post(accept_url, headers=accept_headers, json=CALL_ACCEPT_PAYLOAD)
                response.raise_for_status()
                print(f"Successfully sent /accept for call {call_id}. Status: {response.status_code}")

                # Start the WebSocket handler in a background thread
                threading.Thread(
                    target=lambda: asyncio.run(websocket_task(call_id)),
                    daemon=True,
                ).start()

                # Acknowledge the webhook
                return Response(status=200)

            except requests.exceptions.RequestException as e:
                print(f"Failed to accept call {call_id}: {e}")
                return Response("Failed to accept call", status=500)
        else:
            print(f"Received unhandled event type: {event.type}")
            return Response(status=200)

    except InvalidWebhookSignatureError as e:
        print(f"Invalid webhook signature: {e}")
        return Response("Invalid signature", status=400)
    except Exception as e:
        print(f"Error processing webhook: {e}")
        return Response("Webhook processing error", status=500)

# --- Main ---
if __name__ == "__main__":
    print("Starting Flask server for OpenAI SIP Webhook...")
    # Check if SSL certificate and key files exist
    if os.path.exists('/etc/letsencrypt/live/pastime.agency/fullchain.pem') and os.path.exists('/etc/letsencrypt/live/pastime.agency/privkey.pem'):
        print(f"Found SSL certificates. Starting HTTPS server on port 5000.")
        ssl_context = ('/etc/letsencrypt/live/pastime.agency/fullchain.pem', '/etc/letsencrypt/live/pastime.agency/privkey.pem')
        app.run(host='0.0.0.0', port=5000, ssl_context=ssl_context)
    else:
        print("SSL certificates not found. Starting HTTP server on port 5000.")
        print("WARNING: This server is not secure. Do not use in production without HTTPS.")
        app.run(host='0.0.0.0', port=5000)
