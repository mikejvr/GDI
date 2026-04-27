"""
live_events_tickemaster.py
"""

import json
import requests
import time
from datetime import datetime

def get_ticketmaster_events(lat=30.2672, lon=-97.7431, radius_km=15, size=50, apikey="ELt3l1J64uu3Pu1eZMAkMoZLilam8F0x"):
    print("🎤 Fetching live events from Ticketmaster API...")
    url = "https://app.ticketmaster.com/discovery/v2/events.json"
    params = {
        "apikey": apikey,
        "latlong": f"{lat},{lon}",
        "radius": radius_km,
        "unit": "km",
        "size": size,
        "sort": "date,asc"
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        return {"error": f"Ticketmaster API request failed: {e}"}
    
    events = data.get("_embedded", {}).get("events", [])
    if not events:
        return {"error": "No events found in area"}
    
    parsed_events = []
    for ev in events:
        # Safely extract venue information
        venue = ev.get("_embedded", {}).get("venues", [{}])[0] if "_embedded" in ev else {}
        venue_location = venue.get("location", {}) if venue else {}
        
        # Safely extract dates
        dates = ev.get("dates", {})
        start_date = dates.get("start", {})
        start_time = start_date.get("dateTime") if start_date.get("dateTime") else start_date.get("localDate")
        
        # Build the event data object
        event_data = {
            "event_id": ev.get("id"),
            "name": ev.get("name"),
            "category": ev.get("classifications", [{}])[0].get("segment", {}).get("name", "").lower() if ev.get("classifications") else "",
            "start_time": start_time,
            "venue": {
                "name": venue.get("name") if venue else "",
                "lat": venue_location.get("lat") if venue_location else None,
                "lon": venue_location.get("lon") if venue_location else None,
            },
            "expected_attendance": 0,  # Ticketmaster doesn't provide attendance directly
            "is_sold_out": start_date.get("status", {}).get("code") == "soldOut" if start_date else False,
        }
        # Validate required fields
        if all([event_data["event_id"], event_data["name"], event_data["start_time"]]):
            parsed_events.append(event_data)
        else:
            print(f"⚠️ Skipping incomplete event: {event_data['name']}")
    
    print(f"📊 Processed {len(parsed_events)} valid events out of {len(events)} total")
    return {"events": parsed_events, "count": len(parsed_events)}

if __name__ == "__main__":
    result = get_ticketmaster_events(apikey="YOUR_API_KEY")
    print(json.dumps(result, indent=2))
