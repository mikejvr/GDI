"""
live_weather_nws.py
"""

import json
import requests
import time

def get_nws_forecast(lat=30.2672, lon=-97.7431): # Austin coordinates
    print("🌤️ Fetching live weather data for NEXUS pipeline...")
    # Step 1: Get the forecast URL for the exact coordinates
    points_url = f"https://api.weather.gov/points/{lat},{lon}"
    try:
        response = requests.get(points_url, headers={"User-Agent": "NEXUS/1.0 (your_email@example.com)"})
        response.raise_for_status()
        forecast_url = response.json()["properties"]["forecast"]
    except Exception as e:
        return {"error": f"Failed to get forecast URL: {e}"}

    # Step 2: Fetch the actual forecast data
    try:
        forecast_resp = requests.get(forecast_url, headers={"User-Agent": "NEXUS/1.0 (your_email@example.com)"})
        forecast_resp.raise_for_status()
        data = forecast_resp.json()
    except Exception as e:
        return {"error": f"Failed to fetch forecast: {e}"}

    # Step 3: Parse the current forecast period into your required format
    periods = data["properties"]["periods"]
    if not periods:
        return {"error": "No forecast periods found"}

    # Using first period (today/tonight)
    first = periods[0]
    temp_c = (int(first["temperature"]) - 32) * 5.0/9.0 # Convert from F to C for consistency
    weather_data = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "temp_c": round(temp_c, 1),
        "condition_code": first.get("shortForecast", "").lower(),
        "precip_probability": first.get("probabilityOfPrecipitation", {}).get("value", 0),
    }
    return weather_data

if __name__ == "__main__":
    print(json.dumps(get_nws_forecast(), indent=2))
