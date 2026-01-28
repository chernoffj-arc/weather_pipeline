"""
Weather utility functions for fetching and processing weather data.
This module contains the core logic extracted from weather_fetcher.ipynb
to enable unit testing.
"""

import requests
from datetime import datetime
from typing import Optional, Dict, Any

# Constants
CALGARY_LAT = 51.0447
CALGARY_LON = -114.0719
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather_data(
    latitude: float = CALGARY_LAT,
    longitude: float = CALGARY_LON,
    city: str = "Calgary",
    timezone: str = "America/Edmonton",
    api_url: str = WEATHER_API_URL,
    timeout: int = 10
) -> Optional[Dict[str, Any]]:
    """
    Fetch current weather data from Open-Meteo API.
    
    Args:
        latitude: Latitude coordinate
        longitude: Longitude coordinate  
        city: City name for the record
        timezone: Timezone for the weather data
        api_url: The API endpoint URL
        timeout: Request timeout in seconds
        
    Returns:
        Dictionary with weather information or None if fetch fails
    """
    try:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,weather_code",
            "timezone": timezone
        }
        
        response = requests.get(api_url, params=params, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        current = data.get("current", {})
        
        return {
            "timestamp": datetime.fromisoformat(current.get("time", datetime.now().isoformat())),
            "latitude": latitude,
            "longitude": longitude,
            "temperature": current.get("temperature_2m"),
            "humidity": current.get("relative_humidity_2m"),
            "wind_speed": current.get("wind_speed_10m"),
            "wind_direction": current.get("wind_direction_10m"),
            "weather_code": current.get("weather_code"),
            "city": city,
            "fetch_time": datetime.now()
        }
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return None


def parse_weather_response(api_response: Dict[str, Any], city: str = "Calgary") -> Optional[Dict[str, Any]]:
    """
    Parse the raw API response into a structured weather record.
    
    Args:
        api_response: Raw JSON response from the Open-Meteo API
        city: City name to include in the record
        
    Returns:
        Parsed weather data dictionary or None if parsing fails
    """
    try:
        current = api_response.get("current", {})
        
        if not current:
            return None
            
        return {
            "timestamp": datetime.fromisoformat(current.get("time", datetime.now().isoformat())),
            "latitude": api_response.get("latitude"),
            "longitude": api_response.get("longitude"),
            "temperature": current.get("temperature_2m"),
            "humidity": current.get("relative_humidity_2m"),
            "wind_speed": current.get("wind_speed_10m"),
            "wind_direction": current.get("wind_direction_10m"),
            "weather_code": current.get("weather_code"),
            "city": city,
            "fetch_time": datetime.now()
        }
    except Exception as e:
        print(f"Error parsing weather response: {e}")
        return None


def validate_weather_record(record: Dict[str, Any]) -> bool:
    """
    Validate that a weather record contains all required fields with valid values.
    
    Args:
        record: Weather data dictionary to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = ["timestamp", "latitude", "longitude", "temperature", "city"]
    
    if record is None:
        return False
        
    for field in required_fields:
        if field not in record or record[field] is None:
            return False
    
    # Validate coordinate ranges
    if not (-90 <= record["latitude"] <= 90):
        return False
    if not (-180 <= record["longitude"] <= 180):
        return False
        
    # Validate temperature is reasonable (in Celsius)
    if not (-100 <= record["temperature"] <= 60):
        return False
        
    return True


def get_weather_description(weather_code: int) -> str:
    """
    Convert WMO weather code to human-readable description.
    
    Args:
        weather_code: WMO weather interpretation code
        
    Returns:
        Human-readable weather description
    """
    weather_codes = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Depositing rime fog",
        51: "Light drizzle",
        53: "Moderate drizzle",
        55: "Dense drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        71: "Slight snow",
        73: "Moderate snow",
        75: "Heavy snow",
        77: "Snow grains",
        80: "Slight rain showers",
        81: "Moderate rain showers",
        82: "Violent rain showers",
        85: "Slight snow showers",
        86: "Heavy snow showers",
        95: "Thunderstorm",
        96: "Thunderstorm with slight hail",
        99: "Thunderstorm with heavy hail",
    }
    return weather_codes.get(weather_code, "Unknown")
