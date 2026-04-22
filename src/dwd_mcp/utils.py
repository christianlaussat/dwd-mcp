"""Utility functions for weather data labels and mappings."""

def get_wind_direction_label(degrees: float) -> str:
    """Convert degrees to wind direction labels (N, NNE, etc.)."""
    if degrees is None:
        return "Unknown"
    val = int((degrees / 22.5) + 0.5)
    arr = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return arr[(val % 16)]

def get_precipitation_form_label(code: float) -> str:
    """Map DWD WR codes to human-readable precipitation forms."""
    if code is None:
        return "Unknown"
    # DWD WR codes:
    mapping = {
        0.0: "None",
        1.0: "Rain",
        2.0: "Unknown",
        3.0: "Snow",
        4.0: "Rain and Snow",
        5.0: "Unknown",
        6.0: "Mixed Rain and Snow",
        7.0: "Sleet",
        8.0: "Hail",
        9.0: "None (recently ended)",
    }
    return mapping.get(code, f"Code {code}")

def get_significant_weather_label(code: float) -> str:
    """Map DWD WW codes (MOSMIX) to human-readable weather descriptions."""
    if code is None:
        return "Unknown"
    # DWD WW codes (MOSMIX)
    mapping = {
        0: "Clear",
        1: "Partly Cloudy",
        2: "Cloudy",
        3: "Overcast",
        45: "Fog",
        49: "Fog with Rime",
        51: "Light Drizzle",
        53: "Moderate Drizzle",
        55: "Heavy Drizzle",
        61: "Light Rain",
        63: "Moderate Rain",
        65: "Heavy Rain",
        68: "Light Sleet",
        69: "Heavy Sleet",
        71: "Light Snow",
        73: "Moderate Snow",
        75: "Heavy Snow",
        80: "Light Rain Showers",
        81: "Moderate Rain Showers",
        82: "Violent Rain Showers",
        83: "Light Sleet Showers",
        84: "Heavy Sleet Showers",
        85: "Light Snow Showers",
        86: "Heavy Snow Showers",
        87: "Light Graupel/Hail Showers",
        88: "Heavy Graupel/Hail Showers",
        89: "Light Hail Showers",
        90: "Heavy Hail Showers",
        95: "Light/Moderate Thunderstorm",
        96: "Thunderstorm with Hail",
        99: "Heavy Thunderstorm",
    }
    return mapping.get(int(code), f"Weather Code {code}")
