# DWD Weather - Gemini MCP Extension

An MCP server that provides weather data from the Deutscher Wetterdienst (DWD), designed to be used as a Gemini CLI extension.

For detailed extension documentation, see [GEMINI.md](GEMINI.md).

## Features

- **Current Weather:** Get the latest weather conditions (temperature, humidity, pressure, precipitation, wind, clouds, solar radiation) for a specific location.
- **Forecast:** Get hourly weather forecast (temperature, humidity, pressure, precipitation, wind, clouds, solar radiation) for the next 24 hours (or specified duration).

## Installation & Usage

### As a Gemini Extension

1.  Ensure you have the Gemini CLI installed.
2.  Navigate to this directory.
3.  Add the extension:
    ```bash
    gemini extension add .
    ```
    (Or point to the directory path)

### Manual Setup

1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
2.  Run the server:
    ```bash
    python server.py
    ```

## Testing

Run the test suite using pytest:
```bash
PYTHONPATH=. .venv/bin/python3 -m pytest tests/
```

## Tools

- `get_current_weather(latitude, longitude)`: Returns current weather conditions.
- `get_forecast(latitude, longitude, hours=24)`: Returns hourly weather forecast.
