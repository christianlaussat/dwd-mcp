# DWD Weather - Gemini MCP Extension

An MCP server that provides weather data from the Deutscher Wetterdienst (DWD), designed to be used as a Gemini CLI extension.

For detailed extension documentation, see [GEMINI.md](GEMINI.md).

## Features

- **Current Weather:** Get the latest weather conditions (temperature, humidity, pressure, precipitation, wind, clouds, solar radiation) for a specific location.
- **Forecast:** Get hourly weather forecast (temperature, humidity, pressure, precipitation, wind, clouds, solar radiation) for the next 24 hours (or specified duration).
- **Historical Weather:** Get a summary of historical weather conditions for a specific location and date range.

## Installation & Usage

### As a Gemini Extension

1.  Ensure you have the Gemini CLI installed.
2.  Navigate to this directory.
3.  Add the extension:
    ```bash
    gemini extension add .
    ```

### Manual Setup (using uv)

1.  Ensure you have [uv](https://astral.sh/uv) installed.
2.  Run the server:
    ```bash
    uv run server.py
    ```

## Testing

Run the test suite using pytest:
```bash
uv run pytest
```

## Tools

- `get_current_weather(latitude, longitude)`: Returns current weather conditions.
- `get_forecast(latitude, longitude, hours=24)`: Returns hourly weather forecast.
- `get_historical_weather(latitude, longitude, start_date, end_date)`: Returns historical weather summary.

## Usage Examples

- "What is the current temperature at latitude 52.52, longitude 13.40?"
- "Get the 12-hour forecast for 48.13, 11.58."
- "What was the coldest day in December 2024 in Dresden (lat 51.05, lon 13.74)?"
- "How much rain fell in Berlin (lat 52.52, lon 13.40) last month?"
