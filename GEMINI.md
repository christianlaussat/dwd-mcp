# DWD Weather (MCP Extension)

This extension integrates weather data from the Deutscher Wetterdienst (DWD) into Gemini CLI via the Model Context Protocol (MCP).

## Features

- **Current Weather**: Access real-time weather observations (temperature, precipitation, wind, clouds) from the nearest DWD station.
- **Forecast**: Retrieve hourly forecasts (temperature, precipitation, wind, clouds) using the DWD MOSMIX system.
- **Historical Weather**: Query historical weather summaries (min/max/avg temperature, total rain) for specific date ranges.

## Tools

### `get_current_weather`
Returns current weather conditions for a specific location.
- **Parameters**:
  - `latitude` (number): Latitude of the location.
  - `longitude` (number): Longitude of the location.

### `get_forecast`
Returns weather forecast (temperature, wind, precipitation, clouds) for a specific location.
- **Parameters**:
  - `latitude` (number): Latitude of the location.
  - `longitude` (number): Longitude of the location.
  - `hours` (integer, default=24): Number of hours to forecast.

### `get_historical_weather`
Returns a summary of historical weather conditions (temperature, precipitation, wind) for a specific location and date range.
- **Parameters**:
  - `latitude` (number): Latitude of the location.
  - `longitude` (number): Longitude of the location.
  - `start_date` (string): Start date in ISO 8601 format (e.g., "2024-12-01").
  - `end_date` (string): End date in ISO 8601 format (e.g., "2024-12-31").

## Setup & Configuration

1.  **Install Dependencies**:
    Ensure you have Python installed, then run:
    ```bash
    pip install -r requirements.txt
    ```

2.  **MCP Configuration**:
    Add the server to your Gemini CLI configuration (e.g., in `~/.gemini/config.json` or by using the `/mcp` command if available).

    **Command**: `/path/to/dwd-mcp/.venv/bin/python3` (or path to your python executable)
    **Args**: `["/path/to/dwd-mcp/server.py"]`

    *Note: Adjust the path to `server.py` to match your installation directory.*

## Usage Examples

- "What is the current temperature at latitude 52.52, longitude 13.40?"
- "Get the 12-hour forecast for 48.13, 11.58."
- "What was the coldest day in December 2024 in Dresden (lat 51.05, lon 13.74)?"
- "How much rain fell in Berlin (lat 52.52, lon 13.40) last month?"
