# DWD Weather (MCP Extension)

This extension integrates weather data from the Deutscher Wetterdienst (DWD) into Gemini CLI via the Model Context Protocol (MCP).

## Features

- **Current Weather**: Access real-time weather observations (temperature, precipitation types like snow/hail, wind, clouds) from the nearest DWD station.
- **Forecast**: Retrieve hourly forecasts (weather descriptions, temperature, precipitation, snow depth, wind, clouds) using the DWD MOSMIX system.
- **Historical Weather**: Query historical weather summaries (min/max/avg temperature, total rain, precipitation types, max snow depth) for specific date ranges.

## Tools

### `get_current_weather`
Returns current weather conditions (temperature, humidity, pressure, wind, precipitation amount and form (e.g., snow, hail), clouds, solar radiation) for a specific location.
- **Parameters**:
  - `latitude` (number): Latitude of the location.
  - `longitude` (number): Longitude of the location.

### `get_forecast`
Returns weather forecast (weather description, temperature, humidity, pressure, wind, precipitation, snow depth, clouds, solar radiation) for a specific location.
- **Parameters**:
  - `latitude` (number): Latitude of the location.
  - `longitude` (number): Longitude of the location.
  - `hours` (integer, default=24): Number of hours to forecast.

### `get_historical_weather`
Returns a summary of historical weather conditions (temperature, humidity, precipitation amount and observed types, max snow depth, wind, pressure, solar radiation) for a specific location and date range.
- **Parameters**:
  - `latitude` (number): Latitude of the location.
  - `longitude` (number): Longitude of the location.
  - `start_date` (string): Start date in ISO 8601 format (e.g., "2024-12-01").
  - `end_date` (string): End date in ISO 8601 format (e.g., "2024-12-31").

### `get_stations`
Search for DWD weather stations by name or state.
- **Parameters**:
  - `name` (string, optional): Filter stations by name (case-insensitive).
  - `state` (string, optional): Filter stations by state (case-insensitive).

## Resources

### `weather://current/{latitude}/{longitude}`
Provides current weather for a specific location as a resource. This is useful for fetching data directly into a conversation context.

## Prompts

### `summarize_weather`
Creates a prompt for the LLM to provide a concise summary of the current weather and the 24-hour forecast for a specific location.
- **Parameters**:
  - `latitude` (number): Latitude of the location.
  - `longitude` (number): Longitude of the location.

## Setup & Configuration

1.  **Install uv**:
    Ensure you have `uv` installed ([astral.sh/uv](https://astral.sh/uv)).

2.  **MCP Configuration**:
    Add the server to your Gemini CLI configuration.

    **Command**: `uv`
    **Args**: `["--directory", "/path/to/dwd-mcp", "run", "server.py"]`

    *Note: Using `uv run` ensures all dependencies are automatically installed and managed in an isolated environment.*

## Usage Examples

- "What is the current temperature at latitude 52.52, longitude 13.40?"
- "Get the 12-hour forecast for 48.13, 11.58."
- "Find DWD weather stations in Berlin."
- "What was the coldest day in December 2024 in Dresden (lat 51.05, lon 13.74)?"
- "How much rain fell in Berlin (lat 52.52, lon 13.40) last month?"
