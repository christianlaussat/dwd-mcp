#!/usr/bin/env python3
import asyncio
import logging
from typing import Any, Dict, List
import datetime

from mcp.server import Server
from mcp.server.stdio import stdio_server
import mcp.types as types

from wetterdienst.provider.dwd.observation import DwdObservationRequest
from wetterdienst.provider.dwd.mosmix import DwdMosmixRequest
# wetterdienst uses polars internally
import polars as pl

# Initialize the server
server = Server("dwd-mcp")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dwd-mcp")

@server.list_tools()
async def list_tools() -> List[types.Tool]:
    return [
        types.Tool(
            name="get_current_weather",
            description="Get current weather (temperature, clouds, precipitation, wind) for a location (latitude, longitude) from DWD observation stations.",
            inputSchema={
                "type": "object",
                "properties": {
                    "latitude": {"type": "number", "description": "Latitude of the location"},
                    "longitude": {"type": "number", "description": "Longitude of the location"},
                },
                "required": ["latitude", "longitude"],
            },
        ),
        types.Tool(
            name="get_forecast",
            description="Get weather forecast (temperature, wind, precipitation, clouds) for a location (latitude, longitude) using DWD MOSMIX.",
            inputSchema={
                "type": "object",
                "properties": {
                    "latitude": {"type": "number", "description": "Latitude of the location"},
                    "longitude": {"type": "number", "description": "Longitude of the location"},
                    "hours": {"type": "integer", "description": "Number of hours to forecast (default 24)", "default": 24},
                },
                "required": ["latitude", "longitude"],
            },
        ),
        types.Tool(
            name="get_historical_weather",
            description="Get historical weather data (temperature, precipitation, wind) for a location (latitude, longitude) for a specific date range.",
            inputSchema={
                "type": "object",
                "properties": {
                    "latitude": {"type": "number", "description": "Latitude of the location"},
                    "longitude": {"type": "number", "description": "Longitude of the location"},
                    "start_date": {"type": "string", "description": "Start date (ISO 8601 format, e.g., 2024-12-01)"},
                    "end_date": {"type": "string", "description": "End date (ISO 8601 format, e.g., 2024-12-31)"},
                },
                "required": ["latitude", "longitude", "start_date", "end_date"],
            },
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: Any) -> List[types.TextContent]:
    if name == "get_current_weather":
        return await get_current_weather(arguments)
    elif name == "get_forecast":
        return await get_forecast(arguments)
    elif name == "get_historical_weather":
        return await get_historical_weather(arguments)
    
    raise ValueError(f"Tool {name} not found")

def get_wind_direction_label(degrees: float) -> str:
    if degrees is None:
        return "Unknown"
    val = int((degrees / 22.5) + 0.5)
    arr = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return arr[(val % 16)]

async def get_current_weather(arguments: Any) -> List[types.TextContent]:
    latitude = arguments.get("latitude")
    longitude = arguments.get("longitude")

    if latitude is None or longitude is None:
        raise ValueError("Latitude and longitude are required")

    try:
        # Define parameters to fetch
        parameters = [
            ("hourly", "temperature_air"),
            ("hourly", "precipitation"),
            ("hourly", "wind"),
            ("hourly", "cloud_type"),
        ]
        
        request = DwdObservationRequest(
            parameters=parameters,
            periods="recent"
        )
        
        # Find the nearest station (rank=1)
        # Note: This returns closest station for EACH dataset (parameter set).
        # We need to capture which station is used for which parameter.
        stations_all = request.filter_by_rank(latlon=(latitude, longitude), rank=1)
        
        if stations_all.df.is_empty():
            return [types.TextContent(type="text", text="No weather station found nearby.")]
        
        # Extract unique closest stations for each dataset
        # stations_all.df columns include: dataset, station_id, distance, name, ...
        # We sort by distance to ensure we get the absolute closest for each dataset
        best_stations_df = stations_all.df.sort("distance").unique(subset=["dataset"], keep="first")
        
        # Collect station IDs to fetch data for
        target_station_ids = best_stations_df["station_id"].unique().to_list()
        
        # Map dataset to station info for display
        dataset_station_map = {}
        for row in best_stations_df.iter_rows(named=True):
            dataset_station_map[row["dataset"]] = {
                "name": row["name"],
                "id": row["station_id"],
                "distance": row["distance"]
            }

        # Filter request for these stations
        stations_filtered = request.filter_by_station_id(station_id=target_station_ids)
        
        # Fetch values
        values = stations_filtered.values.all()
        df = values.df
        
        # Drop nulls
        df = df.drop_nulls(subset=["value"])
        
        if df.is_empty():
            return [types.TextContent(type="text", text="No recent data available for the nearest stations.")]

        # Get latest value for each parameter per station
        # We sort by date descending and then unique by parameter+station
        latest_df = df.sort("date", descending=True).unique(subset=["station_id", "parameter"], keep="first")
        
        # Build the response
        # We'll try to aggregate by station or by parameter?
        # By parameter is probably better for the user ("Temp: X, Wind: Y").
        # But we should mention where it comes from.
        
        # Parameter mapping
        param_map = {
            "temperature_air_mean_2m": {"label": "Temperature", "unit": "°C", "dataset": "temperature_air"},
            "humidity": {"label": "Humidity", "unit": "%", "dataset": "temperature_air"},
            "precipitation_height": {"label": "Precipitation", "unit": "mm", "dataset": "precipitation"},
            "wind_speed": {"label": "Wind Speed", "unit": "m/s", "dataset": "wind"},
            "wind_direction": {"label": "Wind Direction", "unit": "°", "dataset": "wind"},
            "cloud_cover_total": {"label": "Cloud Cover", "unit": "%", "dataset": "cloud_type"},
        }
        
        # Order of display
        display_order = ["temperature_air_mean_2m", "humidity", "precipitation_height", "wind_speed", "wind_direction", "cloud_cover_total"]
        
        results = {}
        latest_time = None
        
        for row in latest_df.iter_rows(named=True):
            param = row["parameter"]
            if param in param_map:
                val = row["value"]
                station_id = row["station_id"]
                date = row["date"]
                
                # Update latest time if newer
                if latest_time is None or date > latest_time:
                    latest_time = date
                
                # Determine if this station is the "best" one for this parameter
                expected_dataset = param_map[param]["dataset"]
                best_info = dataset_station_map.get(expected_dataset)
                
                is_best = False
                if best_info and best_info["id"] == station_id:
                    is_best = True
                
                # Logic:
                # If we don't have this param yet, take it.
                # If we have it, but the current one is "is_best", overwrite.
                
                if param not in results or is_best:
                    results[param] = {
                        "value": val,
                        "station_id": station_id,
                        "date": date
                    }

        response_lines = []
        if latest_time:
            response_lines.append(f"Current Weather (latest reading: {latest_time}):")
        else:
            response_lines.append("Current Weather:")
            
        for param in display_order:
            if param in results:
                data = results[param]
                info = param_map[param]
                
                val = data["value"]
                unit = info["unit"]
                label = info["label"]
                station_id = data["station_id"]
                
                # Find station name from our map
                # We need to look up which dataset this parameter belongs to, then find the station name for that dataset
                # OR, strictly speaking, look up station name by ID from stations_all.df
                # Simple lookup:
                station_name = "Unknown Station"
                # Scan best_stations_df or filtered stations to find name for this ID
                # We can do it efficiently
                st_row = best_stations_df.filter(pl.col("station_id") == station_id).head(1)
                if not st_row.is_empty():
                    station_name = st_row.row(0, named=True)["name"]
                    dist = st_row.row(0, named=True)["distance"]
                    station_str = f"{station_name} ({dist:.1f} km)"
                else:
                    station_str = f"ID: {station_id}"

                formatted_val = f"{val} {unit}"
                
                if param == "wind_direction":
                    direction_label = get_wind_direction_label(val)
                    formatted_val = f"{val}° ({direction_label})"
                
                response_lines.append(f"- {label}: {formatted_val} [from {station_str}]")
        
        if not results:
             return [types.TextContent(type="text", text="No relevant weather parameters found in recent data.")]

        return [types.TextContent(type="text", text="\n".join(response_lines))]

    except Exception as e:
        logger.error(f"Error fetching weather: {e}")
        return [types.TextContent(type="text", text=f"Error fetching weather data: {str(e)}")]

async def get_forecast(arguments: Any) -> List[types.TextContent]:
    latitude = arguments.get("latitude")
    longitude = arguments.get("longitude")
    hours = arguments.get("hours", 24)

    if latitude is None or longitude is None:
        raise ValueError("Latitude and longitude are required")

    try:
        # Create a request for forecast with multiple parameters
        request = DwdMosmixRequest(
            parameters=[
                ("hourly", "small", "temperature_air_mean_2m"),
                ("hourly", "small", "precipitation_height_significant_weather_last_1h"),
                ("hourly", "small", "wind_speed"),
                ("hourly", "small", "wind_direction"),
                ("hourly", "small", "cloud_cover_total"),
                ("hourly", "small", "water_equivalent_snow_depth_new_last_1h"),
            ]
        )
        
        # Find the nearest station
        stations = request.filter_by_rank(latlon=(latitude, longitude), rank=1)
        
        if stations.df.is_empty():
            return [types.TextContent(type="text", text="No weather station found nearby.")]
            
        # Get station info
        station_info = stations.df.row(0, named=True)
        station_name = station_info.get("name")
        station_id = station_info.get("station_id")

        # Get the values
        values = stations.values.all()
        df = values.df
        
        # Drop nulls
        df = df.drop_nulls(subset=["value"])
        
        if df.is_empty():
             return [types.TextContent(type="text", text="No forecast data available.")]

        # Collect data by date
        forecast_data = {}
        for row in df.iter_rows(named=True):
            d = row["date"]
            p = row["parameter"]
            v = row["value"]
            
            if d not in forecast_data:
                forecast_data[d] = {}
            forecast_data[d][p] = v
            
        # Sort dates
        sorted_dates = sorted(forecast_data.keys())
        
        # Filter for future dates (approximate)
        now = datetime.datetime.now(datetime.timezone.utc)
        # Allow some margin (e.g. 1 hour back) to show current hour's forecast
        future_dates = [d for d in sorted_dates if d >= now - datetime.timedelta(hours=1)]
        
        if future_dates:
            sorted_dates = future_dates

        # Take up to 'hours' rows
        sorted_dates = sorted_dates[:hours]
        
        if not sorted_dates:
             return [types.TextContent(type="text", text="No future forecast data available.")]

        response_lines = [f"Forecast for {station_name} (ID: {station_id}):"]
        for date in sorted_dates:
            data = forecast_data[date]
            
            # Extract and format values
            parts = []
            
            # Temperature
            temp = data.get("temperature_air_mean_2m")
            if temp is not None:
                parts.append(f"Temp: {temp:.1f} °C")
                
            # Wind
            wind_speed = data.get("wind_speed")
            wind_dir = data.get("wind_direction")
            if wind_speed is not None:
                wind_str = f"Wind: {wind_speed:.1f} m/s"
                if wind_dir is not None:
                    wind_str += f" ({get_wind_direction_label(wind_dir)})"
                parts.append(wind_str)
                
            # Precipitation
            precip = data.get("precipitation_height_significant_weather_last_1h")
            if precip is not None:
                parts.append(f"Precip: {precip:.1f} mm")
            
            # Snow
            snow = data.get("water_equivalent_snow_depth_new_last_1h")
            if snow is not None and snow > 0:
                parts.append(f"Snow: {snow:.1f} mm")
                
            # Cloud Cover
            clouds = data.get("cloud_cover_total")
            if clouds is not None:
                parts.append(f"Clouds: {clouds:.0f}%")
            
            line = f"{date}: {', '.join(parts)}"
            response_lines.append(line)

        return [types.TextContent(type="text", text="\n".join(response_lines))]

    except Exception as e:
        logger.error(f"Error fetching forecast: {e}")
        return [types.TextContent(type="text", text=f"Error fetching forecast data: {str(e)}")]

async def get_historical_weather(arguments: Any) -> List[types.TextContent]:
    latitude = arguments.get("latitude")
    longitude = arguments.get("longitude")
    start_date_str = arguments.get("start_date")
    end_date_str = arguments.get("end_date")

    if latitude is None or longitude is None or not start_date_str or not end_date_str:
        raise ValueError("latitude, longitude, start_date, and end_date are required")

    try:
        # Parse dates
        start_date = datetime.datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
        end_date = datetime.datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        
        # Ensure UTC if no timezone provided
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=datetime.timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=datetime.timezone.utc)
            
        parameters = [
            ("hourly", "temperature_air", "temperature_air_mean_2m"),
            ("hourly", "precipitation", "precipitation_height"),
            ("hourly", "wind", "wind_speed"),
            ("hourly", "wind", "wind_direction"),
            ("hourly", "cloud_type", "cloud_cover_total"),
        ]
        
        request = DwdObservationRequest(
            parameters=parameters,
            periods=["historical", "recent"]
        )
        
        stations = request.filter_by_rank(latlon=(latitude, longitude), rank=1)
        
        if stations.df.is_empty():
            return [types.TextContent(type="text", text="No weather station found nearby.")]
        
        # Get unique closest stations for each dataset
        best_stations_df = stations.df.sort("distance").unique(subset=["dataset"], keep="first")
        target_station_ids = best_stations_df["station_id"].unique().to_list()
        
        # Map dataset to station info for display
        dataset_station_map = {}
        for row in best_stations_df.iter_rows(named=True):
            dataset_station_map[row["dataset"]] = {
                "name": row["name"],
                "id": row["station_id"],
                "distance": row["distance"]
            }

        stations_filtered = request.filter_by_station_id(station_id=target_station_ids)
        values = stations_filtered.values.all()
        df = values.df
        
        if df.is_empty():
             return [types.TextContent(type="text", text="No historical data available for the nearest stations.")]

        # Manual filter by date
        df = df.filter((pl.col("date") >= start_date) & (pl.col("date") <= end_date))
        
        if df.is_empty():
             return [types.TextContent(type="text", text=f"No data found in the requested date range ({start_date} to {end_date}).")]

        # Summary statistics
        response_lines = [f"Historical Weather Summary:"]
        response_lines.append(f"Period: {start_date} to {end_date}")
        response_lines.append("")
        
        # Temperature
        temp_df = df.filter(pl.col("parameter") == "temperature_air_mean_2m").drop_nulls(subset=["value"])
        if not temp_df.is_empty():
            min_temp = temp_df["value"].min()
            max_temp = temp_df["value"].max()
            avg_temp = temp_df["value"].mean()
            
            # Find when min/max occurred
            min_row = temp_df.sort("value").head(1).row(0, named=True)
            max_row = temp_df.sort("value", descending=True).head(1).row(0, named=True)
            
            # Find station name for temperature
            st_info = dataset_station_map.get("temperature_air", {"name": "Unknown"})
            
            response_lines.append(f"Temperature (from {st_info['name']}):")
            response_lines.append(f"- Min: {min_temp:.1f} °C (on {min_row['date']})")
            response_lines.append(f"- Max: {max_temp:.1f} °C (on {max_row['date']})")
            response_lines.append(f"- Average: {avg_temp:.1f} °C")
            response_lines.append("")
            
        # Precipitation
        precip_df = df.filter(pl.col("parameter") == "precipitation_height").drop_nulls(subset=["value"])
        if not precip_df.is_empty():
            total_precip = precip_df["value"].sum()
            max_precip_row = precip_df.sort("value", descending=True).head(1).row(0, named=True)
            
            st_info = dataset_station_map.get("precipitation", {"name": "Unknown"})

            response_lines.append(f"Precipitation (from {st_info['name']}):")
            response_lines.append(f"- Total: {total_precip:.1f} mm")
            if total_precip > 0:
                response_lines.append(f"- Max Hourly: {max_precip_row['value']:.1f} mm (on {max_precip_row['date']})")
            response_lines.append("")
            
        # Wind
        wind_df = df.filter(pl.col("parameter") == "wind_speed").drop_nulls(subset=["value"])
        if not wind_df.is_empty():
            max_wind_row = wind_df.sort("value", descending=True).head(1).row(0, named=True)
            avg_wind = wind_df["value"].mean()
            
            st_info = dataset_station_map.get("wind", {"name": "Unknown"})

            response_lines.append(f"Wind (from {st_info['name']}):")
            response_lines.append(f"- Max Speed: {max_wind_row['value']:.1f} m/s (on {max_wind_row['date']})")
            response_lines.append(f"- Average Speed: {avg_wind:.1f} m/s")
            response_lines.append("")

        if len(response_lines) <= 3:
            # Only header and period, no actual data
            return [types.TextContent(type="text", text=f"No relevant weather parameters found in the requested range for the nearby stations. Found parameters: {df['parameter'].unique().to_list()}")]

        return [types.TextContent(type="text", text="\n".join(response_lines))]

    except Exception as e:
        logger.error(f"Error fetching historical weather: {e}")
        return [types.TextContent(type="text", text=f"Error fetching historical weather data: {str(e)}")]

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())
