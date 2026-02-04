#!/usr/bin/env python3
# /// script
# dependencies = [
#   "mcp",
#   "wetterdienst",
#   "polars",
# ]
# ///
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

def get_precipitation_form_label(code: float) -> str:
    if code is None:
        return "Unknown"
    # DWD WR codes:
    # 0: no precipitation
    # 1: rain
    # 2: unknown
    # 3: snow
    # 4: rain and snow
    # 5: unknown
    # 6: rain and snow (mixed)
    # 7: sleet/ice pellets
    # 8: hail
    # 9: no precipitation (with recent history)
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
    if code is None:
        return "Unknown"
    # DWD WW codes (MOSMIX) - more detailed mapping
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
            ("hourly", "pressure"),
            ("hourly", "solar"),
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
            "pressure_air_site": {"label": "Pressure (Station)", "unit": "hPa", "dataset": "pressure"},
            "pressure_air_sea_level": {"label": "Pressure (Sea Level)", "unit": "hPa", "dataset": "pressure"},
            "precipitation_height": {"label": "Precipitation", "unit": "mm", "dataset": "precipitation"},
            "precipitation_form": {"label": "Precipitation Form", "unit": "", "dataset": "precipitation"},
            "wind_speed": {"label": "Wind Speed", "unit": "m/s", "dataset": "wind"},
            "wind_direction": {"label": "Wind Direction", "unit": "°", "dataset": "wind"},
            "cloud_cover_total": {"label": "Cloud Cover", "unit": "1/8", "dataset": "cloud_type"},
            "radiation_global": {"label": "Global Radiation", "unit": "J/cm²", "dataset": "solar"},
        }
        
        # Order of display
        display_order = [
            "temperature_air_mean_2m", "humidity", "pressure_air_site", "pressure_air_sea_level",
            "precipitation_height", "precipitation_form", "wind_speed", "wind_direction", "cloud_cover_total", "radiation_global"
        ]
        
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
                station_name = "Unknown Station"
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
                elif param == "precipitation_form":
                    formatted_val = get_precipitation_form_label(val)
                elif param == "cloud_cover_total":
                     formatted_val = f"{int(val)}/8"
                
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
                ("hourly", "small", "humidity_air_mean_2m"),
                ("hourly", "small", "pressure_air_site_mean_2m"),
                ("hourly", "small", "precipitation_height_significant_weather_last_1h"),
                ("hourly", "small", "significant_weather"),
                ("hourly", "small", "wind_speed"),
                ("hourly", "small", "wind_direction"),
                ("hourly", "small", "cloud_cover_total"),
                ("hourly", "small", "water_equivalent_snow_depth_new_last_1h"),
                ("hourly", "small", "radiation_global_last_1h"),
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
            
            # Significant Weather
            ww = data.get("significant_weather")
            if ww is not None:
                parts.append(f"Weather: {get_significant_weather_label(ww)}")

            # Humidity
            hum = data.get("humidity_air_mean_2m")
            if hum is not None:
                parts.append(f"Hum: {hum:.1f} %")
                
            # Pressure
            press = data.get("pressure_air_site_mean_2m")
            if press is not None:
                parts.append(f"Press: {press:.1f} hPa")

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

            # Solar
            solar = data.get("radiation_global_last_1h")
            if solar is not None:
                parts.append(f"Solar: {solar:.0f} J/cm²")
            
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
            ("hourly", "temperature_air", "humidity"),
            ("hourly", "precipitation", "precipitation_height"),
            ("hourly", "precipitation", "precipitation_form"),
            ("hourly", "wind", "wind_speed"),
            ("hourly", "wind", "wind_direction"),
            ("hourly", "cloud_type", "cloud_cover_total"),
            ("hourly", "pressure", "pressure_air_site"),
            ("hourly", "pressure", "pressure_air_sea_level"),
            ("hourly", "solar", "radiation_global"),
            ("daily", "climate_summary", "snow_depth"),
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
        
        # Temperature & Humidity
        temp_df = df.filter(pl.col("parameter") == "temperature_air_mean_2m").drop_nulls(subset=["value"])
        hum_df = df.filter(pl.col("parameter") == "humidity").drop_nulls(subset=["value"])
        
        st_info_temp = dataset_station_map.get("temperature_air", {"name": "Unknown"})

        if not temp_df.is_empty():
            min_temp = temp_df["value"].min()
            max_temp = temp_df["value"].max()
            avg_temp = temp_df["value"].mean()
            
            min_row = temp_df.sort("value").head(1).row(0, named=True)
            max_row = temp_df.sort("value", descending=True).head(1).row(0, named=True)
            
            response_lines.append(f"Temperature (from {st_info_temp['name']}):")
            response_lines.append(f"- Min: {min_temp:.1f} °C (on {min_row['date']})")
            response_lines.append(f"- Max: {max_temp:.1f} °C (on {max_row['date']})")
            response_lines.append(f"- Average: {avg_temp:.1f} °C")
        
        if not hum_df.is_empty():
            avg_hum = hum_df["value"].mean()
            min_hum = hum_df["value"].min()
            max_hum = hum_df["value"].max()
            if temp_df.is_empty():
                response_lines.append(f"Humidity (from {st_info_temp['name']}):")
            response_lines.append(f"- Humidity: Avg {avg_hum:.1f}%, Min {min_hum:.0f}%, Max {max_hum:.0f}%")
        
        if not temp_df.is_empty() or not hum_df.is_empty():
            response_lines.append("")
            
        # Pressure
        press_df = df.filter(pl.col("parameter") == "pressure_air_site").drop_nulls(subset=["value"])
        if press_df.is_empty():
             press_df = df.filter(pl.col("parameter") == "pressure_air_sea_level").drop_nulls(subset=["value"])
        
        if not press_df.is_empty():
            avg_press = press_df["value"].mean()
            min_press = press_df["value"].min()
            max_press = press_df["value"].max()
            st_info = dataset_station_map.get("pressure", {"name": "Unknown"})
            response_lines.append(f"Pressure (from {st_info['name']}):")
            response_lines.append(f"- Average: {avg_press:.1f} hPa")
            response_lines.append(f"- Range: {min_press:.1f} to {max_press:.1f} hPa")
            response_lines.append("")

        # Precipitation & Snow
        precip_df = df.filter(pl.col("parameter") == "precipitation_height").drop_nulls(subset=["value"])
        precip_form_df = df.filter(pl.col("parameter") == "precipitation_form").drop_nulls(subset=["value"])
        snow_df = df.filter(pl.col("parameter") == "snow_depth").drop_nulls(subset=["value"])
        
        if not precip_df.is_empty() or not precip_form_df.is_empty() or not snow_df.is_empty():
            st_info = dataset_station_map.get("precipitation", {"name": "Unknown"})
            response_lines.append(f"Precipitation & Snow (from {st_info['name']}):")
            
            if not precip_df.is_empty():
                total_precip = precip_df["value"].sum()
                max_precip_row = precip_df.sort("value", descending=True).head(1).row(0, named=True)
                response_lines.append(f"- Total Precipitation: {total_precip:.1f} mm")
                if total_precip > 0:
                    response_lines.append(f"- Max Hourly Precip: {max_precip_row['value']:.1f} mm (on {max_precip_row['date']})")
            
            if not precip_form_df.is_empty():
                forms = precip_form_df["value"].unique().to_list()
                if forms:
                    form_labels = [get_precipitation_form_label(f) for f in forms if f > 0]
                    if form_labels:
                        response_lines.append(f"- Types observed: {', '.join(sorted(set(form_labels)))}")
            
            if not snow_df.is_empty():
                max_snow = snow_df["value"].max()
                response_lines.append(f"- Max Snow Depth: {max_snow:.1f} cm")

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

        # Solar
        solar_df = df.filter(pl.col("parameter") == "radiation_global").drop_nulls(subset=["value"])
        if not solar_df.is_empty():
            total_solar = solar_df["value"].sum()
            max_solar_row = solar_df.sort("value", descending=True).head(1).row(0, named=True)
            st_info = dataset_station_map.get("solar", {"name": "Unknown"})
            response_lines.append(f"Solar Radiation (from {st_info['name']}):")
            response_lines.append(f"- Total: {total_solar:.1f} J/cm²")
            response_lines.append(f"- Max Hourly: {max_solar_row['value']:.1f} J/cm² (on {max_solar_row['date']})")
            response_lines.append("")

        if len(response_lines) <= 3:
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
