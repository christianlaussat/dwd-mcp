import logging
import datetime
from typing import Any, Dict, List, Optional, Tuple
import polars as pl

from wetterdienst.provider.dwd.observation import DwdObservationRequest
from wetterdienst.provider.dwd.mosmix import DwdMosmixRequest
from src.dwd_mcp.utils import (
    get_wind_direction_label,
    get_precipitation_form_label,
    get_significant_weather_label,
)

logger = logging.getLogger("dwd-mcp")

# Simple in-memory cache for station lookups
# Key: (lat, lon, rank, request_type), Value: (stations_df, timestamp)
_station_cache: Dict[Tuple[float, float, int, str], Tuple[pl.DataFrame, datetime.datetime]] = {}
CACHE_TTL = datetime.timedelta(hours=1)

def _get_cached_stations(lat: float, lon: float, rank: int, request_type: str) -> Optional[pl.DataFrame]:
    key = (lat, lon, rank, request_type)
    if key in _station_cache:
        df, ts = _station_cache[key]
        if datetime.datetime.now() - ts < CACHE_TTL:
            return df
    return None

def _set_cached_stations(lat: float, lon: float, rank: int, request_type: str, df: pl.DataFrame):
    _station_cache[(lat, lon, rank, request_type)] = (df, datetime.datetime.now())

async def search_stations(name: Optional[str] = None, state: Optional[str] = None) -> str:
    """Search for DWD weather stations by name or state."""
    try:
        request = DwdObservationRequest(
            parameters=[("hourly", "temperature_air")],
            periods="recent"
        )
        stations = request.all()
        df = stations.df
        
        if name:
            df = df.filter(pl.col("name").str.contains(f"(?i){name}"))
        if state:
            df = df.filter(pl.col("state").str.contains(f"(?i){state}"))
            
        if df.is_empty():
            return "No stations found matching the criteria."
            
        # Limit to top 20 results for readability
        df = df.head(20)
        
        lines = ["Found Stations (limit 20):"]
        for row in df.iter_rows(named=True):
            lines.append(f"- {row['name']} (ID: {row['station_id']}, State: {row['state']}, Lat: {row['latitude']}, Lon: {row['longitude']})")
            
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error searching stations: {e}")
        return f"Error searching stations: {str(e)}"

async def fetch_current_weather(latitude: float, longitude: float) -> str:
    """Fetch current weather for a location."""
    try:
        parameters = [
            ("hourly", "temperature_air"),
            ("hourly", "precipitation"),
            ("hourly", "wind"),
            ("hourly", "cloud_type"),
            ("hourly", "pressure"),
            ("hourly", "solar"),
        ]
        
        request = DwdObservationRequest(parameters=parameters, periods="recent")
        
        # Use cache for station lookup
        stations_df = _get_cached_stations(latitude, longitude, 1, "observation")
        if stations_df is None:
            stations_all = request.filter_by_rank(latlon=(latitude, longitude), rank=1)
            stations_df = stations_all.df
            _set_cached_stations(latitude, longitude, 1, "observation", stations_df)
        
        if stations_df.is_empty():
            return "No weather station found nearby."
        
        best_stations_df = stations_df.sort("distance").unique(subset=["dataset"], keep="first")
        target_station_ids = best_stations_df["station_id"].unique().to_list()
        
        dataset_station_map = {
            row["dataset"]: {"name": row["name"], "id": row["station_id"], "distance": row["distance"]}
            for row in best_stations_df.iter_rows(named=True)
        }

        stations_filtered = request.filter_by_station_id(station_id=target_station_ids)
        values = stations_filtered.values.all()
        df = values.df.drop_nulls(subset=["value"])
        
        if df.is_empty():
            return "No recent data available for the nearest stations."

        latest_df = df.sort("date", descending=True).unique(subset=["station_id", "parameter"], keep="first")
        
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
        
        display_order = [
            "temperature_air_mean_2m", "humidity", "pressure_air_site", "pressure_air_sea_level",
            "precipitation_height", "precipitation_form", "wind_speed", "wind_direction", "cloud_cover_total", "radiation_global"
        ]
        
        results = {}
        latest_time = None
        
        for row in latest_df.iter_rows(named=True):
            param = row["parameter"]
            if param in param_map:
                if latest_time is None or row["date"] > latest_time:
                    latest_time = row["date"]
                
                best_info = dataset_station_map.get(param_map[param]["dataset"])
                if param not in results or (best_info and best_info["id"] == row["station_id"]):
                    results[param] = {"value": row["value"], "station_id": row["station_id"], "date": row["date"]}

        response_lines = [f"Current Weather (latest reading: {latest_time}):"] if latest_time else ["Current Weather:"]
            
        for param in display_order:
            if param in results:
                data = results[param]
                info = param_map[param]
                
                st_row = best_stations_df.filter(pl.col("station_id") == data["station_id"]).head(1)
                station_str = f"{st_row.row(0, named=True)['name']} ({st_row.row(0, named=True)['distance']:.1f} km)" if not st_row.is_empty() else f"ID: {data['station_id']}"

                val = data["value"]
                formatted_val = f"{val} {info['unit']}"
                if param == "wind_direction":
                    formatted_val = f"{val}° ({get_wind_direction_label(val)})"
                elif param == "precipitation_form":
                    formatted_val = get_precipitation_form_label(val)
                elif param == "cloud_cover_total":
                    formatted_val = f"{int(val)}/8"
                
                response_lines.append(f"- {info['label']}: {formatted_val} [from {station_str}]")
        
        return "\n".join(response_lines) if results else "No relevant weather parameters found in recent data."
    except Exception as e:
        logger.error(f"Error fetching weather: {e}")
        return f"Error fetching weather data: {str(e)}"

async def fetch_forecast(latitude: float, longitude: float, hours: int = 24) -> str:
    """Fetch weather forecast for a location."""
    try:
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
        
        # Use cache for station lookup
        stations_df = _get_cached_stations(latitude, longitude, 1, "mosmix")
        if stations_df is None:
            stations = request.filter_by_rank(latlon=(latitude, longitude), rank=1)
            stations_df = stations.df
            _set_cached_stations(latitude, longitude, 1, "mosmix", stations_df)
        
        if stations_df.is_empty():
            return "No weather station found nearby."
            
        station_info = stations_df.row(0, named=True)
        station_name = station_info.get("name")
        station_id = station_info.get("station_id")

        # Wetterdienst filter_by_station_id can be used if we want to be explicit, 
        # but filter_by_rank already returned the rank 1 station request.
        # We need to get the values for that station.
        # request was already filtered if we didn't use cache, but if we DID use cache, we need to re-filter.
        # To keep it simple, we'll just use the filtered request from the first call if not cached.
        
        stations_req = request.filter_by_station_id(station_id=[station_id])
        values = stations_req.values.all()
        df = values.df.drop_nulls(subset=["value"])
        
        if df.is_empty():
             return "No forecast data available."

        forecast_data = {}
        for row in df.iter_rows(named=True):
            d, p, v = row["date"], row["parameter"], row["value"]
            if d not in forecast_data: forecast_data[d] = {}
            forecast_data[d][p] = v
            
        now = datetime.datetime.now(datetime.timezone.utc)
        sorted_dates = sorted([d for d in forecast_data.keys() if d >= now - datetime.timedelta(hours=1)])[:hours]
        
        if not sorted_dates:
             return "No future forecast data available."

        response_lines = [f"Forecast for {station_name} (ID: {station_id}):"]
        for date in sorted_dates:
            data = forecast_data[date]
            parts = []
            
            if (v := data.get("temperature_air_mean_2m")) is not None: parts.append(f"Temp: {v:.1f} °C")
            if (v := data.get("significant_weather")) is not None: parts.append(f"Weather: {get_significant_weather_label(v)}")
            if (v := data.get("humidity_air_mean_2m")) is not None: parts.append(f"Hum: {v:.1f} %")
            if (v := data.get("pressure_air_site_mean_2m")) is not None: parts.append(f"Press: {v:.1f} hPa")
            
            wind_speed = data.get("wind_speed")
            wind_dir = data.get("wind_direction")
            if wind_speed is not None:
                wind_str = f"Wind: {wind_speed:.1f} m/s"
                if wind_dir is not None: wind_str += f" ({get_wind_direction_label(wind_dir)})"
                parts.append(wind_str)
                
            if (v := data.get("precipitation_height_significant_weather_last_1h")) is not None: parts.append(f"Precip: {v:.1f} mm")
            if (v := data.get("water_equivalent_snow_depth_new_last_1h")) is not None and v > 0: parts.append(f"Snow: {v:.1f} mm")
            if (v := data.get("cloud_cover_total")) is not None: parts.append(f"Clouds: {v:.0f}%")
            if (v := data.get("radiation_global_last_1h")) is not None: parts.append(f"Solar: {v:.0f} J/cm²")
            
            response_lines.append(f"{date}: {', '.join(parts)}")

        return "\n".join(response_lines)
    except Exception as e:
        logger.error(f"Error fetching forecast: {e}")
        return f"Error fetching forecast data: {str(e)}"

async def fetch_historical_weather(latitude: float, longitude: float, start_date_str: str, end_date_str: str) -> str:
    """Fetch historical weather summary."""
    try:
        start_date = datetime.datetime.fromisoformat(start_date_str.replace("Z", "+00:00")).replace(tzinfo=datetime.timezone.utc if "Z" in start_date_str or "+" in start_date_str else None)
        end_date = datetime.datetime.fromisoformat(end_date_str.replace("Z", "+00:00")).replace(tzinfo=datetime.timezone.utc if "Z" in end_date_str or "+" in end_date_str else None)
        
        if start_date.tzinfo is None: start_date = start_date.replace(tzinfo=datetime.timezone.utc)
        if end_date.tzinfo is None: end_date = end_date.replace(tzinfo=datetime.timezone.utc)
            
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
        
        request = DwdObservationRequest(parameters=parameters, periods=["historical", "recent"])
        
        # Station lookup
        stations_df = _get_cached_stations(latitude, longitude, 1, "historical")
        if stations_df is None:
            stations = request.filter_by_rank(latlon=(latitude, longitude), rank=1)
            stations_df = stations.df
            _set_cached_stations(latitude, longitude, 1, "historical", stations_df)
        
        if stations_df.is_empty():
            return "No weather station found nearby."
        
        best_stations_df = stations_df.sort("distance").unique(subset=["dataset"], keep="first")
        target_station_ids = best_stations_df["station_id"].unique().to_list()
        dataset_station_map = {row["dataset"]: {"name": row["name"], "id": row["station_id"]} for row in best_stations_df.iter_rows(named=True)}

        stations_filtered = request.filter_by_station_id(station_id=target_station_ids)
        values = stations_filtered.values.all()
        df = values.df.filter((pl.col("date") >= start_date) & (pl.col("date") <= end_date))
        
        if df.is_empty():
             return f"No data found in requested range ({start_date} to {end_date})."

        lines = [f"Historical Weather Summary:", f"Period: {start_date} to {end_date}", ""]
        
        # Temp/Hum
        temp_df = df.filter(pl.col("parameter") == "temperature_air_mean_2m").drop_nulls(subset=["value"])
        if not temp_df.is_empty():
            st_name = dataset_station_map.get("temperature_air", {"name": "Unknown"})["name"]
            min_row = temp_df.sort("value").head(1).row(0, named=True)
            max_row = temp_df.sort("value", descending=True).head(1).row(0, named=True)
            lines.extend([f"Temperature (from {st_name}):", f"- Min: {min_row['value']:.1f} °C (on {min_row['date']})", f"- Max: {max_row['value']:.1f} °C (on {max_row['date']})", f"- Average: {temp_df['value'].mean():.1f} °C"])
        
        hum_df = df.filter(pl.col("parameter") == "humidity").drop_nulls(subset=["value"])
        if not hum_df.is_empty():
            lines.append(f"- Humidity: Avg {hum_df['value'].mean():.1f}%, Min {hum_df['value'].min():.0f}%, Max {hum_df['value'].max():.0f}%")
        
        if not temp_df.is_empty() or not hum_df.is_empty(): lines.append("")
            
        # Pressure
        press_df = df.filter(pl.col("parameter").is_in(["pressure_air_site", "pressure_air_sea_level"])).drop_nulls(subset=["value"])
        if not press_df.is_empty():
            st_name = dataset_station_map.get("pressure", {"name": "Unknown"})["name"]
            lines.extend([f"Pressure (from {st_name}):", f"- Average: {press_df['value'].mean():.1f} hPa", f"- Range: {press_df['value'].min():.1f} to {press_df['value'].max():.1f} hPa", ""])

        # Precip
        precip_df = df.filter(pl.col("parameter") == "precipitation_height").drop_nulls(subset=["value"])
        precip_form_df = df.filter(pl.col("parameter") == "precipitation_form").drop_nulls(subset=["value"])
        snow_df = df.filter(pl.col("parameter") == "snow_depth").drop_nulls(subset=["value"])
        if not precip_df.is_empty() or not precip_form_df.is_empty() or not snow_df.is_empty():
            st_name = dataset_station_map.get("precipitation", {"name": "Unknown"})["name"]
            lines.append(f"Precipitation & Snow (from {st_name}):")
            if not precip_df.is_empty():
                max_row = precip_df.sort("value", descending=True).head(1).row(0, named=True)
                lines.append(f"- Total Precipitation: {precip_df['value'].sum():.1f} mm")
                if precip_df['value'].sum() > 0: lines.append(f"- Max Hourly Precip: {max_row['value']:.1f} mm (on {max_row['date']})")
            if not precip_form_df.is_empty():
                forms = [get_precipitation_form_label(f) for f in precip_form_df["value"].unique().to_list() if f > 0]
                if forms: lines.append(f"- Types observed: {', '.join(sorted(set(forms)))}")
            if not snow_df.is_empty(): lines.append(f"- Max Snow Depth: {snow_df['value'].max():.1f} cm")
            lines.append("")
            
        # Wind
        wind_df = df.filter(pl.col("parameter") == "wind_speed").drop_nulls(subset=["value"])
        if not wind_df.is_empty():
            st_name = dataset_station_map.get("wind", {"name": "Unknown"})["name"]
            max_row = wind_df.sort("value", descending=True).head(1).row(0, named=True)
            lines.extend([f"Wind (from {st_name}):", f"- Max Speed: {max_row['value']:.1f} m/s (on {max_row['date']})", f"- Average Speed: {wind_df['value'].mean():.1f} m/s", ""])

        # Solar
        solar_df = df.filter(pl.col("parameter") == "radiation_global").drop_nulls(subset=["value"])
        if not solar_df.is_empty():
            st_name = dataset_station_map.get("solar", {"name": "Unknown"})["name"]
            max_row = solar_df.sort("value", descending=True).head(1).row(0, named=True)
            lines.extend([f"Solar Radiation (from {st_name}):", f"- Total: {solar_df['value'].sum():.1f} J/cm²", f"- Max Hourly: {max_row['value']:.1f} J/cm² (on {max_row['date']})", ""])

        return "\n".join(lines) if len(lines) > 3 else f"No relevant data found. Parameters: {df['parameter'].unique().to_list()}"
    except Exception as e:
        logger.error(f"Error fetching historical weather: {e}")
        return f"Error fetching historical weather data: {str(e)}"
