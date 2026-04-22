import pytest
import polars as pl
from unittest.mock import MagicMock, patch
import datetime

from src.dwd_mcp.utils import get_wind_direction_label
from src.dwd_mcp.weather import fetch_current_weather, fetch_forecast, fetch_historical_weather, _station_cache

@pytest.fixture(autouse=True)
def clear_cache():
    _station_cache.clear()

def test_get_wind_direction_label():
    assert get_wind_direction_label(0) == "N"
    assert get_wind_direction_label(90) == "E"
    assert get_wind_direction_label(180) == "S"
    assert get_wind_direction_label(270) == "W"
    assert get_wind_direction_label(360) == "N"
    assert get_wind_direction_label(22.5) == "NNE"
    assert get_wind_direction_label(None) == "Unknown"

@pytest.mark.asyncio
@patch("src.dwd_mcp.weather.DwdObservationRequest")
async def test_fetch_current_weather_success(mock_request_cls):
    # Setup mock
    mock_request = mock_request_cls.return_value
    
    # Mock stations_all
    mock_stations_all = MagicMock()
    mock_stations_all.df = pl.DataFrame({
        "station_id": ["10382"],
        "name": ["Berlin-Tempelhof"],
        "distance": [1.5],
        "dataset": ["temperature_air"]
    })
    mock_request.filter_by_rank.return_value = mock_stations_all
    
    # Mock stations_filtered
    mock_stations_filtered = MagicMock()
    mock_request.filter_by_station_id.return_value = mock_stations_filtered
    
    # Mock values
    mock_values = MagicMock()
    now = datetime.datetime.now(datetime.timezone.utc)
    mock_values.df = pl.DataFrame({
        "station_id": ["10382"] * 3,
        "parameter": ["temperature_air_mean_2m", "humidity", "precipitation_form"],
        "value": [15.5, 80.0, 3.0], # 3.0 is Snow
        "date": [now] * 3
    })
    mock_stations_filtered.values.all.return_value = mock_values
    
    result = await fetch_current_weather(52.52, 13.40)
    
    assert "Temperature: 15.5 °C" in result
    assert "Humidity: 80.0 %" in result
    assert "Precipitation Form: Snow" in result
    assert "Berlin-Tempelhof" in result

@pytest.mark.asyncio
@patch("src.dwd_mcp.weather.DwdMosmixRequest")
async def test_fetch_forecast_success(mock_request_cls):
    # Setup mock
    mock_request = mock_request_cls.return_value
    
    # Mock stations
    mock_stations = MagicMock()
    mock_stations.df = pl.DataFrame({
        "station_id": ["10382"],
        "name": ["Berlin-Tempelhof"]
    })
    mock_request.filter_by_rank.return_value = mock_stations
    
    # Mock values
    mock_values = MagicMock()
    # Mock filtered request
    mock_stations_req = MagicMock()
    mock_request.filter_by_station_id.return_value = mock_stations_req
    
    future_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
    mock_values.df = pl.DataFrame({
        "parameter": ["temperature_air_mean_2m", "wind_speed", "significant_weather"],
        "value": [10.0, 5.0, 71.0], # 71 is Light Snow
        "date": [future_date] * 3
    })
    mock_stations_req.values.all.return_value = mock_values
    
    result = await fetch_forecast(52.52, 13.40, hours=1)
    
    assert "Berlin-Tempelhof" in result or "10382" in result
    assert "Temp: 10.0 °C" in result
    assert "Weather: Light Snow" in result
    assert "Wind: 5.0 m/s" in result
