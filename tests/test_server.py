import pytest
from server import get_wind_direction_label, get_current_weather, get_forecast, get_historical_weather
import polars as pl
from unittest.mock import MagicMock, patch
import datetime
import mcp.types as types

def test_get_wind_direction_label():
    assert get_wind_direction_label(0) == "N"
    assert get_wind_direction_label(90) == "E"
    assert get_wind_direction_label(180) == "S"
    assert get_wind_direction_label(270) == "W"
    assert get_wind_direction_label(360) == "N"
    assert get_wind_direction_label(22.5) == "NNE"
    assert get_wind_direction_label(None) == "Unknown"

@pytest.mark.asyncio
@patch("server.DwdObservationRequest")
async def test_get_current_weather_no_stations(mock_request_cls):
    # Setup mock
    mock_request = mock_request_cls.return_value
    mock_stations = MagicMock()
    mock_stations.df = pl.DataFrame()
    mock_request.filter_by_rank.return_value = mock_stations
    
    arguments = {"latitude": 52.52, "longitude": 13.40}
    result = await get_current_weather(arguments)
    
    assert len(result) == 1
    assert "No weather station found nearby" in result[0].text

@pytest.mark.asyncio
@patch("server.DwdObservationRequest")
async def test_get_current_weather_success(mock_request_cls):
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
        "station_id": ["10382", "10382"],
        "parameter": ["temperature_air_mean_2m", "humidity"],
        "value": [15.5, 80.0],
        "date": [now, now]
    })
    mock_stations_filtered.values.all.return_value = mock_values
    
    arguments = {"latitude": 52.52, "longitude": 13.40}
    result = await get_current_weather(arguments)
    
    assert len(result) == 1
    assert "Temperature: 15.5 °C" in result[0].text
    assert "Humidity: 80.0 %" in result[0].text
    assert "Berlin-Tempelhof" in result[0].text

@pytest.mark.asyncio
@patch("server.DwdMosmixRequest")
async def test_get_forecast_success(mock_request_cls):
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
    future_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
    mock_values.df = pl.DataFrame({
        "parameter": ["temperature_air_mean_2m", "wind_speed"],
        "value": [10.0, 5.0],
        "date": [future_date, future_date]
    })
    mock_stations.values.all.return_value = mock_values
    
    arguments = {"latitude": 52.52, "longitude": 13.40, "hours": 1}
    result = await get_forecast(arguments)
    
    assert len(result) == 1
    assert "Forecast for Berlin-Tempelhof" in result[0].text
    assert "Temp: 10.0 °C" in result[0].text
    assert "Wind: 5.0 m/s" in result[0].text

@pytest.mark.asyncio
@patch("server.DwdObservationRequest")
async def test_get_historical_weather_success(mock_request_cls):
    # Setup mock
    mock_request = mock_request_cls.return_value
    
    # Mock stations
    mock_stations = MagicMock()
    mock_stations.df = pl.DataFrame({
        "station_id": ["10382"] * 5,
        "name": ["Berlin-Tempelhof"] * 5,
        "distance": [1.5] * 5,
        "dataset": ["temperature_air", "pressure", "precipitation", "wind", "solar"]
    })
    mock_request.filter_by_rank.return_value = mock_stations
    
    # Mock stations_filtered
    mock_stations_filtered = MagicMock()
    mock_request.filter_by_station_id.return_value = mock_stations_filtered
    
    # Mock values
    mock_values = MagicMock()
    date1 = datetime.datetime(2024, 12, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    date2 = datetime.datetime(2024, 12, 1, 13, 0, 0, tzinfo=datetime.timezone.utc)
    
    mock_values.df = pl.DataFrame({
        "station_id": ["10382"] * 12,
        "parameter": [
            "temperature_air_mean_2m", "temperature_air_mean_2m",
            "humidity", "humidity",
            "pressure_air_site", "pressure_air_site",
            "precipitation_height", "precipitation_height",
            "wind_speed", "wind_speed",
            "radiation_global", "radiation_global"
        ],
        "value": [
            5.0, 7.0, 
            80.0, 90.0,
            1010.0, 1012.0,
            1.0, 2.0,
            3.0, 5.0,
            100.0, 200.0
        ],
        "date": [date1, date2] * 6
    })
    mock_stations_filtered.values.all.return_value = mock_values
    
    arguments = {
        "latitude": 52.52, 
        "longitude": 13.40, 
        "start_date": "2024-12-01", 
        "end_date": "2024-12-02"
    }
    result = await get_historical_weather(arguments)
    
    assert len(result) == 1
    assert "Historical Weather Summary" in result[0].text
    assert "Temperature (from Berlin-Tempelhof)" in result[0].text
    assert "Min: 5.0 °C" in result[0].text
    assert "Max: 7.0 °C" in result[0].text
    assert "Average: 6.0 °C" in result[0].text
    assert "Humidity: Avg 85.0%, Min 80%, Max 90%" in result[0].text
    assert "Pressure (from Berlin-Tempelhof)" in result[0].text
    assert "Average: 1011.0 hPa" in result[0].text
    assert "Total: 3.0 mm" in result[0].text
    assert "Average Speed: 4.0 m/s" in result[0].text
    assert "Solar Radiation (from Berlin-Tempelhof)" in result[0].text
    assert "Total: 300.0 J/cm²" in result[0].text
