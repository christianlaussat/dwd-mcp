import pytest
import datetime
from src.dwd_mcp.weather import fetch_current_weather, fetch_forecast, fetch_historical_weather

# Use Berlin for integration tests
BERLIN_LAT = 52.52
BERLIN_LON = 13.40

@pytest.mark.asyncio
async def test_integration_current_weather():
    """Real API call to verify current weather retrieval."""
    result = await fetch_current_weather(BERLIN_LAT, BERLIN_LON)
    
    assert isinstance(result, str)
    assert "Current Weather" in result
    assert "Temperature" in result or "No weather station found" in result

@pytest.mark.asyncio
async def test_integration_forecast():
    """Real API call to verify forecast retrieval."""
    result = await fetch_forecast(BERLIN_LAT, BERLIN_LON, hours=3)
    
    assert isinstance(result, str)
    assert "Forecast for" in result
    # Forecast usually has multiple lines
    assert len(result.split("\n")) >= 2

@pytest.mark.asyncio
async def test_integration_historical_weather():
    """Real API call to verify historical weather summary."""
    # Last 2 days
    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(days=2)
    
    result = await fetch_historical_weather(
        BERLIN_LAT, 
        BERLIN_LON, 
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )
    
    assert isinstance(result, str)
    assert "Historical Weather Summary" in result
    assert "Period:" in result
