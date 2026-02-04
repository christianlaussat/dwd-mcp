import pytest
import datetime
from server import get_current_weather, get_forecast, get_historical_weather
import mcp.types as types

# Use Berlin for integration tests
BERLIN_LAT = 52.52
BERLIN_LON = 13.40

@pytest.mark.asyncio
async def test_integration_current_weather():
    """Real API call to verify current weather retrieval."""
    arguments = {"latitude": BERLIN_LAT, "longitude": BERLIN_LON}
    result = await get_current_weather(arguments)
    
    assert isinstance(result, list)
    assert len(result) > 0
    assert isinstance(result[0], types.TextContent)
    # Check for some expected keywords in the output
    text = result[0].text
    assert "Current Weather" in text
    assert "Temperature" in text or "No weather station found" in text

@pytest.mark.asyncio
async def test_integration_forecast():
    """Real API call to verify forecast retrieval."""
    arguments = {"latitude": BERLIN_LAT, "longitude": BERLIN_LON, "hours": 3}
    result = await get_forecast(arguments)
    
    assert isinstance(result, list)
    assert len(result) > 0
    assert "Forecast for" in result[0].text
    # Forecast usually has multiple lines
    assert len(result[0].text.split("\n")) >= 2

@pytest.mark.asyncio
async def test_integration_historical_weather():
    """Real API call to verify historical weather summary."""
    # Last 2 days
    end_date = datetime.datetime.now(datetime.timezone.utc)
    start_date = end_date - datetime.timedelta(days=2)
    
    arguments = {
        "latitude": BERLIN_LAT, 
        "longitude": BERLIN_LON, 
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d")
    }
    result = await get_historical_weather(arguments)
    
    assert isinstance(result, list)
    assert len(result) > 0
    text = result[0].text
    assert "Historical Weather Summary" in text
    assert "Period:" in text
