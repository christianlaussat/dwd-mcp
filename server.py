#!/usr/bin/env python3
import logging
from typing import Optional
from mcp.server.fastmcp import FastMCP

from src.dwd_mcp.weather import (
    fetch_current_weather,
    fetch_forecast,
    fetch_historical_weather,
    search_stations,
)

# Initialize FastMCP server
mcp = FastMCP("dwd-mcp")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dwd-mcp")

@mcp.tool()
async def get_current_weather(latitude: float, longitude: float) -> str:
    """Get current weather (temperature, clouds, precipitation, wind) for a location from DWD observation stations."""
    return await fetch_current_weather(latitude, longitude)

@mcp.tool()
async def get_forecast(latitude: float, longitude: float, hours: int = 24) -> str:
    """Get weather forecast (temperature, wind, precipitation, clouds) for a location using DWD MOSMIX."""
    return await fetch_forecast(latitude, longitude, hours)

@mcp.tool()
async def get_historical_weather(latitude: float, longitude: float, start_date: str, end_date: str) -> str:
    """Get historical weather data (temperature, precipitation, wind) for a location for a specific date range."""
    return await fetch_historical_weather(latitude, longitude, start_date, end_date)

@mcp.tool()
async def get_stations(name: Optional[str] = None, state: Optional[str] = None) -> str:
    """Search for DWD weather stations by name or state."""
    return await search_stations(name, state)

@mcp.resource("weather://current/{latitude}/{longitude}")
async def current_weather_resource(latitude: float, longitude: float) -> str:
    """Provide current weather for a location as a resource."""
    return await fetch_current_weather(latitude, longitude)

@mcp.prompt()
def summarize_weather(latitude: float, longitude: float) -> str:
    """Create a prompt for the LLM to summarize weather for a specific location."""
    return f"Please provide a concise summary of the current weather and the 24-hour forecast for the location at latitude {latitude} and longitude {longitude}. Focus on significant weather events or risks."

if __name__ == "__main__":
    mcp.run()
