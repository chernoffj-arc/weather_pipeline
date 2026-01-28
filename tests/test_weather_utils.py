"""
Unit tests for weather_utils module.

These tests run locally without requiring Spark or Databricks connectivity,
making them suitable for CI/CD pipelines before bundle deployment.
"""

import pytest
from unittest.mock import patch, Mock
from datetime import datetime
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from weather_utils import (
    fetch_weather_data,
    parse_weather_response,
    validate_weather_record,
    get_weather_description,
    CALGARY_LAT,
    CALGARY_LON,
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def sample_api_response():
    """Sample Open-Meteo API response for testing."""
    return {
        "latitude": 51.05,
        "longitude": -114.05,
        "timezone": "America/Edmonton",
        "current": {
            "time": "2025-01-28T10:00",
            "temperature_2m": -5.2,
            "relative_humidity_2m": 72,
            "wind_speed_10m": 15.5,
            "wind_direction_10m": 270,
            "weather_code": 3
        }
    }


@pytest.fixture
def valid_weather_record():
    """Valid weather record for testing."""
    return {
        "timestamp": datetime(2025, 1, 28, 10, 0),
        "latitude": CALGARY_LAT,
        "longitude": CALGARY_LON,
        "temperature": -5.2,
        "humidity": 72,
        "wind_speed": 15.5,
        "wind_direction": 270,
        "weather_code": 3,
        "city": "Calgary",
        "fetch_time": datetime.now()
    }


# =============================================================================
# Tests for fetch_weather_data
# =============================================================================

class TestFetchWeatherData:
    """Tests for the fetch_weather_data function."""
    
    @patch('weather_utils.requests.get')
    def test_successful_fetch(self, mock_get, sample_api_response):
        """Test successful API call returns weather data."""
        mock_response = Mock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        result = fetch_weather_data()
        
        assert result is not None
        assert result["latitude"] == CALGARY_LAT
        assert result["longitude"] == CALGARY_LON
        assert result["temperature"] == -5.2
        assert result["humidity"] == 72
        assert result["city"] == "Calgary"
    
    @patch('weather_utils.requests.get')
    def test_custom_coordinates(self, mock_get, sample_api_response):
        """Test fetch with custom coordinates."""
        mock_response = Mock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        result = fetch_weather_data(latitude=40.7128, longitude=-74.0060, city="New York")
        
        assert result["latitude"] == 40.7128
        assert result["longitude"] == -74.0060
        assert result["city"] == "New York"
    
    @patch('weather_utils.requests.get')
    def test_api_timeout(self, mock_get):
        """Test that timeout errors are handled gracefully."""
        mock_get.side_effect = Exception("Connection timeout")
        
        result = fetch_weather_data()
        
        assert result is None
    
    @patch('weather_utils.requests.get')
    def test_api_error_response(self, mock_get):
        """Test that HTTP errors are handled gracefully."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")
        mock_get.return_value = mock_response
        
        result = fetch_weather_data()
        
        assert result is None


# =============================================================================
# Tests for parse_weather_response
# =============================================================================

class TestParseWeatherResponse:
    """Tests for the parse_weather_response function."""
    
    def test_parse_valid_response(self, sample_api_response):
        """Test parsing a valid API response."""
        result = parse_weather_response(sample_api_response)
        
        assert result is not None
        assert result["temperature"] == -5.2
        assert result["humidity"] == 72
        assert result["wind_speed"] == 15.5
        assert result["weather_code"] == 3
        assert result["city"] == "Calgary"
    
    def test_parse_with_custom_city(self, sample_api_response):
        """Test parsing with a custom city name."""
        result = parse_weather_response(sample_api_response, city="Edmonton")
        
        assert result["city"] == "Edmonton"
    
    def test_parse_empty_response(self):
        """Test parsing an empty response."""
        result = parse_weather_response({})
        
        assert result is None
    
    def test_parse_missing_current(self):
        """Test parsing response with missing 'current' key."""
        response = {"latitude": 51.05, "longitude": -114.05}
        result = parse_weather_response(response)
        
        assert result is None
    
    def test_parse_partial_current_data(self):
        """Test parsing response with partial current data."""
        response = {
            "latitude": 51.05,
            "longitude": -114.05,
            "current": {
                "time": "2025-01-28T10:00",
                "temperature_2m": -5.2
                # Missing other fields
            }
        }
        result = parse_weather_response(response)
        
        assert result is not None
        assert result["temperature"] == -5.2
        assert result["humidity"] is None  # Missing field should be None


# =============================================================================
# Tests for validate_weather_record
# =============================================================================

class TestValidateWeatherRecord:
    """Tests for the validate_weather_record function."""
    
    def test_valid_record(self, valid_weather_record):
        """Test that a valid record passes validation."""
        assert validate_weather_record(valid_weather_record) is True
    
    def test_none_record(self):
        """Test that None fails validation."""
        assert validate_weather_record(None) is False
    
    def test_missing_required_field(self, valid_weather_record):
        """Test that missing required fields fail validation."""
        del valid_weather_record["temperature"]
        assert validate_weather_record(valid_weather_record) is False
    
    def test_none_required_field(self, valid_weather_record):
        """Test that None values in required fields fail validation."""
        valid_weather_record["latitude"] = None
        assert validate_weather_record(valid_weather_record) is False
    
    def test_invalid_latitude(self, valid_weather_record):
        """Test that invalid latitude fails validation."""
        valid_weather_record["latitude"] = 100  # > 90
        assert validate_weather_record(valid_weather_record) is False
        
        valid_weather_record["latitude"] = -100  # < -90
        assert validate_weather_record(valid_weather_record) is False
    
    def test_invalid_longitude(self, valid_weather_record):
        """Test that invalid longitude fails validation."""
        valid_weather_record["longitude"] = 200  # > 180
        assert validate_weather_record(valid_weather_record) is False
    
    def test_invalid_temperature(self, valid_weather_record):
        """Test that unreasonable temperature fails validation."""
        valid_weather_record["temperature"] = 100  # > 60°C
        assert validate_weather_record(valid_weather_record) is False
        
        valid_weather_record["temperature"] = -150  # < -100°C
        assert validate_weather_record(valid_weather_record) is False
    
    def test_edge_case_valid_coordinates(self, valid_weather_record):
        """Test boundary values for coordinates."""
        # Test valid extremes
        valid_weather_record["latitude"] = 90
        valid_weather_record["longitude"] = 180
        assert validate_weather_record(valid_weather_record) is True
        
        valid_weather_record["latitude"] = -90
        valid_weather_record["longitude"] = -180
        assert validate_weather_record(valid_weather_record) is True


# =============================================================================
# Tests for get_weather_description
# =============================================================================

class TestGetWeatherDescription:
    """Tests for the get_weather_description function."""
    
    @pytest.mark.parametrize("code,expected", [
        (0, "Clear sky"),
        (1, "Mainly clear"),
        (2, "Partly cloudy"),
        (3, "Overcast"),
        (45, "Fog"),
        (61, "Slight rain"),
        (71, "Slight snow"),
        (95, "Thunderstorm"),
    ])
    def test_known_weather_codes(self, code, expected):
        """Test that known weather codes return correct descriptions."""
        assert get_weather_description(code) == expected
    
    def test_unknown_weather_code(self):
        """Test that unknown codes return 'Unknown'."""
        assert get_weather_description(999) == "Unknown"
        assert get_weather_description(-1) == "Unknown"
    
    def test_calgary_winter_codes(self):
        """Test weather codes common in Calgary winters."""
        # Snow codes
        assert get_weather_description(71) == "Slight snow"
        assert get_weather_description(73) == "Moderate snow"
        assert get_weather_description(75) == "Heavy snow"
        assert get_weather_description(77) == "Snow grains"


# =============================================================================
# Integration-style tests (still no Spark required)
# =============================================================================

class TestWeatherPipelineFlow:
    """Tests that verify the flow of data through multiple functions."""
    
    @patch('weather_utils.requests.get')
    def test_fetch_and_validate_flow(self, mock_get, sample_api_response):
        """Test fetching data and validating it."""
        mock_response = Mock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Fetch the data
        weather_data = fetch_weather_data()
        
        # Validate it
        is_valid = validate_weather_record(weather_data)
        
        assert is_valid is True
    
    def test_parse_and_validate_flow(self, sample_api_response):
        """Test parsing response and validating the result."""
        # Parse the response
        parsed = parse_weather_response(sample_api_response)
        
        # Validate it
        is_valid = validate_weather_record(parsed)
        
        assert is_valid is True
    
    def test_full_description_flow(self, sample_api_response):
        """Test getting weather description from parsed data."""
        parsed = parse_weather_response(sample_api_response)
        
        description = get_weather_description(parsed["weather_code"])
        
        assert description == "Overcast"  # Code 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
