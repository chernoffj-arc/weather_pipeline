"""
Pytest configuration and shared fixtures.
"""

import pytest
import sys
import os

# Ensure the src directory is in the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def calgary_coordinates():
    """Calgary coordinates for testing."""
    return {
        "latitude": 51.0447,
        "longitude": -114.0719
    }
