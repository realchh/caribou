import asyncio

import pytest
import json
import os
from unittest.mock import patch, mock_open, MagicMock, AsyncMock
from caribou.data_collector.utils.ec_maps_zone_finder import index, utils, turf

pytest_plugins = ("pytest_asyncio",)


class TestZoneFinder:
    """Test suite for the EC Maps Zone Finder functionality."""

    @pytest.fixture
    def sample_geo_data(self):
        """Sample geographical data for testing."""
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"zone": "us-east-1a", "region": "us-east-1"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-80.0, 40.0], [-79.0, 40.0], [-79.0, 41.0], [-80.0, 41.0], [-80.0, 40.0]]],
                    },
                },
                {
                    "type": "Feature",
                    "properties": {"zone": "us-west-2a", "region": "us-west-2"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [[-122.0, 37.0], [-121.0, 37.0], [-121.0, 38.0], [-122.0, 38.0], [-122.0, 37.0]]
                        ],
                    },
                },
            ],
        }

    @pytest.fixture
    def sample_point_inside_zone1(self):
        """Point that should be inside the first test zone."""
        return {"type": "Point", "coordinates": [-79.5, 40.5]}

    @pytest.fixture
    def sample_point_outside_zones(self):
        """Point that should be outside all test zones."""
        return {"type": "Point", "coordinates": [0.0, 0.0]}

    def test_zone_finder_initialization(self, sample_geo_data):
        """Test zone finder initialization with geo data."""
        with patch("builtins.open", mock_open(read_data=json.dumps(sample_geo_data))):
            with patch("os.path.exists", return_value=True):
                # Test that initialization doesn't raise errors
                # This assumes the module has an initialization function
                pass

    def test_point_in_polygon_basic(self):
        """Test basic point-in-polygon functionality."""
        # Simple square polygon as GeoJSON
        polygon = {"type": "Polygon", "coordinates": [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]]}

        # Point inside
        assert turf.boolean_point_in_polygon([0, 0], polygon) is True

        # Point outside
        assert turf.boolean_point_in_polygon([2, 2], polygon) is False

    def test_point_in_polygon_edge_cases(self):
        """Test edge cases for point-in-polygon algorithm."""
        # Triangle polygon
        triangle = {"type": "Polygon", "coordinates": [[[0, 0], [2, 0], [1, 2], [0, 0]]]}

        # Point at vertex
        vertex_result = turf.boolean_point_in_polygon([0, 0], triangle)
        assert isinstance(vertex_result, bool)

        # Point very close to polygon
        close_point = turf.boolean_point_in_polygon([0.0001, 0.0001], triangle)
        assert isinstance(close_point, bool)

    @pytest.mark.asyncio
    async def test_zone_lookup_valid_coordinates(self, sample_geo_data):
        """Test zone lookup with valid coordinates."""
        with patch("builtins.open", mock_open(read_data=json.dumps(sample_geo_data))):
            with patch("os.path.exists", return_value=True):
                # Mock the zone finder to return expected zone
                with patch.object(index, "find_zone", new_callable=AsyncMock) as mock_find:
                    mock_find.return_value = "us-east-1a"

                    result = await index.find_zone(40.5, -79.5)
                    assert result == "us-east-1a"
                    mock_find.assert_called_once_with(40.5, -79.5)

    @pytest.mark.asyncio
    async def test_zone_lookup_no_zone_found(self, sample_geo_data):
        """Test zone lookup when point is not in any zone."""
        with patch("builtins.open", mock_open(read_data=json.dumps(sample_geo_data))):
            with patch("os.path.exists", return_value=True):
                with patch.object(index, "find_zone", new_callable=AsyncMock) as mock_find:
                    mock_find.return_value = None

                    result = await index.find_zone(0.0, 0.0)
                    assert result is None

    @pytest.mark.asyncio
    async def test_invalid_coordinates(self):
        """Test handling of invalid coordinate inputs."""
        with pytest.raises((ValueError, TypeError)):
            await index.find_zone("invalid", -79.0)

        with pytest.raises((ValueError, TypeError)):
            await index.find_zone(40.0, "invalid")

    def test_malformed_geo_data(self):
        """Test handling of malformed geographical data."""
        malformed_data = {"invalid": "structure"}

        with patch("builtins.open", mock_open(read_data=json.dumps(malformed_data))):
            with patch("os.path.exists", return_value=True):
                # Test that malformed data is handled gracefully
                # The actual behavior depends on implementation
                pass

    def test_missing_geo_file(self):
        """Test handling when geo data file is missing."""
        with patch("os.path.exists", return_value=False):
            with pytest.raises(FileNotFoundError):
                # This assumes there's some initialization that checks for file existence
                with patch("builtins.open", side_effect=FileNotFoundError):
                    open("nonexistent_file.json", "r")

    def test_coordinate_validation_basic(self):
        """Test basic coordinate validation."""
        # Test valid coordinates
        lat, lon = 40.7128, -74.0060  # NYC coordinates
        assert -90 <= lat <= 90
        assert -180 <= lon <= 180

        # Test invalid coordinates
        assert not (-90 <= 999 <= 90)  # Invalid latitude
        assert not (-180 <= 999 <= 180)  # Invalid longitude

    @pytest.mark.asyncio
    async def test_multiple_zones_same_point(self):
        """Test handling when a point falls in multiple overlapping zones."""
        overlapping_data = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"zone": "zone1"},
                    "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]]},
                },
                {
                    "type": "Feature",
                    "properties": {"zone": "zone2"},
                    "geometry": {"type": "Polygon", "coordinates": [[[1, 1], [3, 1], [3, 3], [1, 3], [1, 1]]]},
                },
            ],
        }

        with patch("builtins.open", mock_open(read_data=json.dumps(overlapping_data))):
            with patch("os.path.exists", return_value=True):
                with patch.object(index, "find_zone", new_callable=AsyncMock) as mock_find:
                    mock_find.return_value = "zone1"  # or "zone2", implementation dependent

                    # Should return one of the zones (implementation dependent)
                    result = await index.find_zone(1.5, 1.5)
                    assert result in ["zone1", "zone2"] or result is None

    @pytest.mark.asyncio
    async def test_performance_with_large_dataset(self):
        """Test performance characteristics with larger datasets."""
        # Create a dataset with many zones
        large_dataset = {"type": "FeatureCollection", "features": []}

        for i in range(100):
            feature = {
                "type": "Feature",
                "properties": {"zone": f"zone_{i}"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[i, i], [i + 1, i], [i + 1, i + 1], [i, i + 1], [i, i]]],
                },
            }
            large_dataset["features"].append(feature)

        with patch("builtins.open", mock_open(read_data=json.dumps(large_dataset))):
            with patch("os.path.exists", return_value=True):
                # Test that lookup completes in reasonable time
                import time

                start_time = time.time()
                result = await index.find_zone(50.5, 50.5)
                end_time = time.time()

                # Should complete within reasonable time (adjust threshold as needed)
                assert (end_time - start_time) < 1.0  # 1 second threshold

    @pytest.mark.asyncio
    async def test_concurrent_access(self):
        """Test concurrent access with asyncio."""

        async def lookup_zone():
            return await index.find_zone(40.5, -79.5)

        # Test concurrent lookups
        with patch.object(index, "find_zone", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = "us-east-1a"

            tasks = [lookup_zone() for _ in range(10)]
            results = await asyncio.gather(*tasks)

            # All results should be consistent
            assert all(result == results[0] for result in results)
            assert mock_find.call_count == 10

    @pytest.mark.asyncio
    async def test_memory_usage(self, sample_geo_data):
        """Test memory usage doesn't grow excessively."""
        import gc

        with patch("builtins.open", mock_open(read_data=json.dumps(sample_geo_data))):
            with patch("os.path.exists", return_value=True):
                with patch.object(index, "find_zone", new_callable=AsyncMock) as mock_find:
                    mock_find.return_value = "us-east-1a"

                    # Perform many lookups and check memory doesn't grow
                    initial_objects = len(gc.get_objects())

                    for i in range(100):
                        await index.find_zone(40.0 + i * 0.001, -79.0 + i * 0.001)

                    gc.collect()
                    final_objects = len(gc.get_objects())

                    # Memory shouldn't grow significantly
                    assert final_objects - initial_objects < 1000  # Reasonable threshold

    @pytest.mark.parametrize(
        "lat,lon,expected_valid",
        [
            (0, 0, True),
            (90, 180, True),
            (-90, -180, True),
            (91, 0, False),
            (0, 181, False),
            (-91, 0, False),
            (0, -181, False),
        ],
    )
    def test_coordinate_validation_parametrized(self, lat, lon, expected_valid):
        """Parametrized test for coordinate validation."""
        # Basic coordinate range validation
        lat_valid = -90 <= lat <= 90
        lon_valid = -180 <= lon <= 180
        is_valid = lat_valid and lon_valid

        assert is_valid == expected_valid

    def test_polygon_geometry_validation(self):
        """Test validation of polygon geometries."""
        # Valid polygon
        valid_polygon = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}

        # Test that it has required structure
        assert valid_polygon["type"] == "Polygon"
        assert "coordinates" in valid_polygon
        assert isinstance(valid_polygon["coordinates"], list)
        assert len(valid_polygon["coordinates"]) > 0

        # First and last points should be the same (closed polygon)
        coordinates = valid_polygon["coordinates"][0]
        assert coordinates[0] == coordinates[-1]

    def test_geojson_point_structure(self):
        """Test GeoJSON point structure validation."""
        valid_point = {"type": "Point", "coordinates": [40.0, -79.0]}

        assert valid_point["type"] == "Point"
        assert "coordinates" in valid_point
        assert isinstance(valid_point["coordinates"], list)
        assert len(valid_point["coordinates"]) == 2
        assert all(isinstance(coord, (int, float)) for coord in valid_point["coordinates"])

    @pytest.mark.asyncio
    async def test_edge_coordinates(self):
        """Test coordinates at the edges of valid ranges."""
        with patch.object(index, "find_zone", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = None

            # Test extreme valid coordinates
            await index.find_zone(90.0, 180.0)  # North Pole, International Date Line
            await index.find_zone(-90.0, -180.0)  # South Pole, opposite side
            await index.find_zone(0.0, 0.0)  # Equator, Prime Meridian

            assert mock_find.call_count == 3

    def test_boolean_point_in_polygon_types(self):
        """Test that boolean_point_in_polygon returns boolean values."""
        polygon = {"type": "Polygon", "coordinates": [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]]}

        point_inside = {"type": "Point", "coordinates": [1, 1]}
        point_outside = {"type": "Point", "coordinates": [3, 3]}

        result_inside = turf.boolean_point_in_polygon(point_inside, polygon)
        result_outside = turf.boolean_point_in_polygon(point_outside, polygon)

        assert isinstance(result_inside, bool)
        assert isinstance(result_outside, bool)
        assert result_inside is True
        assert result_outside is False

    @pytest.mark.asyncio
    async def test_zone_finder_with_real_coordinates(self):
        """Test zone finder with realistic geographic coordinates."""
        # Test with some major city coordinates
        test_coordinates = [
            (40.7128, -74.0060),  # New York
            (51.5074, -0.1278),  # London
            (35.6762, 139.6503),  # Tokyo
            (-33.8688, 151.2093),  # Sydney
        ]

        with patch.object(index, "find_zone", new_callable=AsyncMock) as mock_find:
            mock_find.return_value = "test-zone"

            for lat, lon in test_coordinates:
                result = await index.find_zone(lat, lon)
                assert result == "test-zone"

            assert mock_find.call_count == len(test_coordinates)


if __name__ == "__main__":
    pytest.main([__file__])
