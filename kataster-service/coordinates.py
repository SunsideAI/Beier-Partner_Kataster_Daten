"""
Koordinatentransformation zwischen WGS84 (EPSG:4326) und ETRS89/UTM32 (EPSG:25832).
Die meisten deutschen WFS-Dienste arbeiten in EPSG:25832.
"""

from pyproj import Transformer

# Transformer: WGS84 → ETRS89/UTM Zone 32N
_to_utm32 = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)
_to_wgs84 = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)


def wgs84_to_utm32(lon: float, lat: float) -> tuple[float, float]:
    """
    Transformiert WGS84-Koordinaten (lon, lat) nach ETRS89/UTM32 (easting, northing).
    
    Args:
        lon: Längengrad (WGS84)
        lat: Breitengrad (WGS84)
    
    Returns:
        Tuple (easting, northing) in EPSG:25832
    """
    easting, northing = _to_utm32.transform(lon, lat)
    return easting, northing


def utm32_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
    """
    Transformiert ETRS89/UTM32 (easting, northing) nach WGS84 (lon, lat).
    
    Returns:
        Tuple (lon, lat) in WGS84
    """
    lon, lat = _to_wgs84.transform(easting, northing)
    return lon, lat


def make_bbox_utm32(lon: float, lat: float, buffer_m: float = 10.0) -> tuple[float, float, float, float]:
    """
    Erzeugt eine Bounding-Box in EPSG:25832 um einen WGS84-Punkt.
    
    Args:
        lon: Längengrad (WGS84)
        lat: Breitengrad (WGS84)
        buffer_m: Puffer in Metern (Standard: 10m)
    
    Returns:
        Tuple (min_easting, min_northing, max_easting, max_northing) in EPSG:25832
    """
    easting, northing = wgs84_to_utm32(lon, lat)
    return (
        easting - buffer_m,
        northing - buffer_m,
        easting + buffer_m,
        northing + buffer_m,
    )
