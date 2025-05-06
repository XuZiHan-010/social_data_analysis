import json

# Load the provided JSON file
with open("china_province.json", "r", encoding="utf-8") as file:
    geo_data = json.load(file)

# Extract the range of coordinates for each province (bounding box)
province_ranges = {}

for feature in geo_data['features']:
    province_name = feature['properties']['NAME_1']
    coordinates = feature['geometry']['coordinates']

    # Flatten the coordinates to find min/max values
    all_coords = []
    if feature['geometry']['type'] == 'Polygon':
        all_coords = coordinates[0]
    elif feature['geometry']['type'] == 'MultiPolygon':
        for polygon in coordinates:
            all_coords.extend(polygon[0])

    # Extract latitudes and longitudes
    longitudes = [coord[0] for coord in all_coords]
    latitudes = [coord[1] for coord in all_coords]

    # Compute the bounding box (min/max latitudes and longitudes)
    min_longitude, max_longitude = min(longitudes), max(longitudes)
    min_latitude, max_latitude = min(latitudes), max(latitudes)

    province_ranges[province_name] = {
        'min_longitude': min_longitude,
        'max_longitude': max_longitude,
        'min_latitude': min_latitude,
        'max_latitude': max_latitude
    }

print(province_ranges)
