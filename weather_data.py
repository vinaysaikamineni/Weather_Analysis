import requests
import json

# Your API Key and endpoint
API_KEY = "f640156891b8f1e737b5973e68a22c80"
BASE_URL = "https://api.openweathermap.org/data/3.0/onecall"

# Location coordinates for the weather data (you can adjust these)
latitude = "39.0997"  # Example: Kansas City, Missouri
longitude = "-94.5786"

# Define parameters
params = {
    "lat": latitude,
    "lon": longitude,
    "exclude": "minutely,hourly,alerts",  # Exclude unnecessary parts if not needed
    "appid": API_KEY,
    "units": "metric"
}

# Make the API request
response = requests.get(BASE_URL, params=params)
weather_data = response.json()

# Print the data (for debugging purposes)
print(json.dumps(weather_data, indent=2))

# Save data as a JSON file
with open("weather_data.json", "w") as f:
    json.dump(weather_data, f)