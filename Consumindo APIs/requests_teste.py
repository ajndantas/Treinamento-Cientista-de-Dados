import requests

# The API endpoint
url = "https://jsonplaceholder.typicode.com/posts"

# Data to be sent
data = {
    "userID": 1,
    "title": "Making a POST request",
    "body": "This is the data we created."
}

# A POST request to the API
response = requests.post(url, json=data)

# Print the response
print(response.json())