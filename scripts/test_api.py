import requests

url = "https://randomuser.me/api/?results=5"

response = requests.get(url)

print("Status Code:", response.status_code)

data = response.json()

print("Number of users:", len(data["results"]))

for user in data["results"]:
    print(
        user["name"]["first"],
        user["name"]["last"],
        user["email"],
        user["location"]["country"]
    )