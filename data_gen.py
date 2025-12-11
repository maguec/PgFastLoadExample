from faker import Faker
import json

fake = Faker()
data = []
for _ in range(1_000_000):
    data.append(
        {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
        }
    )
with open("data.json", "w") as f:
    f.write(json.dumps(data))
