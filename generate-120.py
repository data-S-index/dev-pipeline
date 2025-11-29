services = "".join(
    [
        f"""  fuji{i}:
    image: ghcr.io/pangaea-data-publisher/fuji:latest
    container_name: fuji{i}
    ports:
      - "{54000 + i}:1071"
    restart: unless-stopped

"""
        for i in range(1, 121)
    ]
)

depends = "".join([f"      - fuji{i}\n" for i in range(1, 121)])

content = f"""version: "3.9"

services:
{services}  fill-database-fuji:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: fill-database-fuji
    environment:
      - MINI_DATABASE_URL=${{MINI_DATABASE_URL}}
      - FUJI_HOST=docker
    depends_on:
{depends}    restart: unless-stopped
"""

with open("docker-compose-120.yml", "w", encoding="utf-8") as f:
    f.write(content)

print("File docker-compose-120.yml created successfully!")
