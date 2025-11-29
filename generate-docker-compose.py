import argparse

parser = argparse.ArgumentParser(
    description="Generate docker-compose file with specified number of fuji services"
)
parser.add_argument("number", type=int, help="Number of fuji services to generate")
args = parser.parse_args()

number = args.number

services = "".join(
    [
        f"""  fuji{i}:
    image: ghcr.io/pangaea-data-publisher/fuji:latest
    container_name: fuji{i}
    ports:
      - "{54000 + i}:1071"
    restart: unless-stopped

"""
        for i in range(1, number + 1)
    ]
)

depends = "".join([f"      - fuji{i}\n" for i in range(1, number + 1)])

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

output_filename = f"docker-compose-{number}.yml"
with open(output_filename, "w", encoding="utf-8") as f:
    f.write(content)

print(f"File {output_filename} created successfully!")
