#!/usr/bin/env python3
"""
Helper script to run multiple docker-compose instances with different project names and port ranges.

Usage:
    python run-compose-instance.py <project_name> <port_base> [command]

Examples:
    # Start instance1 with ports starting at 54001
    python run-compose-instance.py instance1 54001 up -d

    # Start instance2 with ports starting at 55001
    python run-compose-instance.py instance2 55001 up -d

    # Stop instance1
    python run-compose-instance.py instance1 54001 down

    # View logs for instance1
    python run-compose-instance.py instance1 54001 logs -f
"""

import argparse
import subprocess
import sys
from pathlib import Path


def generate_compose_file(
    project_name: str, port_base: int, num_instances: int = 30
) -> str:
    """Generate docker-compose.yml content with configurable port base and project name."""
    compose_content = 'version: "3.9"\n\nservices:\n'

    # Generate fuji services
    for i in range(1, num_instances + 1):
        port = port_base + i - 1
        container_name = f"{project_name}-fuji{i}"
        compose_content += f"""  fuji{i}:
    image: ghcr.io/pangaea-data-publisher/fuji:latest
    container_name: {container_name}
    ports:
      - "{port}:1071"
    restart: unless-stopped

"""

    # Add fill-database-fuji service
    fill_container_name = f"{project_name}-fill-database-fuji"
    compose_content += f"""  fill-database-fuji:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: {fill_container_name}
    environment:
      - MINI_DATABASE_URL=${{MINI_DATABASE_URL}}
      - FUJI_HOST=docker
    depends_on:
"""

    # Add dependencies
    for i in range(1, num_instances + 1):
        compose_content += f"      - fuji{i}\n"

    compose_content += "    restart: unless-stopped\n"

    return compose_content


def main():
    parser = argparse.ArgumentParser(
        description="Run docker-compose with different project names and port ranges",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "project_name", help="Project name (e.g., instance1, instance2)"
    )
    parser.add_argument(
        "port_base", type=int, help="Base port number (e.g., 54001, 55001)"
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Docker compose command (e.g., up -d, down, logs)",
    )

    args = parser.parse_args()

    if not args.command:
        parser.error(
            "Docker compose command is required (e.g., 'up -d', 'down', 'logs')"
        )

    # Generate compose file content
    compose_content = generate_compose_file(args.project_name, args.port_base)

    # Create temporary compose file
    project_dir = Path(__file__).parent
    temp_compose = project_dir / f"docker-compose.{args.project_name}.yml"

    try:
        # Write temporary compose file
        temp_compose.write_text(compose_content)

        # Build docker-compose command
        cmd = [
            "docker-compose",
            "-p",
            args.project_name,
            "-f",
            str(temp_compose),
        ] + args.command

        print(f"Running: {' '.join(cmd)}")
        print(f"Project: {args.project_name}")
        print(f"Port range: {args.port_base} - {args.port_base + 29}")
        print()

        # Run docker-compose
        result = subprocess.run(cmd, cwd=project_dir)
        sys.exit(result.returncode)

    finally:
        # Clean up temporary file (optional - you might want to keep it for debugging)
        # Uncomment the next line if you want to delete the temp file after running
        # temp_compose.unlink(missing_ok=True)
        pass


if __name__ == "__main__":
    main()
