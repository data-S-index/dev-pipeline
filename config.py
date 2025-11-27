"""Configuration for the application."""

from os import environ
from pathlib import Path

from dotenv import dotenv_values

# Check if `.env` file exists
env_path = Path(".") / ".env"

LOCAL_ENV_FILE = env_path.exists()

# Load environment variables from .env
config = dotenv_values(".env")


def get_env(key, optional=False):
    """Return environment variable from .env or native environment."""
    if LOCAL_ENV_FILE:
        return config.get(key)

    if key not in environ and not optional:
        raise ValueError(f"Environment variable {key} not set.")

    return environ.get(key)


DATABASE_URL = get_env("DATABASE_URL", optional=True)
MINI_DATABASE_URL = get_env("MINI_DATABASE_URL", optional=True)
