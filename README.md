# Pipeline

## Getting started

### Prerequisites/Dependencies

You will need the following installed on your system:

- Python 3.8+
- [Pip](https://pip.pypa.io/en/stable/)

### Setup

If you would like to update the api, please follow the instructions below.

1. Create a local virtual environment and activate it:

   ```bash
   python -m venv .venv #or py -m venv .venv
   source .venv/bin/activate # linux
   .venv\Scripts\activate # windows
   ```

2. Install the dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Add your environment variables. An example is provided at `.env.example`

   ```bash
   cp .env.example .env
   ```

   Make sure to update the values in `.env` to match your local setup.

4. Format the code:

   ```bash
   poe format_with_isort
   poe format_with_black
   ```

   You can also run `poe format` to run both commands at once.

5. Check the code quality:

   ```bash
   poe typecheck
   poe pylint
   poe flake8
   ```

   You can also run `poe lint` to run all three commands at once.

6. To start the local server, run:

   ```bash
   poe init # pick python
   poe dev
   ```

   This runs `func start` with the `--python` flag.

## Running Fuji Score Pipeline

The `fill-database-fuji.py` script queries the database for datasets without Fuji scores and fills them by calling the FUJI API.

**📖 For detailed instructions, see [RUN-FUJI-PIPELINE.md](RUN-FUJI-PIPELINE.md)**

### Quick Start

**Option 1: Docker Compose (Recommended)**

```bash
# Set MINI_DATABASE_URL in .env file, then:
docker-compose up fill-database-fuji
```

**Option 2: Local Development**

```bash
# Start FUJI services
docker-compose up -d

# Run the script
python fill-database-fuji.py
```

### What the Script Does

- Queries the database for datasets where `score IS NULL`
- Processes datasets in batches using 30 FUJI API instances
- Calls the FUJI API in parallel across all endpoints
- Updates the database with scores and evaluation dates
- Retries failed API calls up to 3 times with exponential backoff
- Shows real-time progress with detailed statistics

See [RUN-FUJI-PIPELINE.md](RUN-FUJI-PIPELINE.md) for complete documentation, troubleshooting, and configuration options.
