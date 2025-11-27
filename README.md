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

### Prerequisites

1. **Docker and Docker Compose** - Required to run the FUJI API instances
2. **Database Setup** - Ensure `MINI_DATABASE_URL` is set in your `.env` file
3. **Database Schema** - The database should have a `Dataset` table with the following structure:

   ```sql
   CREATE TABLE "Dataset" (
       id BIGSERIAL PRIMARY KEY,
       doi TEXT,
       score FLOAT DEFAULT NULL,
       evaluationDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );
   ```

### Steps

1. **Start the FUJI API Docker containers:**

   ```bash
   docker-compose up -d
   ```

   This starts 10 FUJI API instances on ports 54001-54010.

2. **Verify containers are running:**

   ```bash
   docker ps
   ```

   All 10 containers (fuji1 through fuji10) should be running and you should see the container names in the output.

3. **Run the scoring script:**

   ```bash
   python fill-database-fuji.py
   ```

### What the Script Does

- Queries the database for datasets where `score IS NULL`
- Processes datasets in batches of 100
- Calls the FUJI API in parallel using all 10 endpoints
- Updates the database with scores and evaluation dates
- Retries failed API calls up to 3 times with exponential backoff
- Shows progress with a progress bar and summary statistics

### Configuration

The script uses the following default settings (can be modified in the script):

- **API Endpoints**: 10 FUJI instances on ports 54001-54010
- **Batch Size**: 1000 datasets per batch
- **Max Retries**: 3 attempts per API call
- **API Credentials**: Username "marvel", Password "wonderwoman"
- **API Parameters**: Configured for PANGAEA OAI-PMH metadata service

### Stopping the Containers

When finished, stop the Docker containers:

```bash
docker-compose down
```
