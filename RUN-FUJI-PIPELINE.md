# Running the Fuji Score Pipeline

This guide explains how to run the `fill-database-fuji.py` script to populate your database with Fuji FAIR scores. You can run it either via Docker Compose (recommended) or locally.

## Prerequisites

1. **Docker and Docker Compose** - Required for running FUJI API instances
2. **Database Setup** - Ensure `MINI_DATABASE_URL` is set in your `.env` file
3. **Database Schema** - The database should have a `Dataset` table with the following structure:

   ```sql
   CREATE TABLE "Dataset" (
       id BIGSERIAL PRIMARY KEY,
       doi TEXT,
       score FLOAT DEFAULT NULL,
       evaluationdate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );

   CREATE INDEX idx_dataset_doi ON "Dataset" (doi);
   ```

   The script will automatically create the `fuji_jobs` queue table if it doesn't exist.

## Option 1: Running with Docker Compose (Recommended)

This is the easiest way to run the pipeline. Docker Compose will handle building the container, starting all FUJI services, and running the pipeline script.

### Step 1: Set up environment variables

Create a `.env` file in the project root (if you don't have one already):

```bash
cp .env.example .env
```

Edit `.env` and set your database connection string:

```bash
MINI_DATABASE_URL=postgresql://user:password@host:port/database
```

### Step 2: Start all services and run the pipeline

Run everything in one command:

```bash
docker-compose up fill-database-fuji
```

This will:

- Build the Docker image for the pipeline script (first time only)
- Start all 30 FUJI API instances (fuji1 through fuji30)
- Wait for FUJI services to be ready
- Run the pipeline script
- Process all datasets without scores

### Step 3: View logs

To see the progress in real-time:

```bash
docker-compose logs -f fill-database-fuji
```

### Alternative: Start services separately

If you want more control, you can start services separately:

```bash
# Start all FUJI services in the background
docker-compose up -d fuji1 fuji2 fuji3 fuji4 fuji5 fuji6 fuji7 fuji8 fuji9 fuji10 \
  fuji11 fuji12 fuji13 fuji14 fuji15 fuji16 fuji17 fuji18 fuji19 fuji20 \
  fuji21 fuji22 fuji23 fuji24 fuji25 fuji26 fuji27 fuji28 fuji29 fuji30

# Wait a few seconds for services to be ready
sleep 10

# Run the pipeline
docker-compose up fill-database-fuji
```

### Stopping services

When finished, stop all containers:

```bash
docker-compose down
```

Or stop only the FUJI services (if the pipeline container has already exited):

```bash
docker-compose stop fuji1 fuji2 fuji3 ... fuji30
```

## Option 2: Running Locally (Development)

For local development or testing, you can run the script directly with Python.

### Step 1: Install dependencies

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Set up environment variables

Ensure your `.env` file has `MINI_DATABASE_URL` set.

### Step 3: Start FUJI services

Start the FUJI API containers:

```bash
docker-compose up -d
```

This starts 30 FUJI instances on ports 54001-54030.

### Step 4: Verify FUJI services are running

```bash
docker ps --filter "name=fuji" --format "table {{.Names}}\t{{.Status}}"
```

You should see all 30 containers (fuji1 through fuji30) running.

### Step 5: Run the script

```bash
python fill-database-fuji.py
```

The script will automatically:

- Connect to localhost:54001 through localhost:54030
- Query the database for datasets without scores
- Process them in batches
- Update the database with scores

### Step 6: Stop FUJI services

When finished:

```bash
docker-compose down
```

## What the Script Does

1. **Connects to Database**: Uses `MINI_DATABASE_URL` from your `.env` file
2. **Creates Job Queue**: Automatically creates `fuji_jobs` table if needed
3. **Pre-fills Queue**: Adds all datasets with `score IS NULL` to the processing queue
4. **Processes in Batches**:
   - Claims batches of jobs from the queue (90 jobs per batch = 30 instances × 3 concurrent)
   - Distributes work across all 30 FUJI API endpoints
   - Processes requests concurrently using asyncio
5. **Updates Database**: Stores scores and evaluation dates back to the `Dataset` table
6. **Retries**: Automatically retries failed requests up to 3 times with exponential backoff
7. **Progress Tracking**: Shows real-time progress, success/failure counts, and processing rate

## Configuration

### Environment Variables

- `MINI_DATABASE_URL` (required): PostgreSQL connection string
- `FUJI_HOST` (optional):
  - Default: `localhost` (for local development)
  - Set to `docker` when running in Docker Compose (uses service names)
- `FUJI_PORT` (optional): If set, uses a single endpoint instead of multiple

### Script Settings (in `fill-database-fuji.py`)

- **FUJI Instances**: 30 instances (fuji1 through fuji30)
- **Batch Size**: 90 datasets per batch (30 instances × 3 concurrent requests)
- **Max Retries**: 3 attempts per API call
- **Retry Delay**: 2 seconds (with exponential backoff)
- **API Credentials**: Username "marvel", Password "wonderwoman"
- **API Timeout**: 60 seconds per request

## Troubleshooting

### Database Connection Issues

**Error**: `MINI_DATABASE_URL not set in environment or .env file`

**Solution**: Ensure your `.env` file exists and contains `MINI_DATABASE_URL=...`

**Error**: `Database connection error`

**Solution**:

- Verify your database is accessible
- Check the connection string format: `postgresql://user:password@host:port/database`
- If running in Docker, ensure the database host is accessible from the container (may need `host.docker.internal` on Docker Desktop)

### FUJI API Connection Issues

**Error**: `API error: Connection refused` or `HTTP error: 502`

**Solution**:

- Verify FUJI containers are running: `docker ps --filter "name=fuji"`
- Wait a bit longer for containers to fully start (they may need 30-60 seconds)
- Check container logs: `docker-compose logs fuji1`
- If running locally, ensure you're using `localhost` (not `docker` for `FUJI_HOST`)
- If running in Docker Compose, ensure `FUJI_HOST=docker` is set

### Performance Issues

**Slow processing**:

- The script processes 90 datasets concurrently across 30 FUJI instances
- If FUJI instances are slow, consider reducing `INSTANCE_COUNT` or `BATCH_SIZE` in the script
- Check database connection pool settings if you see connection errors

**Out of memory**:

- Reduce `BATCH_SIZE` in the script
- Reduce `CONNECTION_POOL_MAX` if you see database connection issues

### Container Issues

**Error**: `Cannot connect to the Docker daemon`

**Solution**: Ensure Docker is running on your system

**Error**: `Service 'fill-database-fuji' failed to build`

**Solution**:

- Check that `Dockerfile` exists in the project root
- Verify `requirements.txt` is valid
- Try rebuilding: `docker-compose build fill-database-fuji`

## Monitoring Progress

The script provides detailed progress information:

```
🚀 Starting Fuji score database fill process...
📡 Using 30 FUJI API endpoints
🔌 Creating database connection pool...
  ✅ Connection pool created (min=2, max=10)
  ✅ Database connection test successful
  ✅ fuji_jobs table ready
  📊 Found 1,234,567 jobs already in queue
  📊 Progress: 1,000 processed, 1,233,567 remaining (0.1%) - Succeeded: 995, Failed: 5 - Rate: 12.34/s
```

## Stopping the Pipeline

To stop the pipeline:

- **If running in Docker Compose**: Press `Ctrl+C` in the terminal, or run `docker-compose stop fill-database-fuji`
- **If running locally**: Press `Ctrl+C` in the terminal

The script will gracefully stop after finishing the current batch. Jobs that were claimed but not processed will remain in the queue and can be processed in the next run.

## Running Multiple Instances

The pipeline is designed to be run across multiple machines/containers simultaneously:

- Each instance claims jobs from the queue atomically
- No job will be processed twice
- You can scale horizontally by running multiple containers

To run multiple instances in Docker Compose, you can scale the service:

```bash
docker-compose up --scale fill-database-fuji=3 fill-database-fuji
```

Note: You may want to adjust `CONNECTION_POOL_MAX` if running many instances to avoid exhausting database connections.
