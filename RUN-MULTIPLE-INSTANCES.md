# Running Multiple Docker Compose Instances

This guide explains how to run multiple instances of the docker-compose setup with different project names and port ranges.

## Quick Start

Use the `run-compose-instance.py` script to run multiple instances:

```bash
# Start first instance (ports 54001-54030)
python run-compose-instance.py instance1 54001 up -d

# Start second instance (ports 55001-55030)
python run-compose-instance.py instance2 55001 up -d

# Start third instance (ports 56001-56030)
python run-compose-instance.py instance3 56001 up -d
```

## Usage

```bash
python run-compose-instance.py <project_name> <port_base> <docker_compose_command>
```

### Parameters

- **project_name**: Unique identifier for this instance (e.g., `instance1`, `instance2`, `worker1`)
- **port_base**: Starting port number. Each instance uses 30 consecutive ports (e.g., `54001` uses ports 54001-54030)
- **docker_compose_command**: Any docker-compose command (e.g., `up -d`, `down`, `logs -f`, `ps`)

### Examples

#### Starting Instances

```bash
# Start instance1 in detached mode
python run-compose-instance.py instance1 54001 up -d

# Start instance2 in detached mode
python run-compose-instance.py instance2 55001 up -d
```

#### Viewing Logs

```bash
# View logs for instance1
python run-compose-instance.py instance1 54001 logs -f

# View logs for instance2's fill-database-fuji service
python run-compose-instance.py instance2 55001 logs -f fill-database-fuji
```

#### Stopping Instances

```bash
# Stop instance1
python run-compose-instance.py instance1 54001 down

# Stop instance2
python run-compose-instance.py instance2 55001 down
```

#### Checking Status

```bash
# List containers for instance1
python run-compose-instance.py instance1 54001 ps

# List containers for instance2
python run-compose-instance.py instance2 55001 ps
```

## How It Works

1. The script generates a temporary `docker-compose.{project_name}.yml` file with:

   - Port mappings starting from your specified `port_base`
   - Container names prefixed with the project name (e.g., `instance1-fuji1`, `instance2-fuji1`)
   - All 30 fuji services configured

2. Runs docker-compose with:

   - Project name (`-p`) to isolate resources
   - The generated compose file (`-f`)
   - Your specified command

3. Each instance runs in its own isolated network, so services can use the same internal names (`fuji1`, `fuji2`, etc.) without conflicts.

## Port Planning

Make sure port ranges don't overlap:

- Instance 1: 54001-54030 (30 ports)
- Instance 2: 55001-55030 (30 ports)
- Instance 3: 56001-56030 (30 ports)
- etc.

Each instance needs 30 consecutive ports.

## Managing Multiple Instances

### List All Running Instances

```bash
# Using docker directly
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "(instance|fuji)"
```

### Stop All Instances

```bash
# Stop each instance individually
python run-compose-instance.py instance1 54001 down
python run-compose-instance.py instance2 55001 down
python run-compose-instance.py instance3 56001 down
```

### View Logs from All Instances

```bash
# Terminal 1
python run-compose-instance.py instance1 54001 logs -f

# Terminal 2
python run-compose-instance.py instance2 55001 logs -f

# Terminal 3
python run-compose-instance.py instance3 56001 logs -f
```

## Notes

- Each instance generates its own `docker-compose.{project_name}.yml` file in the project directory
- These files are kept for reference but can be safely deleted if needed
- Each instance is completely isolated - they don't share networks, volumes, or containers
- The `fill-database-fuji` service in each instance connects to its own set of 30 fuji services
- Make sure your `.env` file has `MINI_DATABASE_URL` set correctly

## Troubleshooting

### Port Already in Use

If you get a port conflict error, choose a different `port_base`:

```bash
# Check what ports are in use
netstat -an | findstr "54001 55001 56001"  # Windows
# or
lsof -i :54001  # Linux/Mac
```

### Container Name Conflicts

If you see container name conflicts, make sure you're using unique project names for each instance.

### Service Not Starting

Check the logs:

```bash
python run-compose-instance.py instance1 54001 logs
```
