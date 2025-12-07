# Flink Cluster Setup Guide

This guide explains how to run and manage your Flink cluster with the Kafka stack.

## Architecture

Your Docker Compose setup includes:

- **Kafka** (KRaft mode) - Message broker
- **Schema Registry** - Avro schema management
- **Kafka UI** - Web interface for Kafka (http://localhost:8080)
- **Flink JobManager** - Cluster coordinator (Web UI: http://localhost:8082)
- **Flink TaskManager** - Worker node(s) for data processing

## Starting the Cluster

### Start Everything
```bash
cd docker
docker-compose up -d
```

### Start Only Flink (if Kafka is already running)
```bash
cd docker
docker-compose up -d jobmanager taskmanager
```

### Check Status
```bash
docker-compose ps
```

All services should show `healthy` status.

## Accessing Web Interfaces

- **Flink Dashboard**: http://localhost:8082
  - View running jobs, task managers, job metrics, logs
  
- **Kafka UI**: http://localhost:8080
  - Browse topics, messages, consumer groups, schemas

- **Schema Registry**: http://localhost:8081
  - REST API for schema operations

## Submitting Your Fraud Detection Job

### Option 1: Submit via Flink CLI (from inside container)

```bash
# Copy your JAR to the jobs directory
cp ../target/bank-fraud-detection-1.0-SNAPSHOT-shaded.jar ./jobs/

# Submit the job
docker exec -it flink-jobmanager flink run /opt/flink/jobs/bank-fraud-detection-1.0-SNAPSHOT-shaded.jar
```

### Option 2: Submit via Web UI

1. Open http://localhost:8082
2. Click "Submit New Job"
3. Click "Add New" and upload `bank-fraud-detection-1.0-SNAPSHOT-shaded.jar`
4. Click "Submit"

### Option 3: Submit from Host (if Flink CLI installed locally)

```bash
flink run -m localhost:8082 target/bank-fraud-detection-1.0-SNAPSHOT-shaded.jar
```

## Managing Jobs

### List Running Jobs
```bash
docker exec -it flink-jobmanager flink list -r
```

### Cancel a Job
```bash
docker exec -it flink-jobmanager flink cancel <job-id>
```

### Stop with Savepoint (for stateful restart)
```bash
docker exec -it flink-jobmanager flink stop <job-id>
```

## Scaling TaskManagers

To add more TaskManagers for increased parallelism:

```bash
docker-compose up -d --scale taskmanager=3
```

This creates 3 TaskManager instances. Each TaskManager has 4 task slots (configurable), so 3 TaskManagers = 12 total slots.

## Viewing Logs

### JobManager Logs
```bash
docker logs flink-jobmanager -f
```

### TaskManager Logs
```bash
docker logs flink-taskmanager -f
```

### All Services
```bash
docker-compose logs -f
```

## Troubleshooting

### Job Won't Start
1. Check if Kafka and Schema Registry are healthy: `docker-compose ps`
2. Verify topics exist: Check Kafka UI at http://localhost:8080
3. Verify schemas are registered: `curl http://localhost:8081/subjects`

### No TaskManager Connected
- Check TaskManager logs: `docker logs flink-taskmanager`
- Verify network connectivity: `docker network inspect flink-network`

### Checkpoint Failures
- Check checkpoint directory permissions: `/tmp/flink-checkpoints`
- Review JobManager logs for checkpoint errors

## Stopping the Cluster

### Stop Everything
```bash
docker-compose down
```

### Stop But Keep Data (volumes)
```bash
docker-compose stop
```

### Stop and Remove Volumes (CAREFUL: deletes all data)
```bash
docker-compose down -v
```

## Configuration Notes

### Ports
- **8080**: Kafka UI
- **8081**: Schema Registry  
- **8082**: Flink Web UI (not 8081 to avoid conflict with Schema Registry)
- **9092**: Kafka (host access)
- **6123**: Flink JobManager RPC

### Java Version
Both JobManager and TaskManager use **Java 21** via the `flink:1.18.1-java21` image.

### Checkpoint Storage
Checkpoints are stored in Docker volume `flink-checkpoints`, mounted at `/tmp/flink-checkpoints` in containers.

## Next Steps

1. Start the cluster: `docker-compose up -d`
2. Build your job: `mvn clean package` (from project root)
3. Copy JAR to jobs directory: `cp target/bank-fraud-detection-1.0-SNAPSHOT-shaded.jar docker/jobs/`
4. Submit via Web UI: http://localhost:8082
5. Monitor in Flink Dashboard and Kafka UI
