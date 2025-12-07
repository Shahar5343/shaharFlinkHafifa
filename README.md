# Bank Fraud Detection - Apache Flink Streaming Application

A production-grade Apache Flink streaming application demonstrating real-time fraud detection with Kafka, Avro, and Schema Registry integration.

## 🎯 What This Project Teaches

This is **not** a quick tutorial. It's a comprehensive learning resource that shows you how to build production-quality streaming applications. Every component includes detailed explanations of:

- **What** it does
- **Why** it's needed  
- **How** it integrates with the system

### Key Learning Outcomes

1. **Enterprise Schema Management**: Work with centralized Schema Registry (not local schemas)
2. **Event-Time Processing**: Handle out-of-order events with watermarks
3. **Windowed Aggregation**: Detect patterns across time windows
4. **Exactly-Once Semantics**: Guarantee no duplicates or data loss
5. **Fault Tolerance**: Checkpointing and state management
6. **Polymorphic Events**: Single topic with multiple event types (Avro unions)

## 📋 Prerequisites

- **Java 21** (JDK 21+)
- **Maven 3.8+**
- **Docker Desktop** (for Kafka infrastructure)
- **curl** or **Postman** (for testing)
- **Git** (for version control)

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Kafka Topic: bank-events                                       │
│  ┌──────────────┐  ┌──────────────┐                            │
│  │ Transaction  │  │ FraudReport  │  (Polymorphic BankEvent)   │
│  └──────────────┘  └──────────────┘                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  Flink Streaming Job │
              └──────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
  Deserialize      Event Router    Assign Timestamps
  (Schema Reg)     (Side Outputs)   (Watermarks)
                         │
        ┌────────────────┴────────────────┐
        │                                 │
        ▼                                 ▼
  Transaction Stream              FraudReport Stream
        │                                 │
  Key by account_id                       │
        │                                 │
  Window (5-min tumbling)                 │
        │                                 │
  Fraud Detection                         │
  - High frequency (>10 tx)               │
  - Large amount (>$10k)                  │
        │                                 │
        └────────────────┬────────────────┘
                         │
                         ▼
                   Generate Alerts
                         │
                         ▼
              Serialize (Schema Registry)
                         │
                         ▼
              ┌──────────────────────┐
              │ Kafka Topic: alerts  │
              └──────────────────────┘
```

## 🚀 Quick Start

### Step 1: Start Infrastructure

```bash
cd docker
docker-compose up -d
```

**What this does**:
- Starts Kafka (KRaft mode - no Zookeeper)
- Starts Schema Registry
- Starts Kafka UI (http://localhost:8080)

**Verify**:
```bash
docker-compose ps
# All services should show "healthy"
```

### Step 2: Initialize Schemas

```bash
# Windows
cd docker
init-schemas.bat

# Linux/Mac
cd docker
chmod +x init-schemas.sh
./init-schemas.sh
```

**What this does**:
- Uploads Avro schemas to local Schema Registry
- Simulates what your central schema team does in production

**Verify**:
```bash
curl http://localhost:8081/subjects
```

You should see:
```json
[
  "com.shahar.bank.Transaction",
  "com.shahar.bank.FraudReport",
  "com.shahar.bank.BankEvent",
  "com.shahar.flink.Alert"
]
```

### Step 3: Build the Project

```bash
cd ..
mvn clean compile
```

**What this does**:
1. Downloads schemas from Schema Registry
2. Generates Java classes from schemas
3. Compiles your Flink application

**First build will take 2-3 minutes** (downloads dependencies)

**Verify**:
```bash
# Check generated classes
ls target/generated-sources/avro/com/shahar/bank/
# Should see: Transaction.java, FraudReport.java, BankEvent.java

ls target/generated-sources/avro/com/shahar/flink/
# Should see: Alert.java
```

### Step 4: Package the Application

```bash
mvn clean package
```

**What this does**:
- Runs tests
- Creates fat JAR with all dependencies
- Output: `target/bank-fraud-detection-1.0-SNAPSHOT.jar`

### Step 5: Generate Test Data

Open a **new terminal** and run:

```bash
mvn exec:java -Dexec.mainClass="com.shahar.flink.TestDataGenerator"
```

**What this does**:
- Generates realistic transactions and fraud reports
- Sends to `bank-events` topic
- Runs continuously (Ctrl+C to stop)

**You should see**:
```
Generated 5 events (total: 5)
Generated Transaction: Account=xxx, Amount=$523.45
Generated FraudReport: Account=yyy, Risk=HIGH
```

### Step 6: Run the Flink Job

Open **another terminal** and run:

```bash
mvn exec:java -Dexec.mainClass="com.shahar.flink.StreamingJob"
```

**What this does**:
- Starts Flink streaming job
- Reads from `bank-events` topic
- Detects fraud patterns
- Writes alerts to `alerts` topic

**You should see**:
```
INFO  StreamingJob - Starting Bank Fraud Detection Streaming Job
INFO  FraudDetector - FRAUD ALERT: Account xxx - Urgency 3 - HIGH: Large total amount ($12,345.67)
```

### Step 7: View Results

**Option 1: Kafka UI** (Recommended)
1. Open http://localhost:8080
2. Navigate to Topics → `alerts`
3. Click "Messages" tab
4. See fraud alerts in real-time

**Option 2: Console Consumer**
```bash
docker exec -it kafka bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic alerts --from-beginning
```

## 📚 Understanding the Code

### Configuration (`FlinkConfig.java`)

**Purpose**: Centralized configuration management

**Why external config**: Deploy same JAR to dev/staging/prod with different settings

**Key configs**:
- Kafka connection strings
- Schema Registry URL
- Window size (5 minutes)
- Fraud thresholds (10 transactions, $10,000)

### Serialization (`AvroDeserializationSchema.java`, `AvroSerializationSchema.java`)

**Purpose**: Convert between Kafka bytes and Java objects using Schema Registry

**How it works**:
1. Message includes schema ID in header (first 5 bytes)
2. Deserializer fetches schema from registry (cached)
3. Deserializes bytes using that schema

**Why Schema Registry**: Enables schema evolution without breaking consumers

### Event Router (`EventRouter.java`)

**Purpose**: Split polymorphic `BankEvent` into type-specific streams

**Pattern**: Flink side outputs (type-safe stream splitting)

**Why**: Different processing logic for Transactions vs FraudReports

### Fraud Detector (`FraudDetector.java`)

**Purpose**: Windowed aggregation and fraud detection

**Window**: 5-minute tumbling windows per account

**Rules**:
- **Medium urgency**: > 10 transactions in window
- **High urgency**: Total amount > $10,000
- **Critical urgency**: Both conditions

**Learning point**: ProcessWindowFunction gives access to all events + window metadata

### Main Job (`StreamingJob.java`)

**Purpose**: Orchestrates entire pipeline

**Key features**:
- **Checkpointing**: Every 60 seconds (exactly-once semantics)
- **State backend**: HashMapStateBackend (in-memory)
- **Restart strategy**: 3 attempts with 10s delay
- **Watermarks**: 10-second max out-of-orderness

## 🔧 Configuration

Edit `src/main/resources/application.properties`:

```properties
# Kafka
kafka.bootstrap.servers=localhost:9092
kafka.input.topic=bank-events
kafka.output.topic=alerts

# Schema Registry
schema.registry.url=http://localhost:8081

# Windowing
window.size.minutes=5
watermark.max.out.of.orderness.seconds=10

# Fraud Detection
fraud.threshold.transaction.count=10
fraud.threshold.total.amount=10000.0
```

## 🧪 Testing Scenarios

### Scenario 1: Normal Activity (No Alert)

Generate 5 transactions of $100 each for one account:
- **Expected**: No alert (below thresholds)

### Scenario 2: High Frequency (Medium Urgency)

Generate 15 transactions of $100 each in 2 minutes:
- **Expected**: Medium urgency alert (> 10 transactions)

### Scenario 3: Large Amount (High Urgency)

Generate 3 transactions totaling $12,000:
- **Expected**: High urgency alert (> $10,000)

### Scenario 4: Critical (Both Conditions)

Generate 15 transactions totaling $15,000:
- **Expected**: Critical urgency alert

## 📖 Learning Path

### Level 1: Understand the Flow

1. Read `StreamingJob.java` top to bottom
2. Trace one event through the pipeline
3. Understand checkpointing and watermarks

### Level 2: Modify the Logic

1. Change window size to 1 minute
2. Add a new fraud rule (e.g., suspicious transaction types)
3. Implement dead-letter queue for failed events

### Level 3: Add Features

1. Correlate FraudReports with Transactions (join streams)
2. Add state to track historical patterns per account
3. Implement session windows for burst detection

### Level 4: Production Hardening

1. Add metrics (Prometheus)
2. Implement backpressure handling
3. Add integration tests with Flink test harness
4. Deploy to Flink cluster (not local mode)

## 🐛 Troubleshooting

### Build fails: "Schema not found"

**Cause**: Schema Registry not running or schemas not uploaded

**Solution**:
```bash
cd docker
docker-compose ps  # Verify schema-registry is healthy
init-schemas.bat   # Re-upload schemas
```

### Flink job fails: "Connection refused"

**Cause**: Kafka not running

**Solution**:
```bash
cd docker
docker-compose up -d
docker-compose ps  # Verify kafka is healthy
```

### No alerts generated

**Cause**: Thresholds too high or not enough test data

**Solution**:
1. Lower thresholds in `application.properties`
2. Check test data generator is running
3. Verify events in Kafka UI (http://localhost:8080)

### "ClassNotFoundException" at runtime

**Cause**: Missing dependency in fat JAR

**Solution**:
```bash
mvn clean package  # Rebuild fat JAR
# Check JAR contents
jar tf target/bank-fraud-detection-1.0-SNAPSHOT.jar | grep <ClassName>
```

## 🌟 Production Differences

| Aspect | This Project (Local) | Production |
|--------|---------------------|------------|
| **Kafka** | Single broker | 3+ brokers with replication |
| **Schema Registry** | Local Docker | Managed service (Confluent Cloud) |
| **Schemas** | Local files uploaded | Central schema repository + CI/CD |
| **State Backend** | HashMapStateBackend | RocksDBStateBackend (large state) |
| **Checkpoints** | Local filesystem | S3/HDFS/GCS |
| **Monitoring** | Logs | Prometheus + Grafana |
| **Deployment** | Local JVM | Flink cluster (Kubernetes/YARN) |

## 📦 Project Structure

```
shaharFlinkHafifa/
├── docker/                      # Infrastructure
│   ├── docker-compose.yml       # Kafka + Schema Registry + UI
│   ├── init-schemas.bat         # Schema upload (Windows)
│   └── init-schemas.sh          # Schema upload (Linux/Mac)
├── schemas-local/               # Local schema files (for bootstrapping)
│   ├── transaction.avsc
│   ├── fraud-report.avsc
│   ├── bank-event.avsc
│   └── alert.avsc
├── src/main/java/com/shahar/flink/
│   ├── StreamingJob.java        # Main application
│   ├── config/
│   │   └── FlinkConfig.java     # Configuration management
│   ├── serialization/
│   │   ├── AvroDeserializationSchema.java
│   │   └── AvroSerializationSchema.java
│   └── functions/
│       ├── EventRouter.java     # Event type routing
│       └── FraudDetector.java   # Fraud detection logic
├── src/main/resources/
│   ├── application.properties   # Configuration
│   └── log4j2.properties        # Logging
└── src/test/java/com/shahar/flink/
    └── TestDataGenerator.java  # Test data producer
```

## 🤝 Contributing

This is a learning project. Feel free to:
- Add new fraud detection rules
- Implement additional features
- Improve documentation
- Share your learnings

## 📄 License

MIT License - Feel free to use for learning and production

## 🙏 Acknowledgments

Built with:
- Apache Flink 1.18.1
- Apache Kafka 7.6.0 (Confluent Platform)
- Apache Avro 1.11.3
- Java 21

---

**Questions?** Review the inline code comments - every class has detailed explanations!

**Next Steps**: Follow the Learning Path above to deepen your understanding 🚀
