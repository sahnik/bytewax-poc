# OpenShift Deployment Instructions

This guide provides instructions for deploying the Bytewax data pipeline on Red Hat's OpenShift Sandbox with Confluent Cloud Kafka.

## Prerequisites

- **Red Hat OpenShift Sandbox Account:**
  - Sign up at https://developers.redhat.com/developer-sandbox/get-started
  - Free tier provides 14 days of access with auto-renewal
  - No credit card required

- **Confluent Cloud Account:**
  - Sign up at https://confluent.cloud
  - Create a Basic cluster (free $400 credit available)
  - Generate API Key and Secret

- **Required Tools:**
  - OpenShift CLI (`oc`) - download from sandbox console
  - Docker or Podman (for building images)

## Step 1: Access Red Hat OpenShift Sandbox

### Get Your Sandbox Cluster
1. Go to https://developers.redhat.com/developer-sandbox/get-started
2. Click "Start using your sandbox"
3. Login with your Red Hat account
4. Access your cluster via the web console

### Login via CLI
```bash
# Get login command from sandbox web console
# Click your username → "Copy login command" → "Display Token"
oc login --token=<your-token> --server=https://api.sandbox-m2.ll9k.p1.openshiftapps.com:6443
```

## Step 2: Setup Confluent Cloud Kafka

### Create Kafka Cluster and Topics
1. Login to https://confluent.cloud
2. Create a new **Basic** cluster (free tier)
3. Create the required topics:
   ```
   input-topic (8 partitions)
   output-topic (8 partitions) 
   lookup-topic (1 partition, compacted)
   error-topic (8 partitions)
   ```
4. Generate API Key and Secret:
   - Go to **Data Integration → API Keys**
   - Click **Create Key** → **Global Access**
   - Save the Key and Secret

### Get Bootstrap Server URL
1. Go to **Cluster Overview**
2. Copy the **Bootstrap server** URL (e.g., `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`)

## Step 3: Build and Push Container Images

### Using OpenShift BuildConfigs (Recommended)

#### Option A: Binary Build from Local Directory
```bash
# Create a new project for your application
oc new-project sahnik-dev

# Create build config for binary builds (must use default Dockerfile name)
oc new-build --strategy docker \
  --name=bytewax-pipeline \
  --binary=true

# Start build from current directory  
oc start-build bytewax-pipeline --from-dir=. --follow
```

#### Option B: Git Source Build (after pushing to GitHub)
```bash
# First ensure your ocp_setup branch exists and has the Dockerfiles
git checkout -b ocp_setup
git add .
git commit -m "Add OpenShift deployment files"
git push origin ocp_setup

# Then create build from GitHub (pipeline only)
oc new-build https://github.com/sahnik/bytewax-poc.git#ocp_setup \
  --context-dir=. \
  --dockerfile=Dockerfile.pipeline \
  --name=bytewax-pipeline

# Monitor build progress
oc logs -f bc/bytewax-pipeline
```

### Alternative: Push to External Registry
```bash
# Build and push to quay.io or docker.io
docker build -t quay.io/your-username/bytewax-pipeline:latest -f Dockerfile.pipeline .
docker build -t quay.io/your-username/bytewax-publisher:latest -f Dockerfile.publisher .
docker build -t quay.io/your-username/bytewax-populator:latest -f Dockerfile.populator .

# Push images
docker push quay.io/your-username/bytewax-pipeline:latest
docker push quay.io/your-username/bytewax-publisher:latest
docker push quay.io/your-username/bytewax-populator:latest
```

## Step 4: Configure Confluent Cloud Connection

### Create Kafka Credentials Secret
```bash
# Encode your Confluent Cloud API credentials
echo -n "your-api-key" | base64
echo -n "your-api-secret" | base64

# Create secret with encoded values
oc create secret generic bytewax-kafka-credentials \
  --from-literal=KAFKA_SASL_USERNAME="<base64-encoded-api-key>" \
  --from-literal=KAFKA_SASL_PASSWORD="<base64-encoded-api-secret>" \
  -n sahnik-dev
```

### Update Configuration
Edit `k8s/base/configmap.yaml` with your Confluent Cloud details:

```yaml
data:
  KAFKA_BOOTSTRAP_SERVERS: "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
  KAFKA_SECURITY_PROTOCOL: "SASL_SSL"
  KAFKA_SASL_MECHANISM: "PLAIN"
  # ... other settings
```

## Step 5: Deploy the Pipeline

### Deploy Using Kustomize
```bash
# Apply development configuration
oc apply -k k8s/overlays/dev/

# Verify deployment
oc get pods -n sahnik-dev
oc get services -n sahnik-dev

# Check deployment status
oc rollout status deployment/dev-bytewax-pipeline -n sahnik-dev
```

### Initialize Lookup Data (Run Locally)
```bash
# Populate lookup data from your local machine
python utils/populate_lookup.py --count 1000

# Verify topics in Confluent Cloud console
```

## Step 6: Test the Pipeline

### Generate Test Data (Run Locally)
```bash
# Run data publisher from your local machine  
python utils/data_publisher.py --rate 100 --duration 300

# Monitor pipeline logs in OpenShift
oc logs -f deployment/dev-bytewax-pipeline -n sahnik-dev
```

### Access Metrics Dashboard
```bash
# Port forward to access metrics
oc port-forward service/dev-bytewax-pipeline-metrics 8000:8000 -n sahnik-dev

# View metrics in browser
open http://localhost:8000/metrics
```

### Monitor via OpenShift Console
1. Go to your sandbox cluster web console
2. Switch to **Developer** perspective
3. Select **bytewax-pipeline-dev** project
4. Navigate to:
   - **Topology** to see application overview
   - **Monitoring** to view resource usage
   - **Logs** to see application logs

## Troubleshooting

### Common Issues

1. **Build failures:**
   ```bash
   # Check build logs
   oc logs -f bc/bytewax-pipeline
   
   # Restart failed build
   oc start-build bytewax-pipeline
   ```

2. **Kafka connection issues:**
   ```bash
   # Test Confluent Cloud connectivity
   oc run kafka-test --image=confluentinc/cp-kafka:latest --rm -it -- bash
   # Inside container:
   kafka-topics --bootstrap-server pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 \
     --command-config /tmp/client.properties --list
   ```

3. **Authentication errors:**
   ```bash
   # Verify secret is created correctly
   oc get secret bytewax-kafka-credentials -o yaml
   
   # Check if credentials are properly base64 encoded
   echo "your-api-key" | base64
   ```

4. **Resource limits in sandbox:**
   ```bash
   # Check resource quotas
   oc describe quota -n bytewax-pipeline-dev
   
   # Reduce resource requests if needed
   # Edit k8s/overlays/dev/kustomization.yaml
   ```

5. **Pod crashes:**
   ```bash
   # Check pod logs
   oc logs deployment/dev-bytewax-pipeline -n bytewax-pipeline-dev
   
   # Check events
   oc get events -n bytewax-pipeline-dev --sort-by='.lastTimestamp'
   
   # Describe problematic pods
   oc describe pod <pod-name> -n bytewax-pipeline-dev
   ```

### Cleanup

```bash
# Delete the project (removes all resources)
oc delete project bytewax-pipeline-dev
```

## Production Considerations

### Sandbox Limitations
- **14-day auto-renewal limit** - not suitable for long-term production
- **Resource constraints** - limited CPU/memory quotas
- **No persistent storage** - data is lost when sandbox resets
- **No custom domains** - uses sandbox URLs

### Production Deployment
For production use:

1. **Upgrade to Red Hat OpenShift Online/Dedicated**
2. **Use production Confluent Cloud cluster** with appropriate SLAs
3. **Implement proper CI/CD** with GitOps (ArgoCD, Tekton)
4. **Configure monitoring** (OpenShift monitoring stack)
5. **Setup alerting** and incident response
6. **Implement backup and disaster recovery**
7. **Use dedicated image registry** (Quay.io Enterprise)

## Next Steps

1. **Scale testing** - increase partition count and worker replicas
2. **Monitor metrics** - tune performance based on throughput
3. **Implement alerting** - set up Confluent Cloud and OpenShift alerts
4. **Data quality monitoring** - add custom metrics for data validation
5. **Production deployment** - migrate to enterprise OpenShift cluster

## Useful Commands

```bash
# Monitor pipeline logs in real-time
oc logs -f deployment/dev-bytewax-pipeline -n bytewax-pipeline-dev

# Scale pipeline workers
oc scale deployment/dev-bytewax-pipeline --replicas=3 -n bytewax-pipeline-dev

# Check Confluent Cloud topics
kafka-topics --bootstrap-server pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 \
  --command-config client.properties --list

# Port forward for local access
oc port-forward service/dev-bytewax-pipeline-metrics 8000:8000 -n bytewax-pipeline-dev
```