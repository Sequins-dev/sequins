# Sequins Helm Chart

A Helm chart for deploying Sequins, a cross-platform OpenTelemetry database with embedded OTLP endpoint.

## Overview

Sequins is an OpenTelemetry visualization tool that provides:
- Embedded OTLP endpoint (gRPC and HTTP)
- Service map visualization
- Distributed trace visualization
- Log search and viewing
- Metrics dashboards
- Profile flame graphs

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+
- S3-compatible storage (AWS S3, MinIO, etc.)
- S3 credentials with read/write access

## Installation

### Add Helm repository (if published)

```bash
helm repo add sequins https://qard.github.io/sequins
helm repo update
```

### Install from local chart

```bash
# From the repository root
helm install sequins ./deploy/helm/sequins \
  --set storage.s3.bucket=my-sequins-data \
  --set storage.s3.accessKeyId=YOUR_ACCESS_KEY_ID \
  --set storage.s3.secretAccessKey=YOUR_SECRET_ACCESS_KEY
```

### Install with existing secret (recommended)

```bash
# Create secret with S3 credentials
kubectl create secret generic sequins-s3-credentials \
  --from-literal=access-key-id=YOUR_ACCESS_KEY_ID \
  --from-literal=secret-access-key=YOUR_SECRET_ACCESS_KEY

# Install using existing secret
helm install sequins ./deploy/helm/sequins \
  --set storage.s3.bucket=my-sequins-data \
  --set storage.s3.existingSecret=sequins-s3-credentials
```

## Configuration

The following table lists the configurable parameters of the Sequins chart and their default values.

### Image Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.repository` | Container image repository | `ghcr.io/qard/sequins-daemon` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `image.tag` | Image tag (defaults to chart appVersion) | `""` |
| `imagePullSecrets` | Image pull secrets | `[]` |

### Deployment Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `nameOverride` | Override chart name | `""` |
| `fullnameOverride` | Override full name | `""` |

### Service Account

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create service account | `true` |
| `serviceAccount.automount` | Auto-mount service account token | `true` |
| `serviceAccount.annotations` | Service account annotations | `{}` |
| `serviceAccount.name` | Service account name | `""` |

### OTLP Service

| Parameter | Description | Default |
|-----------|-------------|---------|
| `otlp.service.type` | Service type | `ClusterIP` |
| `otlp.service.grpcPort` | gRPC port | `4317` |
| `otlp.service.httpPort` | HTTP port | `4318` |
| `otlp.service.annotations` | Service annotations | `{}` |

### Query API (Enterprise)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `queryApi.enabled` | Enable Query API | `true` |
| `queryApi.service.type` | Service type | `ClusterIP` |
| `queryApi.service.port` | HTTP port | `8080` |
| `queryApi.service.annotations` | Service annotations | `{}` |

### Ingress

| Parameter | Description | Default |
|-----------|-------------|---------|
| `queryApi.ingress.enabled` | Enable ingress | `false` |
| `queryApi.ingress.className` | Ingress class name | `""` |
| `queryApi.ingress.annotations` | Ingress annotations | `{}` |
| `queryApi.ingress.hosts` | Ingress hosts configuration | See values.yaml |
| `queryApi.ingress.tls` | TLS configuration | `[]` |

### Storage Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `storage.s3.bucket` | S3 bucket name | `""` |
| `storage.s3.region` | S3 region | `us-east-1` |
| `storage.s3.endpoint` | S3 endpoint (for S3-compatible services) | `""` |
| `storage.s3.existingSecret` | Existing secret with S3 credentials | `""` |
| `storage.s3.accessKeyIdKey` | Key in secret for access key ID | `access-key-id` |
| `storage.s3.secretAccessKeyKey` | Key in secret for secret access key | `secret-access-key` |
| `storage.s3.accessKeyId` | S3 access key ID (not recommended for production) | `""` |
| `storage.s3.secretAccessKey` | S3 secret access key (not recommended for production) | `""` |

### Data Retention

| Parameter | Description | Default |
|-----------|-------------|---------|
| `storage.retention.traces` | Trace retention period | `7d` |
| `storage.retention.logs` | Log retention period | `30d` |
| `storage.retention.metrics` | Metric retention period | `30d` |
| `storage.retention.profiles` | Profile retention period | `7d` |

### Resources

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.limits.cpu` | CPU limit | `1000m` |
| `resources.limits.memory` | Memory limit | `1Gi` |
| `resources.requests.cpu` | CPU request | `100m` |
| `resources.requests.memory` | Memory request | `256Mi` |

### Probes

| Parameter | Description | Default |
|-----------|-------------|---------|
| `livenessProbe` | Liveness probe configuration | See values.yaml |
| `readinessProbe` | Readiness probe configuration | See values.yaml |

### Autoscaling

| Parameter | Description | Default |
|-----------|-------------|---------|
| `autoscaling.enabled` | Enable autoscaling | `false` |
| `autoscaling.minReplicas` | Minimum replicas | `1` |
| `autoscaling.maxReplicas` | Maximum replicas | `10` |
| `autoscaling.targetCPUUtilizationPercentage` | Target CPU utilization | `80` |

### Security

| Parameter | Description | Default |
|-----------|-------------|---------|
| `podSecurityContext` | Pod security context | See values.yaml |
| `securityContext` | Container security context | See values.yaml |

### Additional Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `podAnnotations` | Pod annotations | `{}` |
| `podLabels` | Pod labels | `{}` |
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `affinity` | Affinity rules | `{}` |
| `volumes` | Additional volumes | `[]` |
| `volumeMounts` | Additional volume mounts | `[]` |
| `extraEnv` | Extra environment variables | `[]` |
| `extraEnvFrom` | Extra environment from secrets/configmaps | `[]` |

## Usage Examples

### Basic Installation

```bash
helm install sequins ./deploy/helm/sequins \
  --set storage.s3.bucket=my-bucket \
  --set storage.s3.region=us-west-2 \
  --set storage.s3.accessKeyId=AKIAIOSFODNN7EXAMPLE \
  --set storage.s3.secretAccessKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### With Custom Retention Policies

```bash
helm install sequins ./deploy/helm/sequins \
  --set storage.s3.bucket=my-bucket \
  --set storage.s3.existingSecret=my-s3-secret \
  --set storage.retention.traces=14d \
  --set storage.retention.logs=60d \
  --set storage.retention.metrics=90d
```

### With Ingress Enabled

```bash
helm install sequins ./deploy/helm/sequins \
  --set storage.s3.bucket=my-bucket \
  --set storage.s3.existingSecret=my-s3-secret \
  --set queryApi.ingress.enabled=true \
  --set queryApi.ingress.className=nginx \
  --set queryApi.ingress.hosts[0].host=sequins.example.com \
  --set queryApi.ingress.hosts[0].paths[0].path=/ \
  --set queryApi.ingress.hosts[0].paths[0].pathType=Prefix
```

### With MinIO (S3-compatible)

```bash
helm install sequins ./deploy/helm/sequins \
  --set storage.s3.bucket=sequins \
  --set storage.s3.endpoint=http://minio.default.svc.cluster.local:9000 \
  --set storage.s3.region=us-east-1 \
  --set storage.s3.accessKeyId=minioadmin \
  --set storage.s3.secretAccessKey=minioadmin
```

### Using AWS IAM Roles (IRSA/Workload Identity)

```bash
# Create service account with IAM role annotation
helm install sequins ./deploy/helm/sequins \
  --set storage.s3.bucket=my-bucket \
  --set storage.s3.region=us-west-2 \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=arn:aws:iam::ACCOUNT_ID:role/sequins-role \
  --set storage.s3.existingSecret="" \
  --set storage.s3.accessKeyId="" \
  --set storage.s3.secretAccessKey=""
```

## Configuring Applications to Send Telemetry

After installing Sequins, configure your applications to send OTLP data:

### Environment Variables

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://sequins-otlp:4318
```

### gRPC

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://sequins-otlp:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
```

### From Outside the Cluster

If you've enabled ingress, use the external URL:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=https://sequins.example.com
```

## Upgrading

```bash
# Upgrade to new chart version
helm upgrade sequins ./deploy/helm/sequins

# Change configuration
helm upgrade sequins ./deploy/helm/sequins \
  --set storage.retention.traces=30d \
  --reuse-values
```

## Uninstalling

```bash
helm uninstall sequins
```

**Note:** This will not delete the S3 bucket or data stored in it.

## Troubleshooting

### Check Pod Status

```bash
kubectl get pods -l app.kubernetes.io/name=sequins
kubectl describe pod -l app.kubernetes.io/name=sequins
```

### View Logs

```bash
kubectl logs -l app.kubernetes.io/name=sequins -f
```

### Verify S3 Connectivity

```bash
kubectl exec -it deployment/sequins -- env | grep S3
```

### Test OTLP Endpoints

```bash
# Port-forward OTLP HTTP endpoint
kubectl port-forward svc/sequins-otlp 4318:4318

# Test with curl
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d '{}'
```

## License

MIT OR Apache-2.0

## Support

For issues and questions:
- GitHub Issues: https://github.com/qard/sequins/issues
- Documentation: https://github.com/qard/sequins
