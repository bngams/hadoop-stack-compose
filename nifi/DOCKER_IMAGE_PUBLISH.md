# Building and Publishing NiFi Custom Image

This custom NiFi image includes HDFS processors that were removed from the default NiFi 2.0 distribution.

## What's Included

- Base: `apache/nifi:2.0.0`
- Added: `nifi-hadoop-nar-2.0.0.nar` (HDFS processors)
- Added: `nifi-hadoop-libraries-nar-2.0.0.nar` (Hadoop libraries)

## Build the Image

```bash
# Navigate to the nifi directory
cd nifi

# Build the image
docker build -t bngams/nifi-custom:2.0.0 .
docker tag bngams/nifi-custom:2.0.0 bngams/nifi-custom:latest
```

## Test Locally

```bash
# Run the custom image locally
docker run -d -p 8443:8443 \
  -e SINGLE_USER_CREDENTIALS_USERNAME=admin \
  -e SINGLE_USER_CREDENTIALS_PASSWORD=adminadminadmin \
  --name nifi-test \
  bngams/nifi-custom:2.0.0

# Check logs for HDFS processors
docker logs nifi-test | grep -i "PutHDFS"

# Cleanup
docker stop nifi-test
docker rm nifi-test
```

## Push to Docker Hub

```bash
# Login to Docker Hub
docker login

# Push both tags
docker push bngams/nifi-custom:2.0.0
docker push bngams/nifi-custom:latest
```

## Update docker-compose.yml

Once pushed, update your docker-compose.yml:

```yaml
nifi:
  image: bngams/nifi-custom:latest
  # Remove the extensions volume mount as NARs are now in the image
  volumes:
    - nifi_conf:/opt/nifi/nifi-current/conf
    - nifi_logs:/opt/nifi/nifi-current/logs
    # ... other volumes (remove the nar_extensions line)
```

## Verify Installation

After starting the container:

```bash
docker exec nifi ls -lh /opt/nifi/nifi-current/lib/ | grep hadoop
```

You should see the two NAR files.
