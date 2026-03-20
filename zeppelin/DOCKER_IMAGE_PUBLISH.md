# Publishing Your Custom Zeppelin Image

## Prerequisites

1. Create a Docker Hub account at https://hub.docker.com
2. Login to Docker Hub from command line:
```bash
docker login
# Enter your Docker Hub username and password
```

---

## Step 1: Tag Your Image

```bash
# Tag the existing image with your Docker Hub username
docker tag hadoop-stack-compose-zeppelin-custom:latest bngams/zeppelin-custom:1.0

# Also tag as 'latest' for convenience
docker tag hadoop-stack-compose-zeppelin-custom:latest bngams/zeppelin-custom:latest
```

---

## Step 2: Push to Docker Hub

```bash
# Push version 1.0
docker push bngams/zeppelin-custom:1.0

# Push latest
docker push bngams/zeppelin-custom:latest
```

---

## Step 3: Update docker-compose.yml

Replace the build configuration with your published image:

**Before:**
```yaml
zeppelin-custom:
  build:
    context: ./zeppelin
    dockerfile: Dockerfile
  container_name: zeppelin-custom
```

**After:**
```yaml
zeppelin-custom:
  image: bngams/zeppelin-custom:1.0
  container_name: zeppelin-custom
```

---

## Alternative: Use GitHub Container Registry (ghcr.io)

If you prefer GitHub:

```bash
# Login to GitHub Container Registry
echo $GITHUB_TOKEN | docker login ghcr.io -u bngams --password-stdin

# Tag for GitHub
docker tag hadoop-stack-compose-zeppelin-custom:latest ghcr.io/bngams/zeppelin-custom:1.0

# Push
docker push ghcr.io/bngams/zeppelin-custom:1.0
```

Then in docker-compose.yml:
```yaml
zeppelin-custom:
  image: ghcr.io/bngams/zeppelin-custom:1.0
  container_name: zeppelin-custom
```

---

## Version Management Best Practices

### Semantic Versioning

Use semantic versioning (MAJOR.MINOR.PATCH):

```bash
# Major version (breaking changes)
docker tag hadoop-stack-compose-zeppelin-custom:latest bngams/zeppelin-custom:2.0.0

# Minor version (new features)
docker tag hadoop-stack-compose-zeppelin-custom:latest bngams/zeppelin-custom:1.1.0

# Patch version (bug fixes)
docker tag hadoop-stack-compose-zeppelin-custom:latest bngams/zeppelin-custom:1.0.1
```

### Tag Multiple Versions

```bash
# Tag specific version and latest
docker tag hadoop-stack-compose-zeppelin-custom:latest bngams/zeppelin-custom:1.0.0
docker tag hadoop-stack-compose-zeppelin-custom:latest bngams/zeppelin-custom:1.0
docker tag hadoop-stack-compose-zeppelin-custom:latest bngams/zeppelin-custom:latest

# Push all tags
docker push bngams/zeppelin-custom:1.0.0
docker push bngams/zeppelin-custom:1.0
docker push bngams/zeppelin-custom:latest
```

---

## Complete Workflow Script

Save this as `scripts/publish-zeppelin.sh`:

```bash
#!/bin/bash

VERSION=${1:-latest}
DOCKER_USER="bngams"
IMAGE_NAME="zeppelin-custom"

echo "📦 Building image..."
docker-compose build zeppelin-custom

echo "🏷️  Tagging as ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}"
docker tag hadoop-stack-compose-zeppelin-custom:latest ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}

echo "🚀 Pushing to Docker Hub..."
docker push ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}

if [ "$VERSION" != "latest" ]; then
    echo "🏷️  Also tagging as latest..."
    docker tag hadoop-stack-compose-zeppelin-custom:latest ${DOCKER_USER}/${IMAGE_NAME}:latest
    docker push ${DOCKER_USER}/${IMAGE_NAME}:latest
fi

echo "✅ Done! Image available at: ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}"
echo ""
echo "📝 Update docker-compose.yml with:"
echo "  image: ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}"
```

Usage:
```bash
chmod +x scripts/publish-zeppelin.sh

# Publish as version 1.0
./scripts/publish-zeppelin.sh 1.0

# Publish as latest
./scripts/publish-zeppelin.sh latest
```

---

## Benefits of Publishing Your Image

✅ **Faster deployments** - No need to rebuild every time
✅ **Share with team** - Others can use your exact configuration
✅ **Version control** - Track different versions of your setup
✅ **Portability** - Use on any Docker host
✅ **CI/CD friendly** - Easier to integrate in pipelines

---

## Image Size Optimization (Optional)

Your current image is quite large due to build tools. To optimize:

### Multi-stage Build

Update `zeppelin/Dockerfile`:

```dockerfile
# Build stage
FROM apache/zeppelin:0.12.0 as builder

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3.8-dev && \
    rm -rf /var/lib/apt/lists/*

RUN python3.8 -m pip install --no-cache-dir \
    pandas \
    numpy \
    matplotlib \
    pyarrow \
    scikit-learn \
    seaborn \
    plotly \
    openpyxl \
    xlrd

# Final stage
FROM apache/zeppelin:0.12.0

USER root

# Copy only Python packages from builder
COPY --from=builder /usr/local/lib/python3.8 /usr/local/lib/python3.8
COPY --from=builder /usr/bin/python3.8 /usr/bin/python3.8

ENV PYSPARK_PYTHON=python3.8
ENV PYSPARK_DRIVER_PYTHON=python3.8

RUN mkdir -p /opt/hadoop/conf

WORKDIR /opt/zeppelin
```

This reduces image size by not keeping build tools in the final image.

---

## Quick Start for Others

Once published, anyone can use your image:

```yaml
# docker-compose.yml
services:
  zeppelin-custom:
    image: bngams/zeppelin-custom:1.0
    # ... rest of configuration
```

```bash
docker-compose up -d
```

No Dockerfile needed!
