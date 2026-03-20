#!/bin/bash

# Publish Zeppelin Custom Image Script
# Usage: ./publish-zeppelin.sh [version]
# Example: ./publish-zeppelin.sh 1.0

VERSION=${1:-latest}
DOCKER_USER="bngams"
IMAGE_NAME="zeppelin-custom"
LOCAL_IMAGE="hadoop-stack-compose-zeppelin-custom:latest"

echo "🔍 Checking Docker login status..."
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker."
    exit 1
fi

echo ""
echo "📦 Building image..."
if ! docker-compose build zeppelin-custom; then
    echo "❌ Build failed"
    exit 1
fi

echo ""
echo "🏷️  Tagging images..."
docker tag ${LOCAL_IMAGE} ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}

if [ "$VERSION" != "latest" ]; then
    docker tag ${LOCAL_IMAGE} ${DOCKER_USER}/${IMAGE_NAME}:latest
    echo "   ✓ Tagged as ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}"
    echo "   ✓ Tagged as ${DOCKER_USER}/${IMAGE_NAME}:latest"
else
    echo "   ✓ Tagged as ${DOCKER_USER}/${IMAGE_NAME}:latest"
fi

echo ""
echo "🚀 Pushing to Docker Hub..."
echo "   (You may need to login with: docker login)"
echo ""

if docker push ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}; then
    echo "   ✅ Pushed ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}"
else
    echo "   ❌ Push failed. Did you login? Run: docker login"
    exit 1
fi

if [ "$VERSION" != "latest" ]; then
    if docker push ${DOCKER_USER}/${IMAGE_NAME}:latest; then
        echo "   ✅ Pushed ${DOCKER_USER}/${IMAGE_NAME}:latest"
    fi
fi

echo ""
echo "✅ Success! Image published to Docker Hub"
echo ""
echo "📝 To use this image, update docker-compose.yml:"
echo ""
echo "  zeppelin-custom:"
echo "    image: ${DOCKER_USER}/${IMAGE_NAME}:${VERSION}"
echo "    container_name: zeppelin-custom"
echo "    # ... rest of config"
echo ""
echo "🌐 View on Docker Hub: https://hub.docker.com/r/${DOCKER_USER}/${IMAGE_NAME}"
echo ""
