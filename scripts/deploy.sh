#!/bin/bash
# ============================================================================
# DAG ORCHESTRATOR DEPLOYMENT SCRIPT
# ============================================================================
# EPOCH: 5 - DAG ORCHESTRATION
# PURPOSE: Build and deploy both orchestrator and worker images
# CREATED: 02 FEB 2026
# ============================================================================
#
# Usage:
#   ./scripts/deploy.sh                    # Deploy version from __version__.py
#   ./scripts/deploy.sh 0.2.1.2            # Deploy specific version
#   ./scripts/deploy.sh 0.2.1.2 --build-only   # Build only, no deploy
#   ./scripts/deploy.sh 0.2.1.2 --deploy-only  # Deploy only (images must exist)
#
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
REGISTRY="rmhazureacr"
RESOURCE_GROUP="rmhazure_rg"
ORCHESTRATOR_APP="rmhdagmaster"
WORKER_APP="rmhdagworker"
ORCHESTRATOR_IMAGE="rmhdagmaster"
WORKER_IMAGE="rmhdagworker"

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Parse arguments
VERSION=""
BUILD_ONLY=false
DEPLOY_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --build-only)
            BUILD_ONLY=true
            shift
            ;;
        --deploy-only)
            DEPLOY_ONLY=true
            shift
            ;;
        *)
            VERSION="$1"
            shift
            ;;
    esac
done

# Get version from __version__.py if not provided
if [ -z "$VERSION" ]; then
    VERSION=$(python3 -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from __version__ import __version__
print(__version__)
")
fi

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}DAG Orchestrator Deployment${NC}"
echo -e "${BLUE}============================================${NC}"
echo -e "Version:     ${GREEN}v${VERSION}${NC}"
echo -e "Registry:    ${REGISTRY}.azurecr.io"
echo -e "Build:       $([ "$DEPLOY_ONLY" = true ] && echo "Skip" || echo "Yes")"
echo -e "Deploy:      $([ "$BUILD_ONLY" = true ] && echo "Skip" || echo "Yes")"
echo -e "${BLUE}============================================${NC}"
echo ""

# Update version in __version__.py if different
CURRENT_VERSION=$(python3 -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
from __version__ import __version__
print(__version__)
")

if [ "$CURRENT_VERSION" != "$VERSION" ]; then
    echo -e "${YELLOW}Updating version from $CURRENT_VERSION to $VERSION...${NC}"

    # Update version file
    sed -i '' "s/__version__ = \".*\"/__version__ = \"$VERSION\"/" "$PROJECT_ROOT/__version__.py"

    # Update build date
    BUILD_DATE=$(date +%Y-%m-%d)
    sed -i '' "s/BUILD_DATE = \".*\"/BUILD_DATE = \"$BUILD_DATE\"/" "$PROJECT_ROOT/__version__.py"

    echo -e "${GREEN}Version updated${NC}"
fi

# Build images
if [ "$DEPLOY_ONLY" != true ]; then
    echo ""
    echo -e "${BLUE}Building Orchestrator Image...${NC}"
    echo -e "  Image: ${REGISTRY}.azurecr.io/${ORCHESTRATOR_IMAGE}:v${VERSION}"

    cd "$PROJECT_ROOT"
    az acr build --registry "$REGISTRY" \
        --image "${ORCHESTRATOR_IMAGE}:v${VERSION}" \
        -f Dockerfile . \
        --no-logs

    echo -e "${GREEN}Orchestrator image built successfully${NC}"

    echo ""
    echo -e "${BLUE}Building Worker Image...${NC}"
    echo -e "  Image: ${REGISTRY}.azurecr.io/${WORKER_IMAGE}:v${VERSION}"

    az acr build --registry "$REGISTRY" \
        --image "${WORKER_IMAGE}:v${VERSION}" \
        -f Dockerfile.worker . \
        --no-logs

    echo -e "${GREEN}Worker image built successfully${NC}"
fi

# Deploy to Web Apps
if [ "$BUILD_ONLY" != true ]; then
    echo ""
    echo -e "${BLUE}Deploying Orchestrator...${NC}"
    echo -e "  App: ${ORCHESTRATOR_APP}"
    echo -e "  Image: ${REGISTRY}.azurecr.io/${ORCHESTRATOR_IMAGE}:v${VERSION}"

    az webapp config container set \
        --name "$ORCHESTRATOR_APP" \
        --resource-group "$RESOURCE_GROUP" \
        --container-image-name "${REGISTRY}.azurecr.io/${ORCHESTRATOR_IMAGE}:v${VERSION}" \
        --output none

    echo -e "${GREEN}Orchestrator deployed${NC}"

    echo ""
    echo -e "${BLUE}Deploying Worker...${NC}"
    echo -e "  App: ${WORKER_APP}"
    echo -e "  Image: ${REGISTRY}.azurecr.io/${WORKER_IMAGE}:v${VERSION}"

    az webapp config container set \
        --name "$WORKER_APP" \
        --resource-group "$RESOURCE_GROUP" \
        --container-image-name "${REGISTRY}.azurecr.io/${WORKER_IMAGE}:v${VERSION}" \
        --output none

    echo -e "${GREEN}Worker deployed${NC}"

    echo ""
    echo -e "${BLUE}Restarting applications...${NC}"

    az webapp restart --name "$ORCHESTRATOR_APP" --resource-group "$RESOURCE_GROUP" &
    az webapp restart --name "$WORKER_APP" --resource-group "$RESOURCE_GROUP" &
    wait

    echo -e "${GREEN}Applications restarted${NC}"

    # Wait for startup
    echo ""
    echo -e "${BLUE}Waiting for applications to start (30s)...${NC}"
    sleep 30

    # Health check
    echo ""
    echo -e "${BLUE}Checking health...${NC}"

    ORCHESTRATOR_URL="https://${ORCHESTRATOR_APP}-gcfzd5bqfxc7g7cv.eastus-01.azurewebsites.net"
    WORKER_URL="https://${WORKER_APP}-fedshwfme6drd6gq.eastus-01.azurewebsites.net"

    echo -e "Orchestrator:"
    curl -s "${ORCHESTRATOR_URL}/health" | python3 -m json.tool 2>/dev/null || echo "  (not responding yet)"

    echo ""
    echo -e "Worker:"
    curl -s "${WORKER_URL}/health" | python3 -m json.tool 2>/dev/null || echo "  (not responding yet)"
fi

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}Deployment Complete - v${VERSION}${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo -e "Orchestrator: ${ORCHESTRATOR_URL}"
echo -e "Worker:       ${WORKER_URL}"
echo ""
