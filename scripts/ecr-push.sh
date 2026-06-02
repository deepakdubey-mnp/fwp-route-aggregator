#!/usr/bin/env bash
# scripts/ecr-push.sh — build the OCI image locally and deploy to ECS.
#
# Mirrors the steps in .github/workflows/deploy.yml so a local push produces
# exactly the same artefact and rollout as the CI pipeline.
#
# Prerequisites:
#   - AWS CLI v2      (brew install awscli)
#   - Docker Desktop  (running)
#   - jq              (brew install jq)
#   - Active AWS credentials with ECR push + ECS deploy permissions
#     (aws sso login, or exported AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
#
# Usage:
#   chmod +x scripts/ecr-push.sh
#   ./scripts/ecr-push.sh                  # build → push → deploy
#   ./scripts/ecr-push.sh --skip-deploy    # build → push only (no ECS rollout)
#   ./scripts/ecr-push.sh --help

set -euo pipefail

# ── Config (mirrors .github/workflows/deploy.yml env block) ──────────────────
AWS_REGION="ap-south-2"
ECR_REPOSITORY="fun-with-flights/airlines-aggregator"
ECS_CLUSTER="fwf-demo-cluster"
ECS_SERVICE="airlines-aggregator-svc"
CONTAINER_NAME="airlines-aggregator"
TASK_FAMILY="airlines-aggregator"
# ─────────────────────────────────────────────────────────────────────────────

SKIP_DEPLOY=false

usage() {
  echo "Usage: $0 [--skip-deploy] [--help]"
  echo ""
  echo "  --skip-deploy   Build and push to ECR but do not trigger ECS rollout."
  echo "  --help, -h      Show this message."
}

for arg in "$@"; do
  case $arg in
    --skip-deploy) SKIP_DEPLOY=true ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown argument: $arg"; usage; exit 1 ;;
  esac
done

# ── Dependency checks ─────────────────────────────────────────────────────────
for cmd in aws docker jq git; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: '$cmd' is not installed or not on PATH."
    echo "       aws → brew install awscli"
    echo "       jq  → brew install jq"
    exit 1
  fi
done

if ! docker info &>/dev/null; then
  echo "Error: Docker daemon is not running. Start Docker Desktop and retry."
  exit 1
fi

# ── Resolve image version ─────────────────────────────────────────────────────
# Same derivation as CI: strip -SNAPSHOT, append short git SHA instead of run number
BASE_VERSION=$(grep "^version = " build.gradle \
  | tr -d "' " | cut -d'=' -f2 | sed 's/-SNAPSHOT//')
GIT_SHA=$(git rev-parse --short HEAD)
IMAGE_TAG="${BASE_VERSION}-${GIT_SHA}"

echo "==> Image tag : ${IMAGE_TAG}"

# ── Verify AWS credentials ────────────────────────────────────────────────────
echo "==> Verifying AWS credentials..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null) || {
  echo "Error: No valid AWS credentials found."
  echo "       Run 'aws sso login' or export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY."
  exit 1
}
echo "    Account : ${ACCOUNT_ID}"
echo "    Region  : ${AWS_REGION}"

ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ECR_IMAGE="${ECR_REGISTRY}/${ECR_REPOSITORY}"
VERSIONED_TAG="${ECR_IMAGE}:${IMAGE_TAG}"
LATEST_TAG="${ECR_IMAGE}:latest"

# ── Build OCI image (linux/amd64 — matches ECS Fargate task architecture) ────
echo ""
echo "==> Building OCI image..."
echo "    Target : ${VERSIONED_TAG}"
./gradlew bootBuildImage --imageName="${VERSIONED_TAG}"

# ── Login to ECR ──────────────────────────────────────────────────────────────
echo ""
echo "==> Logging in to ECR (${ECR_REGISTRY})..."
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

# ── Tag and push ──────────────────────────────────────────────────────────────
echo ""
echo "==> Pushing ${VERSIONED_TAG}..."
docker push "${VERSIONED_TAG}"

echo "==> Tagging and pushing :latest..."
docker tag "${VERSIONED_TAG}" "${LATEST_TAG}"
docker push "${LATEST_TAG}"

# ── ECS deploy ────────────────────────────────────────────────────────────────
if [ "${SKIP_DEPLOY}" = "true" ]; then
  echo ""
  echo "==> Skipping ECS deployment (--skip-deploy was set)."
else
  echo ""
  echo "==> Fetching current ECS task definition (${TASK_FAMILY})..."
  TASK_DEF_JSON=$(aws ecs describe-task-definition \
    --region "${AWS_REGION}" \
    --task-definition "${TASK_FAMILY}" \
    --query taskDefinition \
    --output json 2>/dev/null) || {
    echo "Error: Could not fetch task definition '${TASK_FAMILY}'."
    echo "       If this is the first deploy, register an initial task definition in the AWS console"
    echo "       or run the CI pipeline once to bootstrap it."
    exit 1
  }

  # Strip read-only fields that must not be sent to RegisterTaskDefinition
  echo "==> Rendering new task definition with image ${VERSIONED_TAG}..."
  NEW_TASK_DEF=$(echo "${TASK_DEF_JSON}" | jq \
    --arg IMAGE "${VERSIONED_TAG}" \
    --arg NAME  "${CONTAINER_NAME}" '
    del(
      .taskDefinitionArn,
      .revision,
      .status,
      .requiresAttributes,
      .compatibilities,
      .registeredAt,
      .registeredBy
    )
    | .containerDefinitions |= map(
        if .name == $NAME then .image = $IMAGE else . end
      )
  ')

  # Register the new revision
  NEW_TASK_DEF_ARN=$(echo "${NEW_TASK_DEF}" | aws ecs register-task-definition \
    --region "${AWS_REGION}" \
    --cli-input-json file:///dev/stdin \
    --query taskDefinition.taskDefinitionArn \
    --output text)
  echo "    Registered : ${NEW_TASK_DEF_ARN}"

  # Update the service to use the new task definition revision
  echo "==> Updating ECS service (${ECS_SERVICE})..."
  aws ecs update-service \
    --region "${AWS_REGION}" \
    --cluster "${ECS_CLUSTER}" \
    --service "${ECS_SERVICE}" \
    --task-definition "${NEW_TASK_DEF_ARN}" \
    --output json \
  | jq -r '.service | "    Status: \(.status)  Desired: \(.desiredCount)  Running: \(.runningCount)"'

  # Block until the service reaches steady state (mirrors CI wait-for-service-stability)
  echo "==> Waiting for service stability..."
  echo "    (This typically takes 1-3 minutes. Ctrl-C to skip the wait — deploy continues in AWS.)"
  aws ecs wait services-stable \
    --region "${AWS_REGION}" \
    --cluster "${ECS_CLUSTER}" \
    --services "${ECS_SERVICE}"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "======================================================"
echo " Done."
echo " Image   : ${VERSIONED_TAG}"
if [ "${SKIP_DEPLOY}" = "false" ]; then
  echo " Cluster : ${ECS_CLUSTER}"
  echo " Service : ${ECS_SERVICE}"
  echo " Console : https://${AWS_REGION}.console.aws.amazon.com/ecs/v2/clusters/${ECS_CLUSTER}/services/${ECS_SERVICE}"
fi
echo "======================================================"