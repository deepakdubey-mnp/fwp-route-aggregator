#!/usr/bin/env bash
# Run once from a machine with AWS admin credentials to wire OIDC trust between
# GitHub Actions and AWS. After running, copy the printed ROLE_ARN into GitHub
# Settings → Secrets → Actions → AWS_DEPLOY_ROLE_ARN.
#
# Usage:
#   chmod +x scripts/setup-github-oidc.sh
#   ./scripts/setup-github-oidc.sh

set -euo pipefail

GITHUB_ORG="deepakdubey-mnp"
GITHUB_REPO="fwp-route-aggregator"
AWS_REGION="ap-south-2"
ROLE_NAME="github-actions-fwp-deploy"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

echo "==> Account: ${ACCOUNT_ID}  Region: ${AWS_REGION}"

# ── 1. Create the OIDC provider (idempotent — fails silently if it exists) ───
OIDC_URL="https://token.actions.githubusercontent.com"
OIDC_ARN="arn:aws:iam::${ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"

if ! aws iam get-open-id-connect-provider --open-id-connect-provider-arn "${OIDC_ARN}" \
     &>/dev/null; then
  echo "==> Creating GitHub OIDC provider..."
  aws iam create-open-id-connect-provider \
    --url "${OIDC_URL}" \
    --client-id-list "sts.amazonaws.com" \
    --thumbprint-list "6938fd4d98bab03faadb97b34396831e3780aea1" \
    --thumbprint-list "1c58a3a8518e8759bf075b76b750d4f2df264fcd"
else
  echo "==> OIDC provider already exists, skipping."
fi

# ── 2. Create IAM role with trust policy scoped to this repo ────────────────
TRUST_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "${OIDC_ARN}"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:${GITHUB_ORG}/${GITHUB_REPO}:ref:refs/heads/master"
        }
      }
    }
  ]
}
EOF
)

if ! aws iam get-role --role-name "${ROLE_NAME}" &>/dev/null; then
  echo "==> Creating IAM role: ${ROLE_NAME}..."
  aws iam create-role \
    --role-name "${ROLE_NAME}" \
    --assume-role-policy-document "${TRUST_POLICY}" \
    --description "Assumed by GitHub Actions to deploy fwp-route-aggregator"
else
  echo "==> Updating trust policy on existing role..."
  aws iam update-assume-role-policy \
    --role-name "${ROLE_NAME}" \
    --policy-document "${TRUST_POLICY}"
fi

# ── 3. Attach inline policy — least-privilege for ECR push + ECS deploy ─────
DEPLOY_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ECRAuth",
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Sid": "ECRPush",
      "Effect": "Allow",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload",
        "ecr:PutImage",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"
      ],
      "Resource": "arn:aws:ecr:${AWS_REGION}:${ACCOUNT_ID}:repository/fun-with-flights/airlines-aggregator"
    },
    {
      "Sid": "ECSDeployReadWrite",
      "Effect": "Allow",
      "Action": [
        "ecs:DescribeTaskDefinition",
        "ecs:RegisterTaskDefinition",
        "ecs:UpdateService",
        "ecs:DescribeServices"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassExecutionRole",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": [
        "arn:aws:iam::${ACCOUNT_ID}:role/*ecs-execution*",
        "arn:aws:iam::${ACCOUNT_ID}:role/*ecs-task*"
      ]
    }
  ]
}
EOF
)

aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name "fwp-deploy-policy" \
  --policy-document "${DEPLOY_POLICY}"

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
echo ""
echo "======================================================"
echo " OIDC trust configured."
echo " Add this secret to GitHub → Settings → Secrets → Actions:"
echo ""
echo "   Name : AWS_DEPLOY_ROLE_ARN"
echo "   Value: ${ROLE_ARN}"
echo "======================================================"