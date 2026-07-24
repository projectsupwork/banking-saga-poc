#!/bin/bash
# init-localstack.sh — Automatically executed by LocalStack on startup
# Equivalent to an infrastructure migration script (IaC)

set -e

echo "🔧 Initializing AWS resources (LocalStack)..."

# Configuration
REGION="us-east-1"
ENDPOINT="http://localhost:4566"
AWS="awslocal"  # alias for aws --endpoint-url=$ENDPOINT

# ─── SQS: create queues ────────────────────────────────────────
echo "📬 Creating SQS queues..."

$AWS sqs create-queue \
    --queue-name notifications \
    --region $REGION

$AWS sqs create-queue \
    --queue-name notifications-dlq \
    --region $REGION

# Configure the DLQ (Dead Letter Queue) for unprocessed messages
NOTIF_URL=$($AWS sqs get-queue-url --queue-name notifications --query 'QueueUrl' --output text)
DLQ_ARN=$($AWS sqs get-queue-attributes \
    --queue-url $($AWS sqs get-queue-url --queue-name notifications-dlq --query 'QueueUrl' --output text) \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text)

$AWS sqs set-queue-attributes \
    --queue-url $NOTIF_URL \
    --attributes "{
        \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\",
        \"MessageRetentionPeriod\": \"86400\",
        \"VisibilityTimeout\": \"30\"
    }"

# ─── List created resources ────────────────────────────────────
echo ""
echo "✅ LocalStack resources created successfully!"
echo ""
echo "📋 Available SQS queues:"
$AWS sqs list-queues --region $REGION

echo ""
echo "🌐 AWS Console (LocalStack): http://localhost:4566"
echo "   notifications queue: $NOTIF_URL"
