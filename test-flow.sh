#!/bin/bash
# test-flow.sh — Demonstrates the full POC flow
# Requires: docker, curl, jq, awslocal (pip install awscli-local)

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
RESET='\033[0m'

echo -e "${BLUE}"
echo "╔═══════════════════════════════════════════════╗"
echo "║   Banking Transfer POC — SAGA Demonstration   ║"
echo "╚═══════════════════════════════════════════════╝"
echo -e "${RESET}"

API="http://localhost:8080"
SQS="http://localhost:4566"

# ─── Wait for services ─────────────────────────────────────────
echo -e "${YELLOW}⏳ Waiting for services to start...${RESET}"
until curl -sf "$API/transfers/health" > /dev/null; do
    sleep 2; echo -n "."
done
echo -e " ${GREEN}✓ API ready${RESET}"

# ─── Scenario 1: Successful transfer ───────────────────────────
echo ""
echo -e "${GREEN}═══ SCENARIO 1: Regular Transfer ═════════════════${RESET}"
echo "Alice (ACC-001) → Bob (ACC-002) | \$500.00"
echo ""

RESP=$(curl -s -X POST "$API/transfers" \
  -H "Content-Type: application/json" \
  -d '{"sourceAccount": "ACC-001", "targetAccount": "ACC-002", "amount": 500.00}')

echo "API response:"
echo "$RESP" | jq '.'

PROTOCOL=$(echo "$RESP" | jq -r '.protocolId')
echo -e "${GREEN}✓ Protocol generated: $PROTOCOL${RESET}"

echo ""
echo "⏳ Waiting for SAGA processing (3s)..."
sleep 3

echo ""
echo "📬 Checking the notification on the SQS queue..."
MSG=$(awslocal sqs receive-message \
    --queue-url "http://localhost:4566/000000000000/notifications" \
    --max-number-of-messages 1 \
    --region us-east-1 2>/dev/null || echo '{"Messages":[]}')
echo "$MSG" | jq '.Messages[0].Body | fromjson' 2>/dev/null || echo "(queue empty — message already consumed)"

# ─── Scenario 2: Insufficient balance ──────────────────────────
echo ""
echo -e "${RED}═══ SCENARIO 2: Insufficient Balance (SAGA Step 1 fails) ═${RESET}"
echo "Carol (ACC-003, \$250) tries to transfer \$1,000.00"
echo ""

curl -s -X POST "$API/transfers" \
  -H "Content-Type: application/json" \
  -d '{"sourceAccount": "ACC-003", "targetAccount": "ACC-001", "amount": 1000.00}' | jq '.'

echo -e "${GREEN}✓ API accepted (202) — SAGA Step 1 will cancel internally${RESET}"

# ─── Scenario 3: Multiple transfers ────────────────────────────
echo ""
echo -e "${BLUE}═══ SCENARIO 3: Multiple concurrent transfers ═${RESET}"

for i in 1 2 3; do
  curl -s -X POST "$API/transfers" \
    -H "Content-Type: application/json" \
    -d "{\"sourceAccount\": \"ACC-001\", \"targetAccount\": \"ACC-002\", \"amount\": 10.0$i}" | \
    jq -r '.protocolId' &
done
wait

echo -e "${GREEN}✓ 3 transfers published to Kafka${RESET}"

# ─── Scenario 4: Payroll loan ──────────────────────────────────
echo ""
echo -e "${GREEN}═══ SCENARIO 4: Payroll Loan ═════════════════════════${RESET}"
echo "Alice (ACC-001) takes \$5,000.00 over 36 months, informed income \$3,000.00"
echo ""

RESP_LOAN=$(curl -s -X POST "$API/loans/payroll" \
  -H "Content-Type: application/json" \
  -d '{"customerAccount": "ACC-001", "enrollmentId": "ENR-001", "monthlyIncome": 3000.00, "requestedAmount": 5000.00, "termMonths": 36}')

echo "API response:"
echo "$RESP_LOAN" | jq '.'

CONTRACT=$(echo "$RESP_LOAN" | jq -r '.contractId')
echo -e "${GREEN}✓ Contract generated: $CONTRACT${RESET}"

echo ""
echo "⏳ Waiting for the origination SAGA (Step1 margin → Step2 activate → Step3 disburse → Step4 SQS)..."
sleep 3

echo ""
echo "📄 Contract detail (disbursement already credited to ACC-001 via AccountService.credit):"
curl -s "$API/loans/payroll/$CONTRACT" | jq '.'

echo ""
echo -e "${RED}═══ SCENARIO 5: Payroll Margin Exceeded (SAGA Step 1 fails) ═${RESET}"
echo "An income of \$500.00 cannot afford the installment of a \$10,000.00 loan over 24 months"
echo ""

curl -s -X POST "$API/loans/payroll" \
  -H "Content-Type: application/json" \
  -d '{"customerAccount": "ACC-002", "enrollmentId": "ENR-002", "monthlyIncome": 500.00, "requestedAmount": 10000.00, "termMonths": 24}' | jq '.'

echo -e "${GREEN}✓ API accepted (202) — SAGA Step 1 will cancel the contract internally${RESET}"

# ─── Useful links ──────────────────────────────────────────────
echo ""
echo -e "${YELLOW}═══ USEFUL LINKS ══════════════════════════════════${RESET}"
echo "  API Health:     $API/transfers/health"
echo "  Kafdrop (Kafka UI): http://localhost:9000"
echo "  LocalStack SQS: http://localhost:4566"
echo ""
echo -e "${GREEN}✅ Demonstration completed!${RESET}"
