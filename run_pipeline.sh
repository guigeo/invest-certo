#!/usr/bin/env bash

set -e
set -o pipefail

cd /data/projects/invest-certo

export PYTHONPATH=.

UV="/home/guigeo/.local/bin/uv"

# ===== TELEGRAM CONFIG =====
# carregar .env
set -a
source /data/projects/invest-certo/.env
set +a

send_telegram() {
  curl -s -X POST https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage \
  -d chat_id=${CHAT_ID} \
  -d text="$1" > /dev/null
}

# ===== LOG =====
DATA=$(date +%Y-%m-%d)
LOG_FILE="/data/projects/invest-certo/logs/pipeline_${DATA}.log"

# ===== ALERTA DE ERRO =====
trap 'send_telegram "❌ ERRO no pipeline invest-certo em $(date)"' ERR

echo "========================================" >> $LOG_FILE
echo "INICIANDO PIPELINE - $(date)" >> $LOG_FILE
echo "========================================" >> $LOG_FILE

send_telegram "🚀 Pipeline iniciado: $(date)"

echo "🔧 Sync de dependências..." >> $LOG_FILE
$UV sync >> $LOG_FILE 2>&1

echo "🥉 Rodando Bronze..." >> $LOG_FILE
$UV run python pipelines/bronze/collect_prices.py >> $LOG_FILE 2>&1

echo "🥈 Rodando Silver..." >> $LOG_FILE
$UV run python pipelines/silver/transform_prices.py >> $LOG_FILE 2>&1

echo "🥇 Rodando Gold..." >> $LOG_FILE
$UV run python pipelines/gold/build_features.py >> $LOG_FILE 2>&1

echo "========================================" >> $LOG_FILE
echo "PIPELINE FINALIZADO - $(date)" >> $LOG_FILE
echo "========================================" >> $LOG_FILE

send_telegram "✅ Pipeline finalizado com sucesso: $(date)"