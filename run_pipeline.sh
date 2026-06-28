#!/usr/bin/env bash

set -e
set -o pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

export PYTHONPATH=.
export UV_CACHE_DIR="${UV_CACHE_DIR:-$PROJECT_DIR/.uv-cache}"

UV="${UV:-uv}"
LOG_DIR="${LOG_DIR:-$PROJECT_DIR/logs}"
mkdir -p "$LOG_DIR"

if [ -f "$PROJECT_DIR/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_DIR/.env"
  set +a
fi

send_telegram() {
  if [ -z "${TELEGRAM_TOKEN:-}" ] || [ -z "${CHAT_ID:-}" ]; then
    return 0
  fi

  curl -s -X POST https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage \
  -d chat_id=${CHAT_ID} \
  -d text="$1" > /dev/null
}

DATA=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/pipeline_${DATA}.log"

trap 'send_telegram "ERRO no pipeline invest-certo em $(date)"' ERR

echo "========================================" >> $LOG_FILE
echo "INICIANDO PIPELINE - $(date)" >> $LOG_FILE
echo "========================================" >> $LOG_FILE

send_telegram "Pipeline iniciado: $(date)"

echo "Sync de dependencias..." >> $LOG_FILE
$UV sync >> $LOG_FILE 2>&1

echo "Rodando Bronze..." >> $LOG_FILE
$UV run python pipelines/bronze/collect_prices.py >> $LOG_FILE 2>&1

echo "Rodando Silver..." >> $LOG_FILE
$UV run python pipelines/silver/transform_prices.py >> $LOG_FILE 2>&1

echo "Rodando Gold..." >> $LOG_FILE
$UV run python pipelines/gold/build_features.py >> $LOG_FILE 2>&1

echo "========================================" >> $LOG_FILE
echo "PIPELINE FINALIZADO - $(date)" >> $LOG_FILE
echo "========================================" >> $LOG_FILE

send_telegram "Pipeline finalizado com sucesso: $(date)"
