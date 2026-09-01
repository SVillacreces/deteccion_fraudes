#!/bin/bash

set -e

echo "=========================================="
echo " Plataforma de Validacion en Tiempo Real"
echo "=========================================="

# Verificar Docker
if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: Docker no esta instalado."
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker no esta iniciado."
    exit 1
fi

echo ""
echo "[1/4] Iniciando contenedores..."
docker compose up -d

echo ""
echo "[2/4] Esperando Kafka..."
sleep 10

echo ""
echo "[3/4] Verificando topic transactions..."

if ! docker exec realtime-kafka \
    kafka-topics \
    --bootstrap-server kafka:29092 \
    --list 2>/dev/null | grep -q "^transactions$"; then

    echo "Creando topic transactions..."

    docker exec realtime-kafka \
        kafka-topics \
        --bootstrap-server kafka:29092 \
        --create \
        --topic transactions \
        --partitions 1 \
        --replication-factor 1
else
    echo "Topic transactions disponible."
fi

echo ""
echo "[4/4] Verificando Job de Flink..."

if docker exec realtime-flink-jobmanager \
    flink list 2>/dev/null | grep -q "Real-Time Transaction Validation"; then

    echo "Job de Flink ya esta ejecutandose."

else

    echo "Desplegando Job de Flink..."

    docker cp \
        flink-job/target/realtime-validation-1.0-SNAPSHOT.jar \
        realtime-flink-jobmanager:/tmp/realtime-validation.jar

    docker exec realtime-flink-jobmanager \
        flink run -d /tmp/realtime-validation.jar
fi

echo ""
echo "=========================================="
echo " Infraestructura iniciada correctamente"
echo "=========================================="
echo ""
echo "Flink:"
echo "http://localhost:8081"
echo ""
echo "Iniciando Dashboard..."
echo ""

source .venv/bin/activate

uvicorn dashboard.app:app \
    --host 0.0.0.0 \
    --port 8000
