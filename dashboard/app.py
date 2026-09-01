from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from cassandra.cluster import Cluster

import socket
import urllib.request
import time

from datetime import datetime, timezone


# ==================================================
# FASTAPI
# ==================================================

app = FastAPI(
    title="Monitor de Validación en Tiempo Real"
)


# ==================================================
# ARCHIVOS ESTÁTICOS
# ==================================================

app.mount(
    "/static",
    StaticFiles(directory="dashboard/static"),
    name="static"
)


# ==================================================
# CASSANDRA
# ==================================================

cluster = Cluster(
    ["127.0.0.1"],
    port=9042
)

session = cluster.connect(
    "realtime_validation"
)


# ==================================================
# PÁGINA PRINCIPAL
# ==================================================

@app.get("/")
def inicio():
    return FileResponse(
        "dashboard/static/index.html"
    )


# ==================================================
# HEALTH
# ==================================================

@app.get("/api/health")
def health():
    return {
        "estado": "ACTIVO"
    }


# ==================================================
# TRANSACCIONES
# ==================================================

@app.get("/api/transacciones")
def obtener_transacciones():

    try:

        query = """
            SELECT
                transaction_id,
                amount,
                transaction_frequency,
                expected_status,
                detected_status,
                evaluation,
                latency_ms
            FROM transaction_validation
            LIMIT 100
        """

        rows = session.execute(query)

        transacciones = []

        for row in rows:

            if (
                row.expected_status is None
                or row.detected_status is None
                or row.evaluation is None
                or row.latency_ms is None
            ):
                continue

            transacciones.append({
                "transaction_id":
                    row.transaction_id,

                "amount":
                    row.amount,

                "transaction_frequency":
                    row.transaction_frequency,

                "expected_status":
                    row.expected_status,

                "detected_status":
                    row.detected_status,

                "evaluation":
                    row.evaluation,

                "latency_ms":
                    row.latency_ms
            })

        transacciones.sort(
            key=lambda x: x["transaction_id"],
            reverse=True
        )

        transacciones = transacciones[:20]

        return {
            "total":
                len(transacciones),

            "transacciones":
                transacciones
        }

    except Exception as e:

        return {
            "error":
                str(e),

            "transacciones":
                []
        }


# ==================================================
# MÉTRICAS
# ==================================================

@app.get("/api/metricas")
def obtener_metricas():

    try:

        query = """
            SELECT
                evaluation,
                expected_status,
                detected_status
            FROM transaction_validation
            LIMIT 1000
        """

        rows = session.execute(query)

        tp = 0
        tn = 0
        fp = 0
        fn = 0
        total = 0

        for row in rows:

            if (
                row.evaluation is None
                or row.expected_status is None
                or row.detected_status is None
            ):
                continue

            total += 1

            if row.evaluation == "TP":
                tp += 1

            elif row.evaluation == "TN":
                tn += 1

            elif row.evaluation == "FP":
                fp += 1

            elif row.evaluation == "FN":
                fn += 1

        if (tp + fp) > 0:
            precision = tp / (tp + fp)
        else:
            precision = 0

        if (tp + fn) > 0:
            recall = tp / (tp + fn)
        else:
            recall = 0

        if (precision + recall) > 0:
            f1_score = (
                2
                * precision
                * recall
                / (precision + recall)
            )
        else:
            f1_score = 0

        if (fp + tn) > 0:
            false_positive_rate = (
                fp / (fp + tn)
            )
        else:
            false_positive_rate = 0

        if total > 0:
            porcentaje_anomalias = (
                (tp + fp)
                / total
                * 100
            )
        else:
            porcentaje_anomalias = 0

        return {
            "total":
                total,

            "porcentaje_anomalias":
                porcentaje_anomalias,

            "precision":
                precision * 100,

            "recall":
                recall * 100,

            "f1_score":
                f1_score,

            "false_positive_rate":
                false_positive_rate * 100,

            "matriz_confusion": {
                "tp": tp,
                "tn": tn,
                "fp": fp,
                "fn": fn
            }
        }

    except Exception as e:

        return {
            "error":
                str(e),

            "total":
                0,

            "porcentaje_anomalias":
                0,

            "precision":
                0,

            "recall":
                0,

            "f1_score":
                0,

            "false_positive_rate":
                0,

            "matriz_confusion": {
                "tp": 0,
                "tn": 0,
                "fp": 0,
                "fn": 0
            }
        }


# ==================================================
# RENDIMIENTO
# ==================================================

@app.get("/api/rendimiento")
def obtener_rendimiento():

    try:

        bucket_date = datetime.now(
            timezone.utc
        ).date()

        query_ultimo = """
            SELECT processing_timestamp
            FROM transaction_validation_by_time
            WHERE bucket_date = %s
            LIMIT 1
        """

        ultimo = session.execute(
            query_ultimo,
            (bucket_date,)
        ).one()

        if ultimo is None:

            return {
                "total_eventos_ventana": 0,
                "tps": 0,
                "latencia_promedio_ms": 0,
                "latencia_minima_ms": 0,
                "latencia_maxima_ms": 0,
                "latencia_p95_ms": 0,
                "latencias_negativas": 0,
                "ventana_segundos": 60,
                "flujo_activo": False,
                "tiempo_observado_segundos": 0
            }

        timestamp_mas_reciente = (
            ultimo.processing_timestamp
        )

        timestamp_actual = int(
            time.time() * 1000
        )

        tiempo_sin_eventos_ms = (
            timestamp_actual
            - timestamp_mas_reciente
        )

        if tiempo_sin_eventos_ms > 5000:

            return {
                "total_eventos_ventana": 0,
                "tps": 0,
                "latencia_promedio_ms": 0,
                "latencia_minima_ms": 0,
                "latencia_maxima_ms": 0,
                "latencia_p95_ms": 0,
                "latencias_negativas": 0,
                "ventana_segundos": 60,
                "flujo_activo": False,
                "tiempo_observado_segundos": 0
            }

        limite_inferior = (
            timestamp_mas_reciente
            - 60000
        )

        query = """
            SELECT
                processing_timestamp,
                latency_ms
            FROM transaction_validation_by_time
            WHERE bucket_date = %s
              AND processing_timestamp >= %s
              AND processing_timestamp <= %s
        """

        rows = session.execute(
            query,
            (
                bucket_date,
                limite_inferior,
                timestamp_mas_reciente
            )
        )

        eventos = list(rows)

        if not eventos:

            return {
                "total_eventos_ventana": 0,
                "tps": 0,
                "latencia_promedio_ms": 0,
                "latencia_minima_ms": 0,
                "latencia_maxima_ms": 0,
                "latencia_p95_ms": 0,
                "latencias_negativas": 0,
                "ventana_segundos": 60,
                "flujo_activo": True,
                "tiempo_observado_segundos": 0
            }

        timestamps_eventos = [
            row.processing_timestamp
            for row in eventos
        ]

        timestamp_primero = min(
            timestamps_eventos
        )

        timestamp_ultimo = max(
            timestamps_eventos
        )

        tiempo_observado_segundos = (
            timestamp_ultimo
            - timestamp_primero
        ) / 1000.0

        if (
            tiempo_observado_segundos > 0
            and len(eventos) > 1
        ):

            tps = (
                len(eventos) - 1
            ) / tiempo_observado_segundos

        else:
            tps = 0

        latencias = [
            row.latency_ms
            for row in eventos
            if row.latency_ms is not None
        ]

        latencias_negativas = len([
            latency
            for latency in latencias
            if latency < 0
        ])

        latencias_validas = [
            latency
            for latency in latencias
            if latency >= 0
        ]

        if latencias_validas:

            latencias_ordenadas = sorted(
                latencias_validas
            )

            latencia_promedio = (
                sum(latencias_validas)
                / len(latencias_validas)
            )

            latencia_minima = min(
                latencias_validas
            )

            latencia_maxima = max(
                latencias_validas
            )

            indice_p95 = int(
                0.95
                * (
                    len(latencias_ordenadas)
                    - 1
                )
            )

            latencia_p95 = (
                latencias_ordenadas[
                    indice_p95
                ]
            )

        else:

            latencia_promedio = 0
            latencia_minima = 0
            latencia_maxima = 0
            latencia_p95 = 0

        return {
            "total_eventos_ventana":
                len(eventos),

            "tps":
                round(tps, 2),

            "latencia_promedio_ms":
                round(
                    latencia_promedio,
                    2
                ),

            "latencia_minima_ms":
                latencia_minima,

            "latencia_maxima_ms":
                latencia_maxima,

            "latencia_p95_ms":
                latencia_p95,

            "latencias_negativas":
                latencias_negativas,

            "ventana_segundos":
                60,

            "flujo_activo":
                True,

            "tiempo_observado_segundos":
                round(
                    tiempo_observado_segundos,
                    2
                )
        }

    except Exception as e:

        return {
            "error":
                str(e)
        }


# ==================================================
# INFRAESTRUCTURA
# ==================================================

@app.get("/api/infraestructura")
def obtener_infraestructura():

    componentes = {}

    # Cassandra
    try:

        session.execute(
            "SELECT now() FROM system.local"
        )

        componentes["cassandra"] = {
            "estado": "ACTIVO",
            "detalle": "Conexión establecida"
        }

    except Exception as e:

        componentes["cassandra"] = {
            "estado": "INACTIVO",
            "detalle": str(e)
        }

    # Kafka
    try:

        conexion = socket.create_connection(
            (
                "127.0.0.1",
                9092
            ),
            timeout=2
        )

        conexion.close()

        componentes["kafka"] = {
            "estado": "ACTIVO",
            "detalle": "Puerto 9092 disponible"
        }

    except Exception as e:

        componentes["kafka"] = {
            "estado": "INACTIVO",
            "detalle": str(e)
        }

    # Flink
    try:

        with urllib.request.urlopen(
            "http://127.0.0.1:8081/overview",
            timeout=2
        ) as respuesta:

            if respuesta.status == 200:

                componentes["flink"] = {
                    "estado": "ACTIVO",
                    "detalle": "JobManager disponible"
                }

            else:

                componentes["flink"] = {
                    "estado": "INACTIVO",
                    "detalle": "Respuesta inesperada"
                }

    except Exception as e:

        componentes["flink"] = {
            "estado": "INACTIVO",
            "detalle": str(e)
        }

    # API
    componentes["api"] = {
        "estado": "ACTIVO",
        "detalle": "FastAPI operativo"
    }

    componentes_activos = sum(
        1
        for componente
        in componentes.values()
        if componente["estado"] == "ACTIVO"
    )

    return {
        "estado_general": (
            "ACTIVO"
            if componentes_activos
            == len(componentes)
            else "DEGRADADO"
        ),

        "componentes_activos":
            componentes_activos,

        "total_componentes":
            len(componentes),

        "componentes":
            componentes
    }