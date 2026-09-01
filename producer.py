import argparse
import json
import random
import time

from datetime import datetime, timezone

from kafka import KafkaProducer


# --------------------------------------------------
# Argumentos del experimento
# --------------------------------------------------

def crear_argumentos():

    parser = argparse.ArgumentParser(
        description=(
            "Generador controlado de transacciones "
            "para experimentos de validación"
        )
    )

    parser.add_argument(
        "--eventos",
        type=int,
        default=0,
        help=(
            "Número de eventos a generar. "
            "0 = ejecución continua."
        )
    )

    parser.add_argument(
        "--tps",
        type=float,
        default=1.0,
        help="Transacciones por segundo."
    )

    parser.add_argument(
        "--anomalias",
        type=float,
        default=20.0,
        help=(
            "Porcentaje de anomalías "
            "a generar."
        )
    )

    return parser.parse_args()


# --------------------------------------------------
# Ejecución principal
# --------------------------------------------------

def main():

    args = crear_argumentos()


    # --------------------------------------------------
    # Validaciones
    # --------------------------------------------------

    if args.tps <= 0:

        raise ValueError(
            "El valor de --tps debe ser mayor que 0."
        )


    if args.eventos < 0:

        raise ValueError(
            "El valor de --eventos no puede ser negativo."
        )


    if (
        args.anomalias < 0
        or args.anomalias > 100
    ):

        raise ValueError(
            "El porcentaje de anomalías debe "
            "estar entre 0 y 100."
        )


    # --------------------------------------------------
    # Identificador único del experimento
    # --------------------------------------------------

    experiment_id = datetime.now(
        timezone.utc
    ).strftime(
        "EXP_%Y%m%d_%H%M%S"
    )


    # --------------------------------------------------
    # Producer Kafka
    # --------------------------------------------------

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",

        value_serializer=lambda value:
            json.dumps(value).encode("utf-8")
    )


    # --------------------------------------------------
    # Configuración temporal
    # --------------------------------------------------

    intervalo = (
        1 / args.tps
    )


    # --------------------------------------------------
    # ID inicial de transacción
    #
    # Se utiliza timestamp para evitar reutilizar
    # IDs entre distintas ejecuciones.
    # --------------------------------------------------

    transaction_id = int(
        time.time() * 1000
    )


    # --------------------------------------------------
    # Contadores
    # --------------------------------------------------

    eventos_generados = 0

    anomalias_generadas = 0

    validos_generados = 0


    # --------------------------------------------------
    # Información del experimento
    # --------------------------------------------------

    print()

    print(
        "=============================================="
    )

    print(
        "Generador experimental iniciado"
    )

    print(
        "=============================================="
    )


    print(
        f"ID del experimento    : "
        f"{experiment_id}"
    )


    print(
        f"Eventos objetivo      : "
        f"{args.eventos if args.eventos > 0 else 'CONTINUO'}"
    )


    print(
        f"Tasa configurada      : "
        f"{args.tps} TPS"
    )


    print(
        f"Anomalías configuradas: "
        f"{args.anomalias}%"
    )


    print(
        "Topic                 : transactions"
    )


    print(
        "=============================================="
    )

    print()


    # --------------------------------------------------
    # Generación de eventos
    # --------------------------------------------------

    try:

        while True:


            # ------------------------------------------
            # Fin del experimento controlado
            # ------------------------------------------

            if (
                args.eventos > 0
                and eventos_generados >= args.eventos
            ):

                break


            inicio_evento = (
                time.perf_counter()
            )


            transaction_id += 1


            # ------------------------------------------
            # Ground truth controlado
            # ------------------------------------------

            is_anomaly = (
                random.random()
                < (
                    args.anomalias
                    / 100
                )
            )


            # ------------------------------------------
            # Evento anómalo
            # ------------------------------------------

            if is_anomaly:

                anomaly_type = random.choice(
                    [
                        "AMOUNT",
                        "FREQUENCY"
                    ]
                )


                if anomaly_type == "AMOUNT":

                    amount = round(
                        random.uniform(
                            401,
                            500
                        ),
                        2
                    )


                    transaction_frequency = (
                        random.randint(
                            1,
                            4
                        )
                    )


                else:

                    amount = round(
                        random.uniform(
                            10,
                            400
                        ),
                        2
                    )


                    transaction_frequency = 5


                expected_status = (
                    "ANOMALY"
                )


                anomalias_generadas += 1


            # ------------------------------------------
            # Evento válido
            # ------------------------------------------

            else:

                amount = round(
                    random.uniform(
                        10,
                        400
                    ),
                    2
                )


                transaction_frequency = (
                    random.randint(
                        1,
                        4
                    )
                )


                expected_status = (
                    "VALID"
                )


                validos_generados += 1


            # ------------------------------------------
            # Evento enviado a Kafka
            # ------------------------------------------

            transaction = {

                "experiment_id":
                    experiment_id,

                "transaction_id":
                    transaction_id,

                "amount":
                    amount,

                "transaction_frequency":
                    transaction_frequency,

                "timestamp":
                    int(
                        time.time()
                        * 1000
                    ),

                "expected_status":
                    expected_status

            }


            producer.send(
                "transactions",
                value=transaction
            )


            eventos_generados += 1


            # ------------------------------------------
            # Log
            # ------------------------------------------

            print(

                f"[{eventos_generados}] "

                f"EXP={experiment_id} | "

                f"ID="
                f"{transaction['transaction_id']} | "

                f"Amount="
                f"${transaction['amount']:.2f} | "

                f"Frequency="
                f"{transaction['transaction_frequency']} | "

                f"Expected="
                f"{transaction['expected_status']}"

            )


            # ------------------------------------------
            # Control de TPS
            # ------------------------------------------

            tiempo_utilizado = (
                time.perf_counter()
                - inicio_evento
            )


            tiempo_espera = (
                intervalo
                - tiempo_utilizado
            )


            if tiempo_espera > 0:

                time.sleep(
                    tiempo_espera
                )


    except KeyboardInterrupt:

        print()

        print(
            "Experimento detenido manualmente."
        )


    finally:

        producer.flush()

        producer.close()


    # --------------------------------------------------
    # Resumen final
    # --------------------------------------------------

    print()

    print(
        "=============================================="
    )

    print(
        "Experimento finalizado"
    )

    print(
        "=============================================="
    )


    print(
        f"ID del experimento   : "
        f"{experiment_id}"
    )


    print(
        f"Eventos generados    : "
        f"{eventos_generados}"
    )


    print(
        f"Eventos válidos      : "
        f"{validos_generados}"
    )


    print(
        f"Anomalías generadas  : "
        f"{anomalias_generadas}"
    )


    if eventos_generados > 0:

        porcentaje_real = (
            anomalias_generadas
            / eventos_generados
            * 100
        )


        print(
            f"Anomalías reales     : "
            f"{porcentaje_real:.2f}%"
        )


    print(
        f"TPS configurado      : "
        f"{args.tps}"
    )


    print(
        "=============================================="
    )


# --------------------------------------------------
# Inicio
# --------------------------------------------------

if __name__ == "__main__":

    main()