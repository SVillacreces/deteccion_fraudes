# Plataforma de Validación de Datos en Tiempo Real

Prototipo desarrollado para la validación continua de datos y detección
de anomalías en flujos de datos en tiempo real.

## Arquitectura

El prototipo utiliza:

- Apache Kafka para la ingesta de eventos.
- Apache Flink para procesamiento y validación en tiempo real.
- Apache Cassandra para almacenamiento.
- FastAPI para la API de monitorización.
- Interfaz web para visualización de métricas.
- Python para la generación controlada de eventos.

## Requisitos

Antes de ejecutar el proyecto es necesario disponer de:

- Docker Desktop
- Python 3
- Git

No es necesario instalar localmente Kafka, Flink ni Cassandra,
ya que estos componentes se ejecutan mediante contenedores Docker.

## 1. Clonar el repositorio

git clone <URL-DEL-REPOSITORIO>

cd realtime-validation

## 2. Crear el entorno virtual de Python

python3 -m venv .venv

Activar el entorno:

### macOS / Linux

source .venv/bin/activate

### Windows PowerShell

.venv\Scripts\Activate.ps1

## 3. Instalar las dependencias

pip install -r dashboard/requirements.txt

## 4. Iniciar la plataforma

En macOS/Linux:

./start.sh

El script inicia la infraestructura necesaria y despliega el proceso
de validación en Apache Flink.

Una vez iniciado:

Dashboard:
http://localhost:8000

Apache Flink:
http://localhost:8081

## 5. Generar el flujo de datos

Abrir una segunda terminal, activar el entorno virtual:

source .venv/bin/activate

y ejecutar:

./demo.sh

El productor genera por defecto un flujo continuo de:

- 1 transacción por segundo.
- 20 % de anomalías controladas.
- Ejecución indefinida.

Para detener la generación:

Ctrl + C

También puede ejecutarse manualmente:

python producer.py --eventos 20 --tps 1 --anomalias 20

Prueba de mayor carga:

python producer.py --eventos 300 --tps 10 --anomalias 20

Flujo continuo a 10 TPS:

python producer.py --eventos 0 --tps 10 --anomalias 20

## 6. Detener la plataforma

./stop.sh

Los volúmenes persistentes no se eliminan, por lo que los datos
almacenados en Cassandra pueden conservarse entre ejecuciones.

## Nota sobre la detección de anomalías

El prototipo utiliza un mecanismo basado en reglas y umbrales para
identificar comportamientos potencialmente anómalos.

Los umbrales utilizados tienen carácter experimental y permiten
evaluar el funcionamiento de la arquitectura en un entorno controlado.
No representan reglas universales de detección de fraude ni un sistema
productivo de prevención de fraude.
