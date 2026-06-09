# PI-Palau-Massacesi-Cohen-Puentes

Proyecto integrador de sistema distribuido para "telefono ruidoso":
1. Se envia una frase original.
2. Se generan copias para distorsion.
3. Workers consumen tareas desde cola y guardan frases distorsionadas.
4. Un worker LLM reconstruye la frase original por lotes incrementales.
5. La API expone estado y resultados en tiempo real.

## Diagrama

![Diagrama del proyecto](SD-Proyecto_diagrama.png)

## Objetivo

 Proponer un sistema distribuido orientado a colas, con persistencia y escalado horizontal, desplegable en local con Docker Compose o en Kubernetes.

## Arquitectura

Componentes principales:
1. CLI interactivo: envia frases, consulta estado y visualiza resultados.
2. API FastAPI: crea jobs, publica tareas, expone estado y endpoints.
3. RabbitMQ: broker para jobs y resultados.
4. Workers de distorsion: consumen tareas, aplican ruido y persisten resultados.
5. LLM Worker: consume resultados, hace batches adaptativos, consulta Ollama y guarda guesses.
6. Ollama: motor de inferencia para reconstruccion de frase.
7. PostgreSQL: persistencia de jobs, distorsiones y guesses.
8. Service LoadBalancer: entrada externa al cluster y distribucion de trafico hacia replicas de API.

Rutas de mensajes:
1. API publica en `telephone.jobs`.
2. Worker consume `telephone.jobs`, distorsiona y publica en `telephone.results`.
3. LLM Worker consume `telephone.results`.
4. API consulta PostgreSQL y expone resultados al CLI.

## Estructura del repositorio

1. [code/api.py](code/api.py): API HTTP.
2. [code/worker.py](code/worker.py): worker de distorsion.
3. [code/llm_worker.py](code/llm_worker.py): worker de reconstruccion con Ollama.
4. [code/cli.py](code/cli.py): cliente de consola.
5. [code/database.py](code/database.py): modelos y acceso DB.
6. [config/k8s/](config/k8s/): manifests para Kubernetes.
7. [config/docker/](config/docker/): Dockerfiles por servicio.
8. [docker-compose.yaml](docker-compose.yaml): stack local completo.

## Endpoints de la API

Endpoints actualmente expuestos:
1. `GET /health`
2. `POST /send?phrase=...&num_workers=...`
3. `GET /job/{job_id}`
4. `GET /job/{job_id}/distortions`
5. `GET /job/{job_id}/guesses`
6. `GET /jobs`

## Flujo completo (end-to-end)

1. CLI envia frase y cantidad de copias a `POST /send`.
2. API valida limites y crea un `job_id`.
3. API persiste el job en PostgreSQL con estado `processing`.
4. API publica N mensajes individuales en RabbitMQ (`telephone.jobs`).
5. Workers consumen tareas y aplican `add_noise(...)`.
6. Cada distorsion se guarda en `DistortedPhrase` y se re-publica a `telephone.results`.
7. LLM Worker acumula frases por job y procesa batches adaptativos.
8. Por cada batch consulta Ollama y guarda un guess incremental en `Guess`.
9. Cuando se alcanza el total esperado del job, se marca `completed`.
10. CLI consulta estado/detalle/guesses.

## Como escalamos

### Escalado automatico con KEDA (ScaledJob)

En [config/k8s/scaled-jobs.yaml](config/k8s/scaled-jobs.yaml) hay una estrategia de autoscaling por longitud de cola:

1. Trigger RabbitMQ sobre `telephone.jobs`.
2. `mode: QueueLength` y `value: "200"`.
3. `maxReplicaCount: 5`.
4. KEDA crea Jobs de Kubernetes cuando crece la cola.

Resumen practico:
1. Si la cola sube, KEDA lanza mas ejecuciones para drenar backlog.
2. Si baja la cola, deja de crear nuevos jobs.

### Detalles importantes del escalado actual

1. El worker limita su ciclo de vida a un maximo de 20 mensajes por proceso (`MAX_MESSAGES_PER_WORKER = 20`).
2. Esto favorece "pods cortos" que se reciclan, util para evitar consumidores eternos.
3. El LLM worker corre en replica unica por defecto para serializar llamadas a Ollama (tiene lock global).
4. API corre con 2 replicas por default en Kubernetes ([config/k8s/api.yaml](config/k8s/api.yaml)).

## Redundancia y tolerancia a fallas

Estrategia aplicada hoy:
1. La API corre con multiples replicas y Kubernetes reprograma pods caidos automaticamente.
2. El trafico de entrada llega a un Service de tipo LoadBalancer que reparte solicitudes entre replicas sanas de API.
3. Los workers son consumidores desacoplados por cola: si cae un pod, otros siguen drenando `telephone.jobs`.
4. RabbitMQ y PostgreSQL mantienen estado en PVC, por lo que una recreacion de pod no implica perder datos por defecto.
5. El procesamiento asincrono con colas evita perder solicitudes por fallas temporales de un consumidor individual.


## Ejecucion local con Docker Compose

Levanta:
1. PostgreSQL
2. RabbitMQ (+ panel management)
3. Ollama
4. API
5. 3 workers de distorsion
6. LLM worker
7. CLI

Comandos:

```bash
docker compose up --build -d
docker compose ps
```

Servicios relevantes:
1. API: http://localhost:8000
2. RabbitMQ UI: http://localhost:15672
3. Ollama: http://localhost:11434

CLI dentro de compose:

```bash
docker compose run --rm cli
```

Traer el modelo de Ollama en local:

```bash
docker compose exec ollama ollama pull llama3.2:1b
```

## Despliegue en Kubernetes

Orden recomendado de despliegue:

1. Namespace
2. Secrets
3. Infra base (PostgreSQL, RabbitMQ, Ollama)
4. API
5. Worker
6. LLM worker
7. Escalado KEDA

Exposicion externa:
1. La API se publica con Service tipo LoadBalancer en [config/k8s/api.yaml](config/k8s/api.yaml).

Comandos base:

1) Crear namespace y secretos:

```bash
kubectl apply -f config/k8s/namespace.yaml
kubectl apply -f config/k8s/secrets.yaml
```

2) Instalar KEDA:

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

3) Aplicar el resto de los manifiestos en [config/k8s/](config/k8s/):

```bash
kubectl apply -f config/k8s/
```

Traer el modelo de Ollama en Kubernetes:

```bash
kubectl -n telephone-distortion exec deploy/ollama-deployment -- ollama pull llama3.2:1b
```


Verificacion:

```bash
kubectl -n telephone-distortion get pods
kubectl -n telephone-distortion get svc
kubectl -n telephone-distortion logs deploy/telephone-api-deployment --tail=100
kubectl -n telephone-distortion logs deploy/telephone-llm-worker-deployment --tail=100
```

## Variables de entorno clave

Compartidas entre servicios:
1. `DATABASE_URL`
2. `RABBITMQ_URL`
3. `OLLAMA_URL`
4. `OLLAMA_MODEL`
5. `LOG_LEVEL`

Adicionales:
1. Worker: `WORKER_ID`
2. LLM worker: `BATCH_TIMEOUT_SEC` 

## Persistencia

En Kubernetes, hay PVCs definidos para:
1. PostgreSQL (`postgres-storage`)
2. RabbitMQ (`rabbitmq-storage`)
3. Ollama (`ollama-storage`)

Esto evita perder estado al reiniciar pods o nodos.






