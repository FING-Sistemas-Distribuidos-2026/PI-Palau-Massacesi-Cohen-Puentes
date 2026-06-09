# Deploy en Kubernetes (con imagenes en Docker Hub)

Este documento explica como desplegar el proyecto completo en Kubernetes, asumiendo que las imagenes propias del proyecto se publican en Docker Hub (o en otro registry compatible).

## 1. Que imagenes tenes que subir vos

Imagenes propias (obligatorias para el backend):

- API
- Worker
- LLM Worker

Imagen opcional:

- CLI (solo si queres ejecutar el cliente en un Pod dentro del cluster)

Imagenes de terceros (no las construis vos, se consumen directo del registry):

- postgres:15-alpine
- rabbitmq:3.12-management-alpine
- ollama/ollama:latest

## 2. Prerrequisitos

- Cluster Kubernetes funcionando.
- kubectl con contexto correcto.
- Docker instalado para build/push.
- Cuenta en Docker Hub (o registry privado).
- Recomendado: Helm para desplegar PostgreSQL y RabbitMQ con persistencia.

## 3. Build y push de imagenes propias

Ejemplo usando Docker Hub y tags versionadas (recomendado evitar latest en produccion):

```bash
# Parate en la raiz del repo

docker build -f config/docker/Dockerfile.api -t TU_USUARIO/telephone-api:v1 .
docker push TU_USUARIO/telephone-api:v1

docker build -f config/docker/Dockerfile.worker -t TU_USUARIO/telephone-worker:v1 .
docker push TU_USUARIO/telephone-worker:v1

docker build -f config/docker/Dockerfile.llm_worker -t TU_USUARIO/telephone-llm-worker:v1 .
docker push TU_USUARIO/telephone-llm-worker:v1

# Opcional
docker build -f config/docker/Dockerfile.cli -t TU_USUARIO/telephone-cli:v1 .
docker push TU_USUARIO/telephone-cli:v1
```

Si tu registry es privado, crea un secret para pull:

```bash
kubectl create namespace telephone

kubectl create secret docker-registry regcred \
  --namespace telephone \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username=TU_USUARIO \
  --docker-password=TU_PASSWORD_O_TOKEN \
  --docker-email=TU_EMAIL
```

## 4. Namespace y configuracion base

```bash
kubectl create namespace telephone
```

Crear secretos de aplicacion (DB y RabbitMQ):

```bash
kubectl -n telephone create secret generic telephone-secrets \
  --from-literal=POSTGRES_USER=user \
  --from-literal=POSTGRES_PASSWORD=devpassword123 \
  --from-literal=POSTGRES_DB=telephone_db \
  --from-literal=RABBITMQ_USER=guest \
  --from-literal=RABBITMQ_PASSWORD=guest
```

## 5. Desplegar dependencias de infraestructura

### PostgreSQL

Opciones:

- Opcion A: Helm chart (recomendado para persistencia, probes, upgrades).
- Opcion B: Deployment/StatefulSet propio.

Helm ejemplo rapido:

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

helm upgrade --install postgres bitnami/postgresql \
  -n telephone \
  --set auth.username=user \
  --set auth.password=devpassword123 \
  --set auth.database=telephone_db \
  --set primary.persistence.enabled=true \
  --set primary.persistence.size=10Gi
```

### RabbitMQ

Helm ejemplo:

```bash
helm upgrade --install rabbitmq bitnami/rabbitmq \
  -n telephone \
  --set auth.username=guest \
  --set auth.password=guest \
  --set persistence.enabled=true \
  --set persistence.size=8Gi
```

### Ollama

Podrias usar chart si tu equipo ya lo estandarizo, pero con Deployment+Service tambien funciona.

Consideraciones para Ollama:

- Asignar PVC para modelos.
- Definir requests/limits de CPU y memoria.
- Si usas GPU, agregar nodeSelector/tolerations/runtimeClass segun tu cluster.
- Pre-cargar modelo qwen2.5:0.5b en init o job de bootstrap para reducir latencia inicial.

## 6. Desplegar la aplicacion (API, Worker, LLM Worker)

Variables importantes (segun el codigo actual):

- DATABASE_URL
- RABBITMQ_URL
- OLLAMA_URL
- OLLAMA_MODEL
- LOG_LEVEL
- LLM_BATCH_SIZE
- LLM_BATCH_TIMEOUT

Ejemplo de URLs internas dentro del namespace telephone:

- DATABASE_URL=postgresql://user:devpassword123@postgres-postgresql.telephone.svc.cluster.local:5432/telephone_db
- RABBITMQ_URL=amqp://guest:guest@rabbitmq.telephone.svc.cluster.local:5672/%2F
- OLLAMA_URL=http://ollama.telephone.svc.cluster.local:11434

Notas:

- En Kubernetes no uses localhost para hablar entre servicios. Usa el nombre DNS del Service.
- Tu compose del llm-worker tiene OLLAMA_URL con localhost; en Kubernetes hay que cambiarlo al Service de Ollama.

## 7. Exposicion de la API

Opciones:

- LoadBalancer (si tu cluster tiene MetalLB o cloud LB).
- Ingress (recomendado para routing y TLS).

Si queres algo simple y rapido, usa Service tipo LoadBalancer para la API.

## 8. Escalado recomendado

- API: 1-2 replicas inicialmente.
- Worker: escalar horizontalmente (HPA o manual) segun cola en RabbitMQ.
- LLM Worker: empezar en 1 replica; si escalas, valida estrategia de batch y carga sobre Ollama.

## 9. Orden de despliegue recomendado

1. Namespace + Secrets.
2. PostgreSQL + RabbitMQ + Ollama.
3. API.
4. Worker.
5. LLM Worker.
6. Exposicion externa de API (LoadBalancer o Ingress).
7. Prueba end-to-end con CLI local o Pod CLI.

## 10. Validaciones post-deploy

```bash
kubectl -n telephone get pods
kubectl -n telephone get svc
kubectl -n telephone logs deploy/telephone-api
kubectl -n telephone logs deploy/telephone-worker --tail=100
kubectl -n telephone logs deploy/telephone-llm-worker --tail=100
```

Checks funcionales:

- /health de API responde OK.
- API publica mensajes en RabbitMQ.
- Worker consume de telephone.jobs y publica en telephone.results.
- LLM Worker consume resultados y persiste guesses.
- PostgreSQL recibe filas de jobs/distortions/guesses.

## 11. Consideraciones importantes

- Tags inmutables: usa v1, v1.1.0, etc. Evita latest en produccion.
- Rollback: mantener al menos 1 version anterior publicada.
- Persistencia: PostgreSQL, RabbitMQ y modelos de Ollama deben tener PVC.
- Probes: agregar readiness/liveness en API, workers y Ollama.
- Recursos: fijar requests/limits para evitar eviction.
- Seguridad:
  - No hardcodear credenciales en manifests.
  - Usar Secrets y, si aplica, External Secrets/Vault.
  - Correr contenedores como non-root cuando sea posible.
- Observabilidad:
  - Logs centralizados.
  - Metricas (Prometheus/Grafana).
  - Alertas basicas (pod restart, cola creciendo, errores 5xx).

## 12. Errores comunes

- Imagen no encontrada: tag incorrecta o sin permisos de pull.
- CrashLoopBackOff por variables de entorno faltantes.
- LLM Worker sin conexion a Ollama por URL mal configurada.
- API/Worker sin DB por hostname de PostgreSQL incorrecto.
- RabbitMQ sin persistencia y perdida de mensajes tras reinicio.

## 13. Pipeline CI/CD sugerido (resumen)

1. Test y lint.
2. Build de imagenes API/Worker/LLM Worker.
3. Push a Docker Hub con tag de commit + tag semantico.
4. Update de manifests (o values Helm) con nueva tag.
5. Deploy por entorno (dev -> staging -> prod).
6. Smoke test automatico y rollback si falla.

---
