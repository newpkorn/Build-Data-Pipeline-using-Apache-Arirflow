# Operations Runbook

This runbook covers two production operations workflows for this stack:

- low-downtime rollout sequencing on a server
- debug sidecar access for lean containers that do not expose a shell

## Rollout Strategy

Use this order when deploying compose changes to reduce unnecessary downtime and avoid restarting stable dependencies first.

### 1. Validate before restart

```bash
docker compose config
docker compose pull
```

If the deployment includes custom Airflow image changes, build before bringing services up:

```bash
docker compose build airflow-init webserver scheduler worker triggerer flower
```

### 2. Update supporting observability services first

These services are isolated from the core pipeline and can be refreshed early:

```bash
docker compose up -d renderer loki promtail grafana portainer mailhog
```

Notes:

- `grafana` may require volume cleanup only if provisioning state is already corrupted
- `portainer` should be updated independently from the Airflow core so container management remains available during most of the rollout

### 3. Update stateful databases only when required

Avoid restarting these unless their image, config, or mounted data paths changed:

```bash
docker compose up -d postgres mysql redis
```

Notes:

- if there is no database-related change, leave them untouched
- database restarts are the most likely source of application-visible downtime

### 4. Run Airflow initialization only when image or bootstrap logic changed

```bash
docker compose up airflow-init
```

Notes:

- use this when the Airflow image changed, bootstrap scripts changed, or schema/bootstrap settings changed
- `airflow-init` is not a long-running service and should complete successfully before the app tier is refreshed

### 5. Refresh the Airflow application tier in dependency order

```bash
docker compose up -d webserver
docker compose up -d scheduler
docker compose up -d worker triggerer flower
```

Why this order:

- `webserver` should be available before scheduler-dependent services are refreshed
- `scheduler` should be stable before workers and triggerer reconnect
- `flower` is the least critical of the Airflow services and can come last with worker services

### 6. Verify service health

```bash
docker compose ps
docker compose logs --tail=100 webserver scheduler worker triggerer flower
docker compose logs --tail=100 grafana portainer
```

Recommended checks:

- Airflow UI responds on `:8080`
- Grafana responds on `:3000/grafana/`
- Portainer responds on `:9000` or `:9443`
- no restart loop in `scheduler`, `worker`, or `grafana`

## Emergency Rollback

If a rollout introduces instability:

1. restore the previous image tag in `docker-compose.yaml`
2. redeploy only the affected services with `docker compose up -d <service>`
3. avoid rolling back databases unless the change explicitly modified schema or data

If the issue is isolated to Grafana provisioning state:

```bash
docker compose rm -sf grafana
docker volume rm build-data-pipeline-using-apache-arirflow_grafana-storage
docker compose up -d grafana
```

## Debug Sidecar Workflow

Use a temporary debug container for lean services that may not contain `bash` or any shell at all.

### When to use a sidecar

Use this workflow for:

- `postgres`
- `mysql`
- `redis`
- `renderer`
- `loki`
- `promtail`

Use the service's native logs and metrics first. Open a sidecar only when you need network reachability tests, DNS checks, HTTP probing, or client tooling.

### Generic network debug sidecar

Start a temporary shell on the same Docker network:

```bash
docker run --rm -it \
  --network build-data-pipeline-using-apache-arirflow_default \
  alpine:3.20 sh
```

Inside the container you can install lightweight tools if needed:

```sh
apk add --no-cache curl bind-tools busybox-extras
```

Useful checks:

```sh
nslookup postgres
nslookup loki
nc -vz mysql 3306
nc -vz redis 6379
curl -I http://grafana:3000/grafana/
curl http://loki:3100/ready
```

### MySQL debug sidecar

```bash
docker run --rm -it \
  --network build-data-pipeline-using-apache-arirflow_default \
  mysql:8.0 \
  mysql -hmysql -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -e 'SHOW DATABASES;'
```

### PostgreSQL debug sidecar

```bash
docker run --rm -it \
  --network build-data-pipeline-using-apache-arirflow_default \
  postgres:13 \
  psql postgresql://"${POSTGRES_USER}:${POSTGRES_PASSWORD}"@postgres/"${POSTGRES_DB}" -c '\l'
```

### Redis debug sidecar

```bash
docker run --rm -it \
  --network build-data-pipeline-using-apache-arirflow_default \
  redis:7 \
  redis-cli -h airflow-redis ping
```

### Loki or Promtail HTTP checks

```bash
docker run --rm -it \
  --network build-data-pipeline-using-apache-arirflow_default \
  curlimages/curl:8.8.0 \
  sh -lc 'curl -sS http://loki:3100/ready && echo'
```

## Portainer Console Guidance

- use Portainer console primarily for the Airflow application tier
- if `bash` fails, retry `sh` or `/bin/sh`
- if neither shell exists, prefer a debug sidecar instead of changing a vendor image just for console access

## Production Guardrails

- do not use `latest` tags in production
- pin image tags or digests before rollout
- prefer immutable redeploys over in-container manual fixes
- treat console access as a diagnostic tool, not a routine operations workflow
