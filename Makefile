.PHONY: help up deploy deploy-monitoring deploy-dashboard deploy-metrics-exporter \
        down reset status port-forward logs-api logs-web \
        build-api build-web undeploy-dashboard \
        install-test-deps test-spark test-api test-metrics test \
        smoke-k3s smoke-silver-quality

RED    := \033[0;31m
GREEN  := \033[0;32m
YELLOW := \033[0;33m
BLUE   := \033[0;34m
CYAN   := \033[0;36m
NC     := \033[0m

NS                   := gdelt
AIRFLOW_CHART_VERSION := 1.20.0
DASHBOARD_TAG        := 0.1.0-arm64

help:
	@echo "============================================"
	@echo "  GDELT Data Platform — k3s OCI"
	@echo "============================================"
	@echo ""
	@echo "Setup"
	@echo "  make up                  Verify cluster + create namespace"
	@echo "  make deploy              Deploy full stack (postgres→minio→kafka→nessie→trino→airflow)"
	@echo "  make deploy-monitoring   Deploy Prometheus + Grafana + metrics-exporter"
	@echo "  make deploy-dashboard    Deploy API + Web"
	@echo ""
	@echo "Operations"
	@echo "  make status              Show pods / PVCs / services"
	@echo "  make port-forward        Forward Airflow:8080  MinIO:9001  Grafana:3000  Prometheus:9090"
	@echo "  make down                Tear down platform (data preserved)"
	@echo "  make reset               Full reset including PVCs"
	@echo ""
	@echo "Build"
	@echo "  make build-api           Build + push gdelt-api ARM64 image"
	@echo "  make build-web           Build + push gdelt-web ARM64 image"
	@echo ""
	@echo "Logs"
	@echo "  make logs-api            Tail API logs"
	@echo "  make logs-web            Tail Web logs"

up:
	@echo "$(BLUE)[INFO]$(NC) Verifying k3s cluster..."
	kubectl wait --for=condition=ready nodes --all --timeout=60s
	@echo "$(BLUE)[INFO]$(NC) Creating namespace..."
	kubectl apply -f deploy/overlays/k3s-oci/namespace.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) Cluster ready. Run: make deploy"

deploy:
	@echo "$(BLUE)[INFO]$(NC) 1. Deploying PostgreSQL..."
	kubectl apply -f deploy/overlays/k3s-oci/postgres/postgres.yaml
	kubectl wait --namespace $(NS) --for=condition=ready pod -l app=postgres --timeout=60s
	@echo "$(GREEN)[SUCCESS]$(NC) PostgreSQL ready."

	@echo "$(BLUE)[INFO]$(NC) 2. Deploying MinIO..."
	kubectl apply -f deploy/overlays/k3s-oci/minio/minio.yaml
	kubectl wait --namespace $(NS) --for=condition=ready pod -l app=minio --timeout=60s
	kubectl apply -f deploy/overlays/k3s-oci/minio/minio-init-job.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) MinIO ready."

	@echo "$(BLUE)[INFO]$(NC) 3. Deploying Kafka..."
	kubectl apply -f deploy/overlays/k3s-oci/kafka/kafka.yaml
	kubectl wait --namespace $(NS) --for=condition=ready pod -l app=kafka --timeout=60s
	@echo "$(GREEN)[SUCCESS]$(NC) Kafka ready."

	@echo "$(BLUE)[INFO]$(NC) 4. Deploying Nessie..."
	kubectl apply -f deploy/overlays/k3s-oci/nessie/nessie.yaml
	kubectl wait --namespace $(NS) --for=condition=ready pod -l app=nessie --timeout=60s
	@echo "$(GREEN)[SUCCESS]$(NC) Nessie ready."

	@echo "$(BLUE)[INFO]$(NC) 5. Deploying Trino..."
	kubectl apply -f deploy/overlays/k3s-oci/trino/trino.yaml
	kubectl wait --namespace $(NS) --for=condition=ready pod -l app=trino --timeout=60s
	@echo "$(GREEN)[SUCCESS]$(NC) Trino ready."

	@echo "$(BLUE)[INFO]$(NC) 6. Deploying Airflow via Helm..."
	helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
	helm repo update apache-airflow
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace $(NS) \
		--version $(AIRFLOW_CHART_VERSION) \
		-f deploy/overlays/k3s-oci/airflow/values.yaml \
		--timeout 10m
	@echo "$(GREEN)[SUCCESS]$(NC) Airflow deployed. Check: make status"

deploy-monitoring:
	@echo "$(BLUE)[INFO]$(NC) Deploying Prometheus + Grafana + metrics-exporter..."
	kubectl apply -f deploy/overlays/k3s-oci/monitoring/prometheus.yaml
	kubectl apply -f deploy/overlays/k3s-oci/monitoring/grafana-provisioning.yaml
	kubectl apply -f deploy/overlays/k3s-oci/monitoring/grafana.yaml
	kubectl apply -f deploy/overlays/k3s-oci/monitoring/gdelt-metrics-exporter.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) Monitoring stack deployed."

deploy-dashboard:
	@echo "$(BLUE)[INFO]$(NC) Deploying GDELT dashboard..."
	kubectl apply -f deploy/overlays/k3s-oci/dashboard/api.yaml
	kubectl apply -f deploy/overlays/k3s-oci/dashboard/web.yaml
	kubectl apply -f deploy/overlays/k3s-oci/dashboard/ingress.yaml
	kubectl wait --namespace $(NS) --for=condition=ready pod -l app=gdelt-api --timeout=60s
	kubectl wait --namespace $(NS) --for=condition=ready pod -l app=gdelt-web --timeout=60s
	@echo "$(GREEN)[SUCCESS]$(NC) Dashboard deployed."

undeploy-dashboard:
	kubectl delete -f deploy/overlays/k3s-oci/dashboard/ 2>/dev/null || true
	@echo "$(GREEN)[SUCCESS]$(NC) Dashboard removed."

status:
	@echo "$(CYAN)[STATUS]$(NC) k3s Cluster (namespace: $(NS))"
	@echo ""
	@echo "=== Nodes ==="
	@kubectl get nodes
	@echo ""
	@echo "=== Pods ==="
	@kubectl get pods -n $(NS)
	@echo ""
	@echo "=== PVCs ==="
	@kubectl get pvc -n $(NS)
	@echo ""
	@echo "=== Services ==="
	@kubectl get svc -n $(NS)

port-forward:
	@echo "$(BLUE)[INFO]$(NC) Starting port-forward..."
	@kubectl port-forward --address 0.0.0.0 -n $(NS) svc/airflow-webserver 8080:8080 > /dev/null 2>&1 &
	@kubectl port-forward --address 0.0.0.0 -n $(NS) svc/minio            9001:9001 > /dev/null 2>&1 &
	@kubectl port-forward --address 0.0.0.0 -n $(NS) svc/grafana          3000:3000 > /dev/null 2>&1 &
	@kubectl port-forward --address 0.0.0.0 -n $(NS) svc/prometheus       9090:9090 > /dev/null 2>&1 &
	@echo "$(GREEN)[SUCCESS]$(NC) Airflow:8080  MinIO:9001  Grafana:3000  Prometheus:9090"

down:
	@echo "$(YELLOW)[INFO]$(NC) Tearing down platform (data preserved)..."
	helm uninstall airflow -n $(NS) 2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/monitoring/ 2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/dashboard/  2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/trino/trino.yaml    2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/nessie/nessie.yaml  2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/kafka/kafka.yaml    2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/minio/minio.yaml    2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/postgres/postgres.yaml 2>/dev/null || true
	@echo "$(GREEN)[SUCCESS]$(NC) Platform down. PVCs/data preserved."

reset:
	@echo "$(RED)[RESET]$(NC) Full reset — all data (PVCs included) will be deleted!"
	-helm uninstall airflow -n $(NS) 2>/dev/null
	-kubectl delete pods --all -n $(NS) --force --grace-period=0 2>/dev/null
	-kubectl delete namespace $(NS) --wait=false 2>/dev/null
	@echo "$(YELLOW)[INFO]$(NC) Waiting for namespace to terminate..."
	@until ! kubectl get namespace $(NS) >/dev/null 2>&1; do sleep 2; done
	kubectl apply -f deploy/overlays/k3s-oci/namespace.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) Reset complete. Run: make deploy"

build-api:
	docker buildx build --platform linux/arm64 -t juxpkr/gdelt-api:$(DASHBOARD_TAG) apps/api/ --push
	@echo "$(GREEN)[SUCCESS]$(NC) gdelt-api pushed: juxpkr/gdelt-api:$(DASHBOARD_TAG)"

build-web:
	docker buildx build --platform linux/arm64 -t juxpkr/gdelt-web:$(DASHBOARD_TAG) apps/web/ --push
	@echo "$(GREEN)[SUCCESS]$(NC) gdelt-web pushed: juxpkr/gdelt-web:$(DASHBOARD_TAG)"

logs-api:
	kubectl logs -n $(NS) -l app=gdelt-api --tail=100 -f

logs-web:
	kubectl logs -n $(NS) -l app=gdelt-web --tail=50

# ─── Tests ───────────────────────────────────────────────────────────────────

install-test-deps:
	pip3 install -q -r apps/spark-jobs/requirements-dev.txt
	pip3 install -q -r apps/api/requirements-dev.txt
	pip3 install -q -r apps/metrics-exporter/requirements-dev.txt
	@echo "$(GREEN)[SUCCESS]$(NC) Test dependencies installed."

test-spark:
	cd apps/spark-jobs && python3 -m pytest tests/ -v

test-api:
	cd apps/api && python3 -m pytest tests/ -v

test-metrics:
	cd apps/metrics-exporter && python3 -m pytest tests/ -v

test: test-spark test-api test-metrics
	@echo "$(GREEN)[SUCCESS]$(NC) All tests passed."

MAX_AGE_MINUTES ?= 30

smoke-k3s:
	python3 tools/smoke/check_e2e_batch.py --max-age-minutes $(MAX_AGE_MINUTES)

smoke-silver-quality:
	python3 tools/smoke/check_silver_quality.py
