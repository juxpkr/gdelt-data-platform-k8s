.PHONY: help cluster-up cluster-down deploy clean status logs-airflow port-forward restart-airflow \
        k3s-up k3s-deploy k3s-deploy-monitoring k3s-down k3s-reset k3s-status k3s-port-forward

# ANSI 색상 코드
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
CYAN := \033[0;36m
NC := \033[0m 

# 기본 변수
CLUSTER_NAME := gdelt-platform
NAMESPACE := airflow

# 도움말
help:
	@echo "=========================================="
	@echo "GDELT Data Platform - Kubernetes"
	@echo "=========================================="
	@echo ""
	@echo "Available commands:"
	@echo "  make cluster-up      - Create Kind cluster and deploy infrastructure"
	@echo "  make cluster-down    - Delete Kind cluster"
	@echo "  make deploy          - Deploy applications (Airflow, etc.)"
	@echo "  make clean           - Clean all resources"
	@echo "  make status          - Check cluster status"
	@echo "  make logs-airflow    - View Airflow logs"
	@echo "  make port-forward    - Port forward Airflow (8080)"
	@echo "  make restart-airflow - Restart Airflow webserver"
	@echo ""

# 클러스터 생성 및 기본 인프라 배포
cluster-up:
	@echo "$(BLUE)[INFO]$(NC) Creating Kind cluster..."
	kind create cluster --name $(CLUSTER_NAME) --config deploy/overlays/dev-kind/kind-config.yaml
	@echo ""
	@echo "$(BLUE)[INFO]$(NC) Waiting for cluster to be ready..."
	kubectl wait --for=condition=ready nodes --all --timeout=300s
	@echo ""
	@echo "$(BLUE)[INFO]$(NC) Installing MetalLB..."
	kubectl apply -f deploy/base/metallb/metallb-install.yaml
	kubectl wait --namespace metallb-system --for=condition=ready pod --selector=app=metallb --timeout=90s
	kubectl apply -f deploy/overlays/dev-kind/metallb-config.yaml
	@echo ""
	@echo "$(BLUE)[INFO]$(NC) Installing Ingress-Nginx..."
	kubectl apply -f deploy/base/ingress/ingress-nginx.yaml
	kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=90s
	@echo ""
	@echo "$(GREEN)[SUCCESS]$(NC) Cluster is ready!"
	@echo ""
	@echo "Next step: make deploy"

# 클러스터 삭제
cluster-down:
	@echo "$(YELLOW)[INFO]$(NC) Deleting Kind cluster..."
	kind delete cluster --name $(CLUSTER_NAME)
	@echo "$(GREEN)[SUCCESS]$(NC) Cluster deleted successfully"

# 애플리케이션 배포 (DB -> MinIO -> Kafka -> Airflow)
deploy:
	@echo "$(BLUE)[INFO]$(NC) Creating namespace..."
	# 네임스페이스가 없으면 만들고, 있으면 넘어가는 명령어
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	@echo ""

	@echo "$(BLUE)[INFO]$(NC) Deploying PostgreSQL..."
	# 초기화 스크립트(ConfigMap) 먼저 등록해야 파드가 마운트 가능
	kubectl apply -f deploy/base/databases/postgres/init-configmap.yaml

	# Postgres 배포
	kubectl apply -f deploy/base/databases/postgres/external-service.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) PostgreSQL deployed."
	@echo ""

	@echo "$(BLUE)[INFO]$(NC) 2. Deploying MinIO..."
	# HDD 연결 (PV/PVC)
	kubectl apply -f deploy/base/storage/minio/minio-storage.yaml -n $(NAMESPACE)
	# MinIO 설치 (Bitnami 14.7.14)
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
	helm upgrade --install minio bitnami/minio \
		--namespace $(NAMESPACE) \
		--version 14.7.14 \
		-f deploy/base/storage/minio/values.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) MinIO deployed."
	@echo ""

	@echo "$(BLUE)[INFO]$(NC) 2. Deploying Kafka..."
	# Helm 대신 직접 만든 설정 파일 적용
	kubectl apply -f deploy/base/messaging/kafka/kafka.yaml
	# Kafka가 뜰 때까지 잠시 대기
	kubectl wait --namespace $(NAMESPACE) --for=condition=ready pod -l app=kafka --timeout=120s
	@echo "$(GREEN)[SUCCESS]$(NC) Kafka deployed."
	@echo ""
	
	@echo "$(BLUE)[INFO]$(NC) Deploying Airflow via Helm..."
	helm repo add apache-airflow https://airflow.apache.org
	helm repo update
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--version 1.14.0 \
		-f deploy/base/orchestration/airflow/values.yaml \
		--debug
	@echo ""
	@echo "$(GREEN)[SUCCESS]$(NC) Airflow deployment triggered!"
	@echo "Check status with: make status"

# 모니터링 (ES + Kibana) 배포
deploy-monitoring:
	@echo "$(BLUE)[INFO]$(NC) Deploying Monitoring Stack (ES + Kibana 7.17.3)..."
	# Elastic 공식 Repo 추가
	helm repo add elastic https://helm.elastic.co
	helm repo update
	
	# Elasticsearch 설치 (7.17.3 버전)
	helm upgrade --install elasticsearch elastic/elasticsearch \
		--version 7.17.3 \
		--namespace monitoring --create-namespace \
		-f deploy/base/monitoring/elasticsearch/values.yaml
	
	# Kibana 설치 (7.17.3 버전)
	helm upgrade --install kibana elastic/kibana \
		--version 7.17.3 \
		--namespace monitoring \
		-f deploy/base/monitoring/kibana/values.yaml
	
	@echo "$(GREEN)[SUCCESS]$(NC) Monitoring stack triggered."
	@echo "Check status: kubectl get pods -n monitoring"

# 전체 정리
clean: cluster-down
	@echo "$(YELLOW)[INFO]$(NC) Cleaning up resources..."
	@echo "$(GREEN)[SUCCESS]$(NC) All resources cleaned"

# 클러스터 상태 확인
status:
	@echo "$(CYAN)[STATUS]$(NC) Cluster Status:"
	@echo ""
	@echo "=== Nodes ==="
	@kubectl get nodes
	@echo ""
	@echo "=== Airflow & Data Pods (Namespace: $(NAMESPACE)) ==="
	@kubectl get pods -n $(NAMESPACE)
	@echo ""
	@echo "=== Monitoring Pods (Namespace: monitoring) ==="
	@kubectl get pods -n monitoring
	@echo ""
	@echo "=== Ingress & Services ==="
	@kubectl get svc -A

# 포트 포워딩 (외부 접속 허용)
port-forward:
	@echo "$(BLUE)[INFO]$(NC) Port forwarding services..."
	@echo "$(CYAN)[INFO]$(NC) Airflow UI: http://100.109.182.57:8080"
	@echo "$(CYAN)[INFO]$(NC) Kibana UI: http://100.109.182.57:5601"
	@echo "$(CYAN)[INFO]$(NC) MinIO Console: http://100.109.182.57:9090"
	
	# 1. Airflow (8080) - 모든 IP에서 접속 허용
	@kubectl port-forward --address 0.0.0.0 -n $(NAMESPACE) svc/airflow-webserver 8080:8080 > /dev/null 2>&1 &
	
	# 2. Kibana (5601)
	@kubectl port-forward --address 0.0.0.0 -n monitoring svc/kibana-kibana 5601:5601 > /dev/null 2>&1 &
	
	# 3. MinIO Console (9001)
	@kubectl port-forward --address 0.0.0.0 -n $(NAMESPACE) svc/minio 9090:9001 > /dev/null 2>&1 &
	
	@echo "$(GREEN)[SUCCESS]$(NC) Forwarding started! Access via your Tailscale IP."

# =============================================================================
# k3s OCI 타깃 (deploy/overlays/k3s-oci)
# =============================================================================

K3S_NS := gdelt
AIRFLOW_CHART_VERSION := 1.20.0

k3s-up:
	@echo "$(BLUE)[INFO]$(NC) Verifying k3s cluster..."
	kubectl wait --for=condition=ready nodes --all --timeout=60s
	@echo "$(BLUE)[INFO]$(NC) Creating namespace..."
	kubectl apply -f deploy/overlays/k3s-oci/namespace.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) Cluster ready. Run: make k3s-deploy"

k3s-deploy:
	@echo "$(BLUE)[INFO]$(NC) 1. Deploying PostgreSQL..."
	kubectl apply -f deploy/overlays/k3s-oci/postgres/postgres.yaml
	kubectl wait --namespace $(K3S_NS) --for=condition=ready pod -l app=postgres --timeout=120s
	@echo "$(GREEN)[SUCCESS]$(NC) PostgreSQL ready."

	@echo "$(BLUE)[INFO]$(NC) 2. Deploying MinIO..."
	kubectl apply -f deploy/overlays/k3s-oci/minio/minio.yaml
	kubectl wait --namespace $(K3S_NS) --for=condition=ready pod -l app=minio --timeout=120s
	kubectl apply -f deploy/overlays/k3s-oci/minio/minio-init-job.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) MinIO ready."

	@echo "$(BLUE)[INFO]$(NC) 3. Deploying Kafka..."
	kubectl apply -f deploy/overlays/k3s-oci/kafka/kafka.yaml
	kubectl wait --namespace $(K3S_NS) --for=condition=ready pod -l app=kafka --timeout=120s
	@echo "$(GREEN)[SUCCESS]$(NC) Kafka ready."

	@echo "$(BLUE)[INFO]$(NC) 4. Deploying Nessie..."
	kubectl apply -f deploy/overlays/k3s-oci/nessie/nessie.yaml
	kubectl wait --namespace $(K3S_NS) --for=condition=ready pod -l app=nessie --timeout=120s
	@echo "$(GREEN)[SUCCESS]$(NC) Nessie ready."

	@echo "$(BLUE)[INFO]$(NC) 5. Deploying Trino..."
	kubectl apply -f deploy/overlays/k3s-oci/trino/trino.yaml
	kubectl wait --namespace $(K3S_NS) --for=condition=ready pod -l app=trino --timeout=120s
	@echo "$(GREEN)[SUCCESS]$(NC) Trino ready."

	@echo "$(BLUE)[INFO]$(NC) 6. Deploying Airflow via Helm..."
	helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
	helm repo update apache-airflow
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace $(K3S_NS) \
		--version $(AIRFLOW_CHART_VERSION) \
		-f deploy/overlays/k3s-oci/airflow/values.yaml \
		--timeout 10m
	@echo "$(GREEN)[SUCCESS]$(NC) Airflow deployment triggered."
	@echo "Check status: make k3s-status"

k3s-deploy-monitoring:
	@echo "$(BLUE)[INFO]$(NC) Deploying Prometheus + Grafana..."
	kubectl apply -f deploy/overlays/k3s-oci/monitoring/prometheus.yaml
	kubectl apply -f deploy/overlays/k3s-oci/monitoring/grafana.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) Monitoring stack deployed."

k3s-status:
	@echo "$(CYAN)[STATUS]$(NC) k3s Cluster (namespace: $(K3S_NS))"
	@echo ""
	@echo "=== Nodes ==="
	@kubectl get nodes
	@echo ""
	@echo "=== Pods ==="
	@kubectl get pods -n $(K3S_NS)
	@echo ""
	@echo "=== PVCs ==="
	@kubectl get pvc -n $(K3S_NS)
	@echo ""
	@echo "=== Services ==="
	@kubectl get svc -n $(K3S_NS)

k3s-port-forward:
	@echo "$(BLUE)[INFO]$(NC) Port forwarding k3s services..."
	@kubectl port-forward --address 0.0.0.0 -n $(K3S_NS) svc/airflow-webserver 8080:8080 > /dev/null 2>&1 &
	@kubectl port-forward --address 0.0.0.0 -n $(K3S_NS) svc/minio 9001:9001 > /dev/null 2>&1 &
	@kubectl port-forward --address 0.0.0.0 -n $(K3S_NS) svc/grafana 3000:3000 > /dev/null 2>&1 &
	@kubectl port-forward --address 0.0.0.0 -n $(K3S_NS) svc/prometheus 9090:9090 > /dev/null 2>&1 &
	@echo "$(GREEN)[SUCCESS]$(NC) Airflow:8080  MinIO Console:9001  Grafana:3000  Prometheus:9090"

k3s-down:
	@echo "$(YELLOW)[INFO]$(NC) Tearing down k3s platform (namespace only, data preserved)..."
	helm uninstall airflow -n $(K3S_NS) 2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/nessie/nessie.yaml 2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/kafka/kafka.yaml 2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/minio/minio-init-job.yaml 2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/minio/minio.yaml 2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/postgres/postgres.yaml 2>/dev/null || true
	kubectl delete -f deploy/overlays/k3s-oci/monitoring/ 2>/dev/null || true
	@echo "$(GREEN)[SUCCESS]$(NC) Platform down. PVCs/data preserved."

k3s-reset:
	@echo "$(RED)[RESET]$(NC) Full reset — deleting all data (PVCs included)..."
	helm uninstall airflow -n $(K3S_NS) 2>/dev/null || true
	kubectl delete namespace $(K3S_NS) --timeout=120s 2>/dev/null || true
	@echo "$(YELLOW)[INFO]$(NC) Namespace deleted. Redeploying..."
	kubectl apply -f deploy/overlays/k3s-oci/namespace.yaml
	@echo "$(GREEN)[SUCCESS]$(NC) Reset complete. Run: make k3s-deploy"
