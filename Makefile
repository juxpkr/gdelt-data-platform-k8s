.PHONY: help cluster-up cluster-down deploy clean status logs-airflow port-forward restart-airflow

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
