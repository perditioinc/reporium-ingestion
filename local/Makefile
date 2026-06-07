# $0 / OSS / local substrate for reporium-ingestion.
# All targets operate ONLY on local containers — never production or live cloud.

COMPOSE := docker compose -f docker-compose.yml
ROOT    := ..
PYTHON  ?= python

export REPORIUM_API_URL := http://localhost:58000
export REPORIUM_API_KEY := local-dev
export INGEST_API_KEY := local-dev
export DATABASE_URL := postgresql://reporium:reporium@localhost:55432/reporium
export OLLAMA_URL := http://localhost:11434
export EMBEDDING_MODEL := stub-embed-384
export GRAPH_SNAPSHOT_LOCAL_PATH := $(abspath $(ROOT)/local/out/knowledge-graph.json)
export PUBSUB_REPO_INGESTED_TOPIC :=

.PHONY: help up down seed embed graph smoke logs ps clean

help:
	@echo "reporium-ingestion local substrate ($$0/OSS/local):"
	@echo "  make up     - build + start all OSS substitutes (--wait for health)"
	@echo "  make seed   - post synthetic repos via the real API client -> stub API"
	@echo "  make embed  - generate embeddings via local stub embedder + pgvector"
	@echo "  make graph  - build + publish the knowledge graph snapshot to a local file"
	@echo "  make smoke  - full up -> exercise path -> assert -> down -v (PASS/FAIL)"
	@echo "  make down   - stop and remove containers + volumes"
	@echo "  make logs   - tail substrate logs"
	@echo "  make ps     - show substrate container status"

up:
	$(COMPOSE) up -d --build --wait
	$(COMPOSE) ps

down:
	$(COMPOSE) down -v --remove-orphans

seed:
	cd $(ROOT) && $(PYTHON) local/seed/seed_repos.py

embed:
	cd $(ROOT) && $(PYTHON) local/seed/embed_via_stub.py

graph:
	cd $(ROOT) && $(PYTHON) scripts/publish_graph_snapshot.py

smoke:
	bash smoke.sh

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

clean: down
	rm -rf $(ROOT)/local/out
