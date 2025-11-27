.PHONY: up down build logs1 logs2 logs3 logs4 logs5 stop1 stop2 stop3 stop4 stop5

up:
	@docker build -t tp-health-checker:latest -f Dockerfile ..
	@./run_ring.sh

down:
	@docker kill hc1_container hc2_container hc3_container hc4_container hc5_container 2>/dev/null || true
	@docker rm -f hc1_container hc2_container hc3_container hc3_container hc4_container hc5_container 2>/dev/null || true

build:
	@docker build -t tp-health-checker -f Dockerfile ..
logs:
	@for c in hc1_container hc2_container hc3_container hc4_container hc5_container; do \
		docker logs -f $$c & \
	done; \
	wait

logs1:
	@docker logs -f hc1_container

logs2:
	@docker logs -f hc2_container

logs3:
	@docker logs -f hc3_container

logs4:
	@docker logs -f hc4_container

logs5:
	@docker logs -f hc5_container

stop1:
	@docker stop hc1_container

stop2:
	@docker stop hc2_container

stop3:
	@docker stop hc3_container

stop4:
	@docker stop hc4_container

stop5:
	@docker stop hc5_container
