.PHONY: up down build logs1 logs2 logs 3 stop1 stop2 stop3

up:
	@./run_ring.sh

down:
	@docker kill hc1_container hc2_container hc3_container
	@docker rm -f hc1_container hc2_container hc3_container 2>/dev/null

build:
	@docker build -t tp-health-checker -f Dockerfile .

logs:
	@docker logs -f $(docker ps -q --filter ancestor=tp-health-checker)

logs1:
	@docker logs -f hc1_container

logs2:
	@docker logs -f hc2_container

logs3:
	@docker logs -f hc3_container

stop1:
	@docker stop hc1_container

stop2:
	@docker stop hc2_container

stop3:
	@docker stop hc3_container
