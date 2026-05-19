.PHONY: stress

stress:
	docker run --rm -v "$(CURDIR)/put.lua:/data/request.lua" haydenjeune/wrk2:latest -t1 -c1 -R200 -d30s --latency -s /data/request.lua http://host.docker.internal:8080
	docker run --rm -v "$(CURDIR)/get.lua:/data/request.lua" haydenjeune/wrk2:latest -t1 -c1 -R200 -d30s --latency -s /data/request.lua http://host.docker.internal:8080