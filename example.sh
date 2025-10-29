#!/bin/sh

echo example 1 using arrow-cat
./rs-docker-containers2arrow-ipc \
	--docker-sock-path ~/docker.sock \
	--docker-conn-timeout 10 |
	arrow-cat |
	tail -3

echo example 2 using sql
./rs-docker-containers2arrow-ipc \
	--docker-sock-path ~/docker.sock \
	--docker-conn-timeout 10 |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'docker_containers' \
	--sql "
		SELECT
			id,
			image,
			command,
			state,
			status,
			created
		FROM docker_containers
		WHERE image NOT LIKE 'f%'
		LIMIT 10
	" |
	rs-arrow-ipc-stream-cat
