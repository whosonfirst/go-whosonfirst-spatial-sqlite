cli:
	go build -mod vendor -o bin/query cmd/query/main.go

docker:
	@make docker-query

docker-query:
	docker build -f Dockerfile.query -t point-in-polygon .
