cli:
	go build -mod vendor -o bin/query cmd/query/main.go

docker:
	@make docker-query

docker-query:
	docker build -f Dockerfile.query -t point-in-polygon .

make local:
	@make local-query

# test with:
# curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"latitude":37.616951,"longitude":-122.383747}'
# curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{"latitude":37.616951,"longitude":-122.383747,"is_current":[1]}'

local-query:
	docker run -e PIP_MODE=lambda -e PIP_SPATIAL_DATABASE_URI=sqlite://?dsn=/usr/local/data/arch.db -p 9000:8080 point-in-polygon:latest /main
