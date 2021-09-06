# Run tests
.PHONY: test
test:
	-docker container rm $$(docker ps -aq) -f
	go test -race -count=1 ./...
