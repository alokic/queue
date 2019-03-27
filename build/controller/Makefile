.PHONY: build
build:		## Builds controller
	@echo "Builiding controller"
	@rm ${GOBIN}/controller  2>/dev/null|| true
	@env GOARCH=amd64 go build -ldflags="${LDFLAGS}" -i -o ${GOBIN}/controller ${CMD_FOLDER}/controller/main.go