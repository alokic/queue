.PHONY: build
build:		## Builds dispatcher
	@echo "Builiding dispatcher"
	@rm ${GOBIN}/dispatcher  2>/dev/null|| true
	@env GOARCH=amd64 go build -ldflags="${LDFLAGS}" -i -o ${GOBIN}/dispatcher ${CMD_FOLDER}/dispatcher/main.go
