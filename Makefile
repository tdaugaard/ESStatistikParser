SRC_FILES=$(shell ls -1 *.go | grep -v _test.go)
BIN="bin/$(shell basename "$(shell pwd)")"

all:
	@mkdir bin/ 2>/dev/null || true
	@go build -v -o "$(BIN)"
	@strip -s "$(BIN)"

clean:
	@go clean
	@rm -f "$(BIN)"

run:
	@go run $(SRC_FILES)

.PHONY: all clean run
