SRC_DIR = pilosa/internal
DST_DIR = pilosa/internal

.PHONY: test test-all cover generate-proto

test:
	py.test tests

test-all:
	py.test tests integration_tests

cover:
	py.test --cov=pilosa tests integration_tests

generate-proto:
	protoc -I=$(SRC_DIR) --python_out=$(DST_DIR) $(SRC_DIR)/internal.proto