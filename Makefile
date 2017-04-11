SRC_DIR = pilosa/internal
DST_DIR = pilosa/internal

test:
	py.test

cover:
	py.test --cov=pilosa

generate-proto:
	protoc -I=$(SRC_DIR) --python_out=$(DST_DIR) $(SRC_DIR)/internal.proto