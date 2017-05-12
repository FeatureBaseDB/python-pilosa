SRC_DIR = pilosa/internal
DST_DIR = pilosa/internal

.PHONY: cover doc generate test test-all

cover:
	py.test --cov=pilosa tests integration_tests

doc:
	cd doc && make html

generate:
	protoc -I=$(SRC_DIR) --python_out=$(DST_DIR) $(SRC_DIR)/public.proto

test:
	py.test tests

test-all:
	py.test tests integration_tests
