SRC_DIR = pilosa/internal
DST_DIR = pilosa/internal

.PHONY: cover generate readme test test-all

cover:
	py.test --cov=pilosa tests integration_tests

generate:
	protoc -I=$(SRC_DIR) --python_out=$(DST_DIR) $(SRC_DIR)/public.proto

publish:
	python setup.py sdist upload

readme: README.rst

README.rst: README.md
	pandoc --from=markdown --to=rst --output=README.rst README.md

test:
	py.test tests

test-all:
	py.test tests integration_tests
