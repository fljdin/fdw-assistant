export PGDATABASE=test
export PGOPTIONS=--client_min_messages=warning

TESTS := $(wildcard test/*_test.sql)

.PHONY: test
test: $(TESTS)

env:
	@dropdb --if-exists $(PGDATABASE)
	@createdb
	@psql -qf tools.sql
	@psql -qf test/model.sql
	@psql -qc "CREATE EXTENSION pgtap"

$(TESTS): env
	@echo "-- Running tests from $@"
	@psql -qf $@
