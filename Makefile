PROJECT = assistant
export PGDATABASE = test
export PGOPTIONS  = --search_path=$(PROJECT)
export PSQL_PAGER =

SQL := $(wildcard sql/*_test.sql)
EXP := $(patsubst sql/%_test.sql,expected/%_test.out,$(SQL))
OUT := $(patsubst expected/%,results/%,$(EXP))

.PHONY: test
test: $(OUT)

setup:
	@mkdir -p results
	@dropdb --if-exists $(PGDATABASE)
	@createdb
	@psql -qf fdw-assistant.sql -v INSTALL=$(PROJECT)
	@psql -qf sql/testdata.sql

results/%_test.out: sql/%_test.sql setup
	@echo "-- Running tests from $<"
	@psql -a < $< > $@ 2>&1
	@diff -u expected/$*_test.out $@ || true

clean:
	rm -rf results
	dropdb --if-exists $(PGDATABASE)
