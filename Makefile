export PGDATABASE = test
export PGOPTIONS  = --search_path=tools
export PSQL_PAGER =

EXP := $(wildcard expected/*_test.out)
OUT := $(patsubst expected/%,results/%,$(EXP))

.PHONY: test
test: $(OUT)

setup:
	@mkdir -p results
	@dropdb --if-exists $(PGDATABASE)
	@createdb
	@psql -qf tools.sql
	@psql -qf sql/testdata.sql

results/%_test.out: sql/%_test.sql setup
	@echo "-- Running tests from $<"
	@psql -af $< > $@ 2>&1
	@diff -u expected/$*_test.out $@ || true

clean:
	rm -rf results
	dropdb --if-exists $(PGDATABASE)
