PROJECT = assistant
export PGOPTIONS  = --search_path=$(PROJECT)
export PSQL_PAGER =

SQL := $(wildcard sql/*.sql)
EXP := $(patsubst sql/%.sql,expected/%.out,$(SQL))
OUT := $(patsubst expected/%,results/%,$(EXP))

.PHONY: test
test: $(OUT)

setup:
	@mkdir -p results

define dropdb
	2>/dev/null dropdb --if-exists $(1);
endef

define createdb
	$(call dropdb, $(1))
	createdb $(1)
	psql -d $(1) -qf fdw-assistant.sql -v INSTALL=$(PROJECT)
endef

results/%.out: DB = $(patsubst sql/%.sql,_%,$<)
results/%.out: sql/%.sql setup
	@echo "-- Running tests from $<"
	@$(call createdb, $(DB))
	@psql -d $(DB) -a < $< 2>&1 | \
	  grep -v "PL/pgSQL function .* line [0-9]* at" | \
	  grep -v "CALL _execute" > $@
	@diff -u expected/$*.out $@ || true

clean: DBS = $(patsubst sql/%.sql,_%,$(SQL))
clean:
	@$(foreach db,$(DBS),$(call dropdb,$(db)))
	rm -rf results
