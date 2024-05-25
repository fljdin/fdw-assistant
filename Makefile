PGDATABASE=etude

copy:
	@echo "-- Drop and create the database"
	@dropdb $(PGDATABASE)
	@createdb $(PGDATABASE)
	@echo "-- Create testdata"
	psql -d $(PGDATABASE) -f testdata.sql
	psql -d $(PGDATABASE) -f tools.sql
	psql -d $(PGDATABASE) -f config.sql
	@echo "-- Perform data copy"
	psql -d $(PGDATABASE) -At -c "SELECT statement FROM tools.newrun() WHERE relname IN ('public.t2'::regclass)" | xargs -I {} psql -d $(PGDATABASE) -c "{}"
	psql -d $(PGDATABASE) -c "SELECT * FROM tools.job"

diff:
	@echo "-- Add new data"
	psql -d $(PGDATABASE) -c "INSERT INTO source.t2 (id, name) SELECT i, 'name' || i FROM generate_series(1001, 2000) i";
	@echo "-- Perform data differential copy"
	psql -d $(PGDATABASE) -At -c "SELECT statement FROM tools.run(1)" | xargs -I {} psql -d $(PGDATABASE) -c "{}"
	psql -d $(PGDATABASE) -c "SELECT * FROM tools.job"
