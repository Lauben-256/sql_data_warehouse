
CREATE DATABASE DataWarehouse;

CREATE OR REPLACE PROCEDURE create_schema(name_of_schema VARCHAR)
LANGUAGE plpgsql
AS $$ 
BEGIN
	EXECUTE 'CREATE SCHEMA ' || quote_ident(name_of_schema);
END;
$$;

-- CREATE OR REPLACE PROCEDURE create_schema(name_of_schema VARCHAR)
-- LANGUAGE plpgsql
-- AS $$ 
-- BEGIN
-- 	EXECUTE 'CREATE SCHEMA IF NOT EXISTS ' || quote_ident(name_of_schema);
-- END;
-- $$;

CALL create_schema('bronze');
CALL create_schema('silver');
CALL create_schema('gold');

DROP SCHEMA name_of_schema;
