-- Create temp table
create temporary table import_temp AS select * from {table} limit 0;

-- Copy new data into temp table
COPY {table} FROM '{filepath}' DELIMITER ',' CSV HEADER;

-- Start data upload, replacing duplicates
BEGIN TRANSACTION;

DELETE FROM {table}
WHERE property_url IN (SELECT property_url FROM import_temp);

INSERT INTO {table} SELECT * FROM import_temp;

COMMIT TRANSACTION;
