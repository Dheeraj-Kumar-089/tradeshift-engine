-- Connect to the specific database first if running interactively,
-- but since we are running via command line flag -d or connection string,
-- we just need the GRANT command.

-- Grant usage and create permissions on the public schema to 'user'
GRANT ALL ON SCHEMA public TO "user";
GRANT ALL PRIVILEGES ON DATABASE tradeshift_db TO "user";

-- If you are on Postgres 15+, the public schema permissions were changed.
-- You explicitly need to grant CREATE on the public schema.
GRANT CREATE ON SCHEMA public TO "user";
