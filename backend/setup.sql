-- Run this in pgAdmin or psql (as a superuser, usually 'postgres')

-- 1. Create the User (if not exists)
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'user') THEN

      CREATE ROLE "user" WITH LOGIN PASSWORD 'password';
   END IF;
END
$do$;

-- 2. Grant permissions
ALTER ROLE "user" CREATEDB;

-- 3. Create the Database (You might need to run this separately if using a GUI tool)
-- SELECT 'CREATE DATABASE tradeshift'
-- WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'tradeshift')\gexec

-- NOTE: If the above dynamic create doesn't work in your tool, just run:
-- CREATE DATABASE tradeshift OWNER "user";
