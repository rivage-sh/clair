-- One-time Snowflake setup for the clair integration tests.
--
-- Run this script once, with ACCOUNTADMIN. It makes a warehouse, a database, a
-- role and a service user. The role reaches the CLAIR_CI database only, thus a
-- fault in a test cannot touch your other data.
--
-- Put your public key in the RSA_PUBLIC_KEY line before you run the script. To
-- make a key pair:
--
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out clair_ci_key.p8 -nocrypt
--   openssl rsa -in clair_ci_key.p8 -pubout -out clair_ci_key.pub
--   base64 -i clair_ci_key.p8 | tr -d '\n' | pbcopy   # the GitHub secret
--
-- The public key goes in this script. The private key goes in the GitHub
-- secret CLAIR_CI_SNOWFLAKE_PRIVATE_KEY_BASE64. See tests/integration/README.md.

USE ROLE ACCOUNTADMIN;

-- A small warehouse. It suspends after 60 seconds, thus an interrupted job
-- costs almost nothing.
CREATE WAREHOUSE IF NOT EXISTS CLAIR_CI_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- A credit limit. The repository is public, thus a limit protects the account
-- if a workflow runs more often than you expect.
CREATE RESOURCE MONITOR IF NOT EXISTS CLAIR_CI_MONITOR
    WITH CREDIT_QUOTA = 5
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 100 PERCENT DO SUSPEND
             ON 110 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE CLAIR_CI_WH SET RESOURCE_MONITOR = CLAIR_CI_MONITOR;

-- The database name must be CLAIR_CI. A SOURCE Trouve never routes, and the
-- database directory of tests/integration/pipeline_project gives this name.
CREATE DATABASE IF NOT EXISTS CLAIR_CI;

CREATE ROLE IF NOT EXISTS CLAIR_CI_ROLE;

GRANT USAGE, OPERATE ON WAREHOUSE CLAIR_CI_WH TO ROLE CLAIR_CI_ROLE;
GRANT USAGE ON DATABASE CLAIR_CI TO ROLE CLAIR_CI_ROLE;

-- The role makes the schemas of each run, thus it owns them and it can drop
-- them. It gets no grant on another database.
GRANT CREATE SCHEMA ON DATABASE CLAIR_CI TO ROLE CLAIR_CI_ROLE;

-- A service user cannot log in with a password, and it cannot use the web
-- interface. It uses the key pair only.
CREATE USER IF NOT EXISTS CLAIR_CI_USER
    TYPE = SERVICE
    DEFAULT_ROLE = CLAIR_CI_ROLE
    DEFAULT_WAREHOUSE = CLAIR_CI_WH
    COMMENT = 'The service user of the clair integration tests in GitHub Actions.'
    RSA_PUBLIC_KEY = 'PUT-THE-CONTENT-OF-clair_ci_key.pub-HERE-WITHOUT-THE-BEGIN-AND-END-LINES';

GRANT ROLE CLAIR_CI_ROLE TO USER CLAIR_CI_USER;

-- The values of the GitHub secrets:
--   CLAIR_CI_SNOWFLAKE_ACCOUNT    -- the next query gives it
--   CLAIR_CI_SNOWFLAKE_USER       -- CLAIR_CI_USER
--   CLAIR_CI_SNOWFLAKE_ROLE       -- CLAIR_CI_ROLE
--   CLAIR_CI_SNOWFLAKE_WAREHOUSE  -- CLAIR_CI_WH
SELECT concat(current_organization_name(), '-', current_account_name()) AS account;
