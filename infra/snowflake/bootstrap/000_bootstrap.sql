-- Veloparc (fictitious company) · Snowflake bootstrap, run ONCE by a human in a Snowsight worksheet.
-- Everything else is versioned in infra/snowflake/migrations and applied by GitHub Actions (snowflake-deploy).
--
-- What it creates:
--   * resource monitor on the account (cost guard for the trial)
--   * VLP_DEPLOYER role: owns the project objects, can create databases, warehouses, roles and service users
--   * VLP_ADMIN database: authentication policy and the schemachange history
--   * WH_DEPLOY warehouse and the SVC_VELOPARC_CI service user (GitHub Actions)
--   * a programmatic access token (PAT) for SVC_VELOPARC_CI -> copy it once into the GitHub secret SNOWFLAKE_PAT
--
-- Safe to re-run: every statement is IF NOT EXISTS / idempotent, except the last one (a new token each time).

USE ROLE ACCOUNTADMIN;

-- 1. Cost guard: 25 credits per month, notify at 75%, suspend warehouses at 100%.
CREATE RESOURCE MONITOR IF NOT EXISTS VLP_MONTHLY
  WITH CREDIT_QUOTA = 25 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND
           ON 110 PERCENT DO SUSPEND_IMMEDIATE;
ALTER ACCOUNT SET RESOURCE_MONITOR = VLP_MONTHLY;

-- 2. Deployer role (granted to SYSADMIN so admins still see everything it owns).
CREATE ROLE IF NOT EXISTS VLP_DEPLOYER COMMENT = 'Owns Veloparc objects; used by GitHub Actions';
GRANT ROLE VLP_DEPLOYER TO ROLE SYSADMIN;
GRANT CREATE DATABASE, CREATE WAREHOUSE, CREATE ROLE, CREATE USER, EXECUTE TASK ON ACCOUNT TO ROLE VLP_DEPLOYER;

-- 3. Admin database: authentication policy and schemachange history.
CREATE DATABASE IF NOT EXISTS VLP_ADMIN COMMENT = 'Veloparc platform administration';
CREATE SCHEMA IF NOT EXISTS VLP_ADMIN.POLICIES;
CREATE SCHEMA IF NOT EXISTS VLP_ADMIN.SCHEMACHANGE;
GRANT USAGE ON DATABASE VLP_ADMIN TO ROLE VLP_DEPLOYER;
GRANT OWNERSHIP ON SCHEMA VLP_ADMIN.SCHEMACHANGE TO ROLE VLP_DEPLOYER COPY CURRENT GRANTS;

-- GitHub-hosted runners have no fixed IPs: the CI user authenticates with a PAT without a network policy,
-- and with nothing else (no password, no browser login).
CREATE AUTHENTICATION POLICY IF NOT EXISTS VLP_ADMIN.POLICIES.CI_PAT_ONLY
  AUTHENTICATION_METHODS = ('PROGRAMMATIC_ACCESS_TOKEN')
  PAT_POLICY = (NETWORK_POLICY_EVALUATION = ENFORCED_NOT_REQUIRED, MAX_EXPIRY_IN_DAYS = 90)
  COMMENT = 'SVC_VELOPARC_CI: PAT only';

-- 4. Deploy warehouse and CI service user.
CREATE WAREHOUSE IF NOT EXISTS WH_DEPLOY
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE
  COMMENT = 'schemachange and CI checks';
GRANT USAGE, OPERATE ON WAREHOUSE WH_DEPLOY TO ROLE VLP_DEPLOYER;

CREATE USER IF NOT EXISTS SVC_VELOPARC_CI
  TYPE = SERVICE DEFAULT_ROLE = VLP_DEPLOYER DEFAULT_WAREHOUSE = WH_DEPLOY
  COMMENT = 'GitHub Actions (Environment dev)';
GRANT ROLE VLP_DEPLOYER TO USER SVC_VELOPARC_CI;
ALTER USER SVC_VELOPARC_CI SET AUTHENTICATION POLICY VLP_ADMIN.POLICIES.CI_PAT_ONLY;

-- 5. Token for GitHub Actions. The result shows token_secret ONCE: copy it into the GitHub secret SNOWFLAKE_PAT
--    (Settings -> Environments -> dev). Never paste it anywhere else. To rotate: run this statement again with a
--    new name, update the secret, then ALTER USER SVC_VELOPARC_CI REMOVE PROGRAMMATIC ACCESS TOKEN <old name>.
ALTER USER SVC_VELOPARC_CI ADD PROGRAMMATIC ACCESS TOKEN GITHUB_ACTIONS_1
  ROLE_RESTRICTION = 'VLP_DEPLOYER'
  DAYS_TO_EXPIRY = 90
  COMMENT = 'GitHub Actions, Environment dev';
