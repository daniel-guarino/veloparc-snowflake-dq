-- Schema for the dbt project object VLP_DP.DBT.VELOPARC (dbt Projects on Snowflake). The `dbt` workflow deploys it
-- as VLP_DEPLOYER (owner of this schema, so it may create the object) and executes it; the models run with the
-- privileges of VLP_TRANSFORMER (dbt/veloparc/dbt_projects_profiles.yml).
CREATE SCHEMA IF NOT EXISTS VLP_DP.DBT COMMENT = 'dbt project objects (code deployed from the repository)';
GRANT USAGE ON SCHEMA VLP_DP.DBT TO ROLE VLP_TRANSFORMER;
