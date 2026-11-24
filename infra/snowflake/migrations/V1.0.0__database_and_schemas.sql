-- Veloparc data products database (fictitious company). Owner: VLP_DEPLOYER.
CREATE DATABASE IF NOT EXISTS VLP_DP COMMENT = 'Veloparc Sales & Finance data products (fictitious)';

CREATE SCHEMA IF NOT EXISTS VLP_DP.BRONZE COMMENT = 'Raw, append-only: RAW_* tables, landing stages, quarantine';
CREATE SCHEMA IF NOT EXISTS VLP_DP.SILVER COMMENT = 'Cleaned and conformed (dbt)';
CREATE SCHEMA IF NOT EXISTS VLP_DP.GOLD COMMENT = 'Star schema (dbt)';
CREATE SCHEMA IF NOT EXISTS VLP_DP.DQ COMMENT = 'Data quality results, scorecard, anomaly ground truth';
CREATE SCHEMA IF NOT EXISTS VLP_DP.REF COMMENT = 'Reference tables: FX_RATE_DAILY, POSTAL_ZONE, COUNTRY_ALIAS';
