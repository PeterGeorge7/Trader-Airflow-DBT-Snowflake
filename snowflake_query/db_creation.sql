CREATE database IF NOT EXISTS trader;
USE database trader;
CREATE schema RAW;
CREATE schema staging;
CREATE schema marts;
CREATE schema snapshots;
DROP TABLE IF EXISTS trader.raw.world_bank_raw;
CREATE
OR
ALTER TABLE
    trader.raw.world_bank_raw (
        country_code VARCHAR,
        country_name VARCHAR,
        indicator_code VARCHAR,
        indicator_name VARCHAR,
        YEAR VARCHAR,
        VALUE FLOAT,
        date_retrived VARCHAR
    );
DROP TABLE IF EXISTS trader.raw.exchange_rate;
CREATE
    OR
ALTER TABLE
    trader.raw.exchange_rate (
        country_name VARCHAR,
        country_currency_code VARCHAR,
        usd_exchange_rate VARCHAR,
        date_timestamp VARCHAR,
        DATE VARCHAR,
        source VARCHAR
    );
