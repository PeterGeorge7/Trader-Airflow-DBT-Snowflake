SELECT
    DISTINCT country_code,
    country_name,
    country_currency_code
FROM
    {{ ref("stg_exchange_rate") }} AS ex
