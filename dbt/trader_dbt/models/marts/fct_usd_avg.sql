SELECT
    ex.country_code,
    ROUND(AVG(ex.exchange_rate), 3) :: DECIMAL(
        10,
        3
    ) AS avg_usd_exchange_rate,
    ex.year
FROM
    {{ ref("stg_exchange_rate") }} AS ex
GROUP BY
    ex.country_code,
    ex.year
