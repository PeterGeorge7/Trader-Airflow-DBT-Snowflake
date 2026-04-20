SELECT
    CASE
        WHEN ex.country_currency_code = 'EGP' THEN 'EGY'
        WHEN ex.country_currency_code = 'SAR' THEN 'SAU'
        WHEN ex.country_currency_code = 'AED' THEN 'ARE'
        WHEN ex.country_currency_code = 'QAR' THEN 'QAT'
        WHEN ex.country_currency_code = 'KWD' THEN 'KWT'
        ELSE NULL
    END AS country_code,
    ex.country_name,
    ex.country_currency_code,
    ex.usd_exchange_rate :: DECIMAL(
        8,
        3
    ) AS exchange_rate,
    ex.date,
    YEAR(
        ex.date :: DATE
    ) AS YEAR,
    MONTH(
        ex.date :: DATE
    ) AS MONTH,
    DAY(
        ex.date :: DATE
    ) AS DAY
FROM
    {{ source(
        'trader_raw',
        'exchange_rate'
    ) }} AS ex
