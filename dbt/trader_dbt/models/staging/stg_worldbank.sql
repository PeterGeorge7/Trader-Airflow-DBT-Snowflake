SELECT
    country_code,
    CASE
        WHEN country_name LIKE 'Egypt%' THEN 'Egypt'
        ELSE country_name
    END AS country_name,
    indicator_code,
    indicator_name,
    YEAR :: INT AS YEAR,
    VALUE :: FLOAT AS indicator_value
FROM
    {{ source(
        'trader_raw',
        'world_bank_raw'
    ) }}
