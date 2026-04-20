SELECT
    country_code,
    indicator_code,
    YEAR,
    indicator_value
FROM
    {{ ref("stg_worldbank") }} AS wb
