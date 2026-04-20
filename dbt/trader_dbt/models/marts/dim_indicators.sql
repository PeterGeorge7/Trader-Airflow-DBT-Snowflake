SELECT
    indicator_code,
    indicator_name
FROM
    {{ ref("stg_worldbank") }}
