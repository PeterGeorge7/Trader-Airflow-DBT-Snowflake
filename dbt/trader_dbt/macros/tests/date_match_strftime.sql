{% test date_match_strftime(
    model,
    column_name,
    format
) %}
{% set regex_pattern = '' %}
{% if format == '%Y-%m-%d' %}
    {% set regex_pattern = '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' %}
    {% elif format == '%Y/%m/%d' %}
    {% set regex_pattern = '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' %}
    {% elif format == '%Y-%m-%d %H:%M:%S' %}
    {% set regex_pattern = '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' %}
{% else %}
    {{ exceptions.raise_compiler_error(
        "Unsupported date format: " ~ format
    ) }}
{% endif %}

WITH validation AS (
    SELECT
        {{ column_name }} AS date_column
    FROM
        {{ model }}
    WHERE
        {{ column_name }} IS NOT NULL
)
SELECT
    *
FROM
    validation
WHERE
    NOT REGEXP_LIKE(
        date_column :: STRING,
        '{{ regex_pattern }}'
    ) {% endtest %}
