{% test not_empty(model) %}

SELECT 1
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ model }}
    LIMIT 1
)

{% endtest %}
