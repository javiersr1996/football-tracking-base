{#
  A primary key of more than one column, without pulling dbt_utils in. Three of
  the six dimensions have one, and `unique` on a single column cannot say it.
  Written as a generic test so the key is declared in schema.yml next to the
  columns it is made of, and not hidden in a file of its own.
#}
{% test unique_combination(model, columns) %}

SELECT
    {{ columns | join(', ') }},
    count(*) AS n_rows
FROM {{ model }}
GROUP BY ALL
HAVING count(*) > 1

{% endtest %}
