CREATE TABLE IF NOT EXISTS {{ staging_db }}.{{ schema }}.{{ table }} AS
SELECT
    {%- for col in all_columns if col not in exclude_columns %}
    {{ col }}{{ "," if not loop.last or flatten_columns }}
    {%- endfor %}
    {%- if flatten_columns %}
    {%- for flatten in flatten_columns %}
        {%- for field in flatten.fields %}
        {{ field }}{{ "," if not loop.last or (loop.parent is defined and not loop.parent.last) }}
        {%- endfor %}
    {%- endfor %}
    {%- endif %}
FROM {{ raw_db }}.{{ schema }}.{{ table }}
{%- if flatten_columns %}
    {%- for flatten in flatten_columns %}
, LATERAL FLATTEN(input => {{ flatten.column }}) AS {{ flatten.column | lower }}_flat
    {%- endfor %}
{%- endif %}
LIMIT 0;
