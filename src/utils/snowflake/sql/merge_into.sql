MERGE INTO {{ staging_db }}.{{ schema }}.{{ table }} AS tgt
USING (
    SELECT
        {%- for col in all_columns if col not in exclude_columns %}
        {{ col }}{% if not loop.last or flatten_columns %},{% endif %}
        {%- endfor %}

        {# --- flatten fields without parent access --- #}
        {%- if flatten_columns %}
            {%- set flat_fields = [] %}
            {%- for flatten in flatten_columns %}
                {%- for field in flatten.fields %}
                    {%- set _ = flat_fields.append(field) %}
                {%- endfor %}
            {%- endfor %}
            {%- for field in flat_fields %}
            {{ field }}{% if not loop.last %},{% endif %}
            {%- endfor %}
        {%- endif %}

    FROM {{ raw_db }}.{{ schema }}.{{ table }}
    {%- if flatten_columns %}
        {%- for flatten in flatten_columns %}
        , LATERAL FLATTEN(input => {{ flatten.column }}) AS {{ flatten.column | lower }}_flat
        {%- endfor %}
    {%- endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {{ primary_keys | join(", ") }}
        ORDER BY {{ sort_keys | join(", ") }}
    ) = 1
) AS src
ON
    {%- for pk in primary_keys %}
    tgt.{{ pk }} = src.{{ pk }}{{ " AND" if not loop.last }}
    {%- endfor %}
WHEN MATCHED THEN
    UPDATE SET
        {%- set update_cols = (all_columns + flatten_fields) | reject('in', exclude_columns + primary_keys) | list %}
        {%- for col in update_cols %}
        tgt.{{ col }} = src.{{ col }}{% if not loop.last %},{% endif %}
        {%- endfor %}
WHEN NOT MATCHED THEN
    INSERT (
        {%- set insert_cols = (all_columns + flatten_fields) | reject('in', exclude_columns) | list %}
        {%- for col in insert_cols %}
        {{ col }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    )
    VALUES (
        {%- for col in insert_cols %}
        src.{{ col }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    );
