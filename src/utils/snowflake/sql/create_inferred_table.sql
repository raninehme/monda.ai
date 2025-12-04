CREATE TABLE IF NOT EXISTS {{ database }}.{{ schema }}.{{ table }}
USING TEMPLATE (
    SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
            'COLUMN_NAME', UPPER(COLUMN_NAME),
            'EXPRESSION',  EXPRESSION,
            'TYPE',
                CASE
                {% for col, dtype in column_overrides.items() %}
                    WHEN UPPER(COLUMN_NAME) = '{{ col | upper }}' THEN '{{ dtype }}'
                {% endfor %}
                ELSE TYPE
                END,
            'NULLABLE',    NULLABLE
        )
    )
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION       => '@{{ database }}.{{ schema }}.{{ stage }}/{{ path }}/',
            FILE_FORMAT    => '{{ file_format_ref }}',
            MAX_FILE_COUNT => {{ max_files }}
        )
    )
)
ENABLE_SCHEMA_EVOLUTION = TRUE;
