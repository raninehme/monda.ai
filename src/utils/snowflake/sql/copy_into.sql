COPY INTO {{ database }}.{{ schema }}.{{ table }}
FROM @{{ database }}.{{ schema }}.{{ stage }}
FILE_FORMAT = '{{ file_format_ref }}'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE'
{% if include_metadata %}
INCLUDE_METADATA = ({{ include_metadata }})
{% endif %}
;
