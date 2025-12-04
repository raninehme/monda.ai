{%- for col in new_columns %}
ALTER TABLE {{ staging_db }}.{{ schema }}.{{ table }}
    ADD COLUMN IF NOT EXISTS {{ col.name }} {{ col.type }};
{%- endfor %}
