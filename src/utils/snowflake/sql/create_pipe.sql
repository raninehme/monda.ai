CREATE PIPE IF NOT EXISTS {{ database }}.{{ schema }}.{{ table }}
AUTO_INGEST = FALSE
AS {{ copy_sql }};
