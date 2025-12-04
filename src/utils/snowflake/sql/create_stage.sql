CREATE OR ALTER STAGE {{ database }}.{{ schema }}.{{ stage }}
    FILE_FORMAT = (FORMAT_NAME = {{ file_format_ref }})
    COMMENT = 'Internal stage for {{ stage }} ingestion';
