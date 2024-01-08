# processors/transform.py

def sanitize_column_names(record):
    """Sanitize column names by replacing spaces with underscores."""
    return {key.replace(' ', '_'): value for key, value in record.items()}
