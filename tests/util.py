PARQUET_BASE_URL = (
    'https://raw.githubusercontent.com/apache/parquet-testing/master/data'
)


def parquet_url(name: str) -> str:
    return f'{PARQUET_BASE_URL}/{name}.parquet'
