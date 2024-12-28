import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Generator, Any, List

# Maximum number of rows for each RecordBatch chunk.
CHUNK_SIZE = 1000


def parse_parquet_generator(filepath: str) -> Generator[List[Any], None, None]:
    """
    Generator to read a Parquet file by chunks and yield each row as a list of values.

    This function reads a Parquet file by chunks, and for each chunk, it yields each row as a list
    of values. The chunk size is determined by the `CHUNK_SIZE` constant.

    Args:
        filepath: The path to the Parquet file. Can be S3 or local file path.

    Yields:
        A list of values for each row in the Parquet file.

    Example:
        >>> list(parse_parquet_generator('data.parquet'))
        [[1, 'a', True], [2, 'b', False], [3, 'c', True], [4, 'd', False]]
    """
    table = pq.read_table(filepath)
    for batch in table.to_batches(CHUNK_SIZE):
        for row in batch.to_pylist():
            yield row.values()


def prepare_s3_partition_key(
    base_key: str, logical_date: pendulum.DateTime, filename: str = "events.json.gz"
) -> str:
    """
    Generate an S3 partition key based on a base key and a logical date.

    Constructs an S3 key by combining the provided base key with the partitioned
    date and hour derived from the logical date. The format of the key is:
    'base_key/YYYY-MM-DD/HH/events.json.gz'.

    Args:
        base_key: The base path for the S3 key.
        logical_date: The date and time used to generate the partitioned key.
        filename: The name of the file to be stored in S3.

    Returns:
        A string representing the full S3 partition key.
    """
    assert filename.endswith(".json.gz") is True, "filename must end with .json.gz"

    date_partition = logical_date.strftime("%Y-%m-%d")
    hour_partition = logical_date.strftime("%H")
    return f"{base_key}/{date_partition}/{hour_partition}/{filename}"
