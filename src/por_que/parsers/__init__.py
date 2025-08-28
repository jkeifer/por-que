"""
Parsing infrastructure for binary format parsing.

This package contains the low-level parsers that handle the binary format parsing
for both Thrift protocol and Parquet metadata structures.
"""

from .parquet.metadata import MetadataParser
from .thrift.parser import ThriftCompactParser, ThriftStructParser

__all__ = [
    'MetadataParser',
    'ThriftCompactParser',
    'ThriftStructParser',
]
