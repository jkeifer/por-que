import pytest

from por_que.enums import TimeUnit
from por_que.exceptions import ThriftParsingError
from por_que.parsers.parquet.schema import SchemaParser
from por_que.parsers.thrift.parser import ThriftCompactParser

# TimeUnit is a Thrift *union* whose arms (MILLIS=1, MICROS=2, NANOS=3) are
# empty structs, so the set field ID *is* the unit -- it is not an integer on
# the parent `unit` field. Regression test for parsing files that annotate
# columns with the TIMESTAMP/TIME logical type (as opposed to the older
# converted-type path). See _parse_time_unit.
#
# Compact-protocol bytes for a TimestampType struct:
#   0x11  field 1, BOOL_TRUE          -> isAdjustedToUTC = true
#   0x1C  field 2, STRUCT (unit union)
#     0xN0..  field N, STRUCT         -> the set arm (N = TimeUnit value)
#       0x00  STOP                    -> empty arm struct
#     0x00  STOP                      -> end of union
#   0x00  STOP                        -> end of TimestampType


def _timestamp_bytes(unit_field_id: int) -> bytes:
    arm_header = (unit_field_id << 4) | 0x0C  # delta<<4 | STRUCT
    return bytes([0x11, 0x1C, arm_header, 0x00, 0x00, 0x00])


@pytest.mark.parametrize(
    ('field_id', 'expected'),
    [(1, TimeUnit.MILLIS), (2, TimeUnit.MICROS), (3, TimeUnit.NANOS)],
)
def test_timestamp_unit_union(field_id: int, expected: TimeUnit) -> None:
    parser = ThriftCompactParser(_timestamp_bytes(field_id), 0)
    info = SchemaParser(parser)._parse_timestamp_type()

    assert info.is_adjusted_to_utc is True
    assert info.unit is expected


def test_time_unit_union_with_no_set_field_raises() -> None:
    # An empty union struct body (immediate STOP) has no set arm.
    parser = ThriftCompactParser(bytes([0x00]), 0)
    with pytest.raises(ThriftParsingError):
        SchemaParser(parser)._parse_time_unit()
