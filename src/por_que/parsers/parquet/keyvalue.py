import warnings

from typing import Any, cast

from por_que.file_metadata import KeyValueMetadata

from .base import BaseParser
from .enums import KeyValueFieldId


class KeyValueParser(BaseParser):
    def parse(self) -> KeyValueMetadata:
        start_offset = self.parser.pos
        props: dict[str, Any] = {
            'start_offset': start_offset,
        }
        for field_id, field_type, _value in self.parse_struct_fields():
            match field_id:
                case KeyValueFieldId.KEY:
                    props['key'] = cast(bytes, _value).decode('utf-8')
                case KeyValueFieldId.VALUE:
                    props['value'] = cast(bytes, _value).decode('utf-8')
                case _:
                    warnings.warn(
                        f'Skipping unknown key-value field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        end_offset = self.parser.pos
        props['byte_length'] = end_offset - start_offset

        return KeyValueMetadata(**props)
