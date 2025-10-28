import base64
import json

from decimal import Decimal

BASE64_ENCODE_PREFIX = '*-*-*-||por-que_base64_encoded||-*-*-*>'
DECIMAL_ENCODE_PREFIX = '*-*-*-||por-que_decimal_encoded||-*-*-*>'


class FixtureEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return BASE64_ENCODE_PREFIX + base64.b64encode(o).decode()
        if isinstance(o, Decimal):
            return DECIMAL_ENCODE_PREFIX + str(o)
        if hasattr(o, 'isoformat'):  # datetime, date, time objects
            return o.isoformat()
        return json.JSONEncoder.default(self, o)


class FixtureDecoder(json.JSONDecoder):
    def decode(self, s):  # type: ignore
        # Parse normally first
        obj = super().decode(s)
        # Then post-process
        return self._decode_base64_strings(obj)

    def _decode_base64_strings(self, obj):
        if isinstance(obj, str):
            if obj.startswith(BASE64_ENCODE_PREFIX):
                return base64.b64decode(obj[len(BASE64_ENCODE_PREFIX) :])
            if obj.startswith(DECIMAL_ENCODE_PREFIX):
                return Decimal(obj[len(DECIMAL_ENCODE_PREFIX) :])
        if isinstance(obj, dict):
            return {k: self._decode_base64_strings(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._decode_base64_strings(item) for item in obj]
        return obj
