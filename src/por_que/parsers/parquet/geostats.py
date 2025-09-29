"""
Column metadata parsing for Parquet column chunks.

Teaching Points:
- Column chunks are the fundamental storage unit in Parquet row groups
- Each chunk contains metadata about compression, encoding, and page locations
- Statistics in column metadata enable query optimization
- Path in schema connects column chunks back to the logical schema structure
"""

from __future__ import annotations

import logging
import warnings

from typing import Any

from por_que.enums import GeospatialType
from por_que.file_metadata import (
    BoundingBox,
    GeospatialStatistics,
)

from .base import BaseParser
from .enums import (
    BoundingBoxFieldId,
    GeospatialStatisticsFieldId,
)

logger = logging.getLogger(__name__)


class GeoStatsParser(BaseParser):
    def read_geo_stats(self) -> GeospatialStatistics:
        props: dict[str, Any] = {}

        for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case GeospatialStatisticsFieldId.BBOX:
                    props['bbox'] = self._parse_bounding_box()
                case GeospatialStatisticsFieldId.GEOSPATIAL_TYPES:
                    props['geospatial_types'] = [
                        GeospatialType(self.read_i32()) for _ in value
                    ]
                case _:
                    warnings.warn(
                        f'Skipping unknown geo stats field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        return GeospatialStatistics(**props)

    def _parse_bounding_box(self) -> BoundingBox:
        props: dict[str, float] = {}

        for field_id, field_type, value in self.parse_struct_fields():
            match field_id:
                case BoundingBoxFieldId.XMIN:
                    props['xmin'] = value
                case BoundingBoxFieldId.XMAX:
                    props['xmax'] = value
                case BoundingBoxFieldId.YMIN:
                    props['ymin'] = value
                case BoundingBoxFieldId.YMAX:
                    props['ymax'] = value
                case BoundingBoxFieldId.ZMIN:
                    props['zmin'] = value
                case BoundingBoxFieldId.ZMAX:
                    props['zmax'] = value
                case BoundingBoxFieldId.MMIN:
                    props['mmin'] = value
                case BoundingBoxFieldId.MMAX:
                    props['mmax'] = value
                case _:
                    warnings.warn(
                        f'Skipping unknown bounding box field ID {field_id}',
                        stacklevel=1,
                    )
                    self.maybe_skip_field(field_type)

        return BoundingBox(**props)
