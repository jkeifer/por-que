from __future__ import annotations

from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    Discriminator,
    Field,
    PrivateAttr,
)

from .enums import (
    ColumnConvertedType,
    ColumnLogicalType,
    ConvertedType,
    ConvertedTypeName,
    EdgeInterpolationAlgorithmName,
    GroupConvertedType,
    GroupLogicalType,
    ListSemantics,
    LiteralEnumByName,
    LogicalType,
    Repetition,
    RepetitionName,
    SchemaElementType,
    TimeUnit,
    TimeUnitName,
    Type,
    TypeName,
)


class LogicalTypeInfo(BaseModel, frozen=True):
    """Base class for logical type information."""

    logical_type: LogicalType


class StringTypeInfo(LogicalTypeInfo, frozen=True):
    """String logical type."""

    logical_type: Annotated[
        Literal[LogicalType.STRING],
        LiteralEnumByName(LogicalType.STRING),
    ] = LogicalType.STRING


class IntTypeInfo(LogicalTypeInfo, frozen=True):
    """Integer logical type with bit width and signedness."""

    logical_type: Annotated[
        Literal[LogicalType.INTEGER],
        LiteralEnumByName(LogicalType.INTEGER),
    ] = LogicalType.INTEGER
    bit_width: int = 32
    is_signed: bool = True


class DecimalTypeInfo(LogicalTypeInfo, frozen=True):
    """Decimal logical type with scale and precision."""

    logical_type: Annotated[
        Literal[LogicalType.DECIMAL],
        LiteralEnumByName(LogicalType.DECIMAL),
    ] = LogicalType.DECIMAL
    scale: int = 0
    precision: int = 10


class TimeTypeInfo(LogicalTypeInfo, frozen=True):
    """Time logical type with unit and UTC adjustment."""

    logical_type: Annotated[
        Literal[LogicalType.TIME],
        LiteralEnumByName(LogicalType.TIME),
    ] = LogicalType.TIME
    is_adjusted_to_utc: bool = True
    unit: TimeUnitName = TimeUnit.MILLIS


class TimestampTypeInfo(LogicalTypeInfo, frozen=True):
    """Timestamp logical type with unit and UTC adjustment."""

    logical_type: Annotated[
        Literal[LogicalType.TIMESTAMP],
        LiteralEnumByName(LogicalType.TIMESTAMP),
    ] = LogicalType.TIMESTAMP
    is_adjusted_to_utc: bool = True
    unit: TimeUnitName = TimeUnit.MILLIS


class DateTypeInfo(LogicalTypeInfo, frozen=True):
    """Date logical type."""

    logical_type: Annotated[
        Literal[LogicalType.DATE],
        LiteralEnumByName(LogicalType.DATE),
    ] = LogicalType.DATE


class EnumTypeInfo(LogicalTypeInfo, frozen=True):
    """Enum logical type."""

    logical_type: Annotated[
        Literal[LogicalType.ENUM],
        LiteralEnumByName(LogicalType.ENUM),
    ] = LogicalType.ENUM


class JsonTypeInfo(LogicalTypeInfo, frozen=True):
    """JSON logical type."""

    logical_type: Annotated[
        Literal[LogicalType.JSON],
        LiteralEnumByName(LogicalType.JSON),
    ] = LogicalType.JSON


class BsonTypeInfo(LogicalTypeInfo, frozen=True):
    """BSON logical type."""

    logical_type: Annotated[
        Literal[LogicalType.BSON],
        LiteralEnumByName(LogicalType.BSON),
    ] = LogicalType.BSON


class UuidTypeInfo(LogicalTypeInfo, frozen=True):
    """UUID logical type."""

    logical_type: Annotated[
        Literal[LogicalType.UUID],
        LiteralEnumByName(LogicalType.UUID),
    ] = LogicalType.UUID


class Float16TypeInfo(LogicalTypeInfo, frozen=True):
    """Float16 logical type."""

    logical_type: Annotated[
        Literal[LogicalType.FLOAT16],
        LiteralEnumByName(LogicalType.FLOAT16),
    ] = LogicalType.FLOAT16


class MapTypeInfo(LogicalTypeInfo, frozen=True):
    """Map logical type."""

    logical_type: Annotated[
        Literal[LogicalType.MAP],
        LiteralEnumByName(LogicalType.MAP),
    ] = LogicalType.MAP


class ListTypeInfo(LogicalTypeInfo, frozen=True):
    """List logical type."""

    logical_type: Annotated[
        Literal[LogicalType.LIST],
        LiteralEnumByName(LogicalType.LIST),
    ] = LogicalType.LIST


class VariantTypeInfo(LogicalTypeInfo, frozen=True):
    """Variant logical type."""

    logical_type: Annotated[
        Literal[LogicalType.VARIANT],
        LiteralEnumByName(LogicalType.VARIANT),
    ] = LogicalType.VARIANT


class GeometryTypeInfo(LogicalTypeInfo, frozen=True):
    """Geometry logical type."""

    logical_type: Annotated[
        Literal[LogicalType.GEOMETRY],
        LiteralEnumByName(LogicalType.GEOMETRY),
    ] = LogicalType.GEOMETRY


class GeographyTypeInfo(LogicalTypeInfo, frozen=True):
    """Geography logical type."""

    logical_type: Annotated[
        Literal[LogicalType.GEOGRAPHY],
        LiteralEnumByName(LogicalType.GEOGRAPHY),
    ] = LogicalType.GEOGRAPHY
    # Optional in thrift; defaults to SPHERICAL when the writer omits it.
    algorithm: EdgeInterpolationAlgorithmName | None = None


class UnknownTypeInfo(LogicalTypeInfo, frozen=True):
    """Unknown logical type."""

    logical_type: Annotated[
        Literal[LogicalType.UNKNOWN],
        LiteralEnumByName(LogicalType.UNKNOWN),
    ] = LogicalType.UNKNOWN


LogicalTypeInfoUnion = (
    StringTypeInfo
    | IntTypeInfo
    | DecimalTypeInfo
    | TimeTypeInfo
    | TimestampTypeInfo
    | DateTypeInfo
    | EnumTypeInfo
    | JsonTypeInfo
    | BsonTypeInfo
    | UuidTypeInfo
    | Float16TypeInfo
    | MapTypeInfo
    | ListTypeInfo
    | VariantTypeInfo
    | GeometryTypeInfo
    | GeographyTypeInfo
    | UnknownTypeInfo
)

LogicalTypeInfoDiscriminated = Annotated[
    LogicalTypeInfoUnion,
    Discriminator('logical_type'),
]


CONVERTED_TYPE_TO_LOGICAL_TYPE: dict[
    ConvertedType,
    Callable[
        [int | None, int | None],
        LogicalTypeInfo,
    ],
] = {
    ConvertedType.UTF8: lambda _, __: StringTypeInfo(),
    ConvertedType.MAP: lambda _, __: MapTypeInfo(),
    ConvertedType.LIST: lambda _, __: ListTypeInfo(),
    ConvertedType.ENUM: lambda _, __: EnumTypeInfo(),
    ConvertedType.DATE: lambda _, __: DateTypeInfo(),
    ConvertedType.JSON: lambda _, __: JsonTypeInfo(),
    ConvertedType.BSON: lambda _, __: BsonTypeInfo(),
    ConvertedType.TIME_MILLIS: lambda _, __: TimeTypeInfo(unit=TimeUnit.MILLIS),
    ConvertedType.TIME_MICROS: lambda _, __: TimeTypeInfo(unit=TimeUnit.MICROS),
    ConvertedType.TIMESTAMP_MILLIS: (
        lambda _, __: TimestampTypeInfo(unit=TimeUnit.MILLIS)
    ),
    ConvertedType.TIMESTAMP_MICROS: (
        lambda _, __: TimestampTypeInfo(unit=TimeUnit.MICROS)
    ),
    ConvertedType.INT_8: lambda _, __: IntTypeInfo(bit_width=8, is_signed=True),
    ConvertedType.INT_16: lambda _, __: IntTypeInfo(bit_width=16, is_signed=True),
    ConvertedType.INT_32: lambda _, __: IntTypeInfo(bit_width=32, is_signed=True),
    ConvertedType.INT_64: lambda _, __: IntTypeInfo(bit_width=64, is_signed=True),
    ConvertedType.UINT_8: lambda _, __: IntTypeInfo(bit_width=8, is_signed=False),
    ConvertedType.UINT_16: lambda _, __: IntTypeInfo(bit_width=16, is_signed=False),
    ConvertedType.UINT_32: lambda _, __: IntTypeInfo(bit_width=32, is_signed=False),
    ConvertedType.UINT_64: lambda _, __: IntTypeInfo(bit_width=64, is_signed=False),
    ConvertedType.DECIMAL: (
        lambda scale, precision: DecimalTypeInfo(
            scale=scale or 0,
            precision=precision or 10,
        )
    ),
}


class SchemaElement(BaseModel, frozen=True):
    element_type: SchemaElementType
    name: str
    full_path: str
    start_offset: int
    byte_length: int
    # These levels are calculated during schema parsing based on the full path
    definition_level: int = 0
    repetition_level: int = 0

    def _repr_extra(self) -> list[str]:
        return []

    def __repr__(self) -> str:
        extra = self._repr_extra()
        extra_str = f': {" ".join(extra)}' if extra else None
        return f'{self.element_type}({self.name}{extra_str})'

    def get_logical_type(self) -> LogicalTypeInfo | None:
        """Get the logical type, prioritizing logical_type field over converted_type."""
        return self._logical_type_info

    @cached_property
    def _logical_type_info(self) -> LogicalTypeInfo | None:
        logical_type = getattr(self, 'logical_type', None)

        if logical_type is not None:
            return logical_type

        # Fallback to converting converted_type to logical equivalent
        return self._converted_type_to_logical_type(
            getattr(self, 'converted_type', None),
            getattr(self, 'scale', None),
            getattr(self, 'precision', None),
        )

    @staticmethod
    def _converted_type_to_logical_type(
        converted_type: ConvertedType | None,
        scale: int | None = None,
        precision: int | None = None,
    ) -> LogicalTypeInfo | None:
        """Convert a ConvertedType to a LogicalTypeInfo for backward compatibility."""
        if converted_type is None:
            return None

        return CONVERTED_TYPE_TO_LOGICAL_TYPE.get(
            converted_type,
            lambda _, __: None,
        )(scale, precision)

    @staticmethod
    def new(
        start_offset: int,
        byte_length: int,
        name: str | None = None,
        type: Type | None = None,
        type_length: int | None = None,
        repetition: Repetition | None = None,
        num_children: int | None = None,
        converted_type: ConvertedType | None = None,
        scale: int | None = None,
        precision: int | None = None,
        field_id: int | None = None,
        logical_type: LogicalTypeInfoUnion | None = None,
        first: bool = False,
        **kwargs,
    ) -> SchemaRoot | SchemaGroup | SchemaLeaf:
        # The first (depth-0) schema element is always the root record. The
        # parquet spec does not constrain the root's repetition, and real
        # writers vary: some omit it while others (Spark, and every Overture
        # Maps file) set it to REQUIRED. The root never carries a physical,
        # converted, or logical type, so accept it on that shape regardless of
        # repetition rather than second-guessing a healthy file.
        if (
            first
            and name
            and num_children is not None
            and type is None
            and converted_type is None
            and logical_type is None
        ):
            return SchemaRoot(
                name=name,
                num_children=num_children,
                start_offset=start_offset,
                byte_length=byte_length,
                **kwargs,
            )

        # Check type compatibility for column/leaf element
        is_column_converted_type = (
            converted_type is None or converted_type in ColumnConvertedType
        )
        is_column_logical_type = (
            logical_type is None or logical_type.logical_type in ColumnLogicalType
        )

        if (
            name
            and num_children is None
            and repetition is not None
            and type is not None
            and is_column_converted_type
            and is_column_logical_type
        ):
            return SchemaLeaf(
                name=name,
                type=type,
                type_length=type_length,
                repetition=repetition,
                converted_type=converted_type,
                scale=scale,
                precision=precision,
                field_id=field_id,
                logical_type=logical_type,
                start_offset=start_offset,
                byte_length=byte_length,
                # Levels will be calculated later during schema tree building
                definition_level=0,
                repetition_level=0,
                **kwargs,
            )

        # Check type compatibility for group element
        is_group_converted_type = (
            converted_type is None or converted_type in GroupConvertedType
        )
        is_group_logical_type = (
            logical_type is None or logical_type.logical_type in GroupLogicalType
        )

        if (
            name
            and is_group_converted_type
            and num_children is not None
            and repetition is not None
            and type is None
            and is_group_logical_type
        ):
            return SchemaGroup(
                name=name,
                repetition=repetition,
                num_children=num_children,
                converted_type=converted_type,
                start_offset=start_offset,
                byte_length=byte_length,
                field_id=field_id,
                logical_type=logical_type,
                **kwargs,
            )

        raise ValueError(
            'Could not resolve schema element type for args: '
            f"name='{name}', type='{type}', type_length='{type_length}', "
            f"repetition='{repetition}', num_children='{num_children}', "
            f"converted_type='{converted_type}",
        )


class BaseSchemaGroup(SchemaElement, frozen=True):
    element_type: SchemaElementType = SchemaElementType.GROUP
    num_children: int
    children: dict[str, SchemaGroup | SchemaLeaf] = Field(default_factory=dict)

    def count_leaf_columns(self) -> int:
        """Count all columns (leaves) in this schema element and its children."""
        return sum(
            child.count_leaf_columns() if isinstance(child, BaseSchemaGroup) else 1
            for child in self.children.values()
        )

    def find_element(self, path: str | list[str]) -> SchemaElement:
        """Finds a descendant schema element by its dotted path."""
        not_found = ValueError(f"Schema element for path '{path}' not found")

        if isinstance(path, str):
            path = path.split('.')

        child_name = path.pop(0)
        try:
            child = self.children[child_name]
        except KeyError:
            raise not_found from None

        if path and isinstance(child, BaseSchemaGroup):
            return child.find_element(path)

        if path:
            raise not_found

        return child

    def add_element(
        self,
        element: SchemaGroup | SchemaLeaf,
        path: str | list[str] | None = None,
    ) -> None:
        """Add an element to the schema at the specified dotted path."""
        if path is None:
            path = [element.name]
        elif isinstance(path, str):
            path = path.split('.')

        child_name = path.pop(0)

        if len(path) == 0:
            # Direct child
            self.children[child_name] = element
            return

        # Nested path - find parent group
        try:
            group = self.children[child_name]
        except KeyError:
            raise ValueError('Parent group not found') from None

        if not isinstance(group, BaseSchemaGroup):
            raise ValueError(
                'Found parent group, but it is not a group!',
            ) from None

        group.add_element(element, path)

    def __repr__(self) -> str:
        result = super().__repr__()
        for child in self.children.values():
            result += f'  {child}'
        return result


class SchemaRoot(BaseSchemaGroup, frozen=True):
    element_type: SchemaElementType = SchemaElementType.ROOT
    repetition: RepetitionName = Repetition.REQUIRED


class SchemaGroup(BaseSchemaGroup, frozen=True):
    repetition: RepetitionName
    converted_type: ConvertedTypeName | None
    field_id: int | None = None
    logical_type: LogicalTypeInfoUnion | None = None

    def _repr_extra(self) -> list[str]:
        return [
            self.repetition.name,
            self.converted_type.name if self.converted_type else str(None),
        ]


class SchemaLeaf(SchemaElement, frozen=True):
    element_type: SchemaElementType = SchemaElementType.COLUMN
    type: TypeName
    repetition: RepetitionName
    converted_type: ConvertedTypeName | None
    type_length: int | None = None
    scale: int | None = None
    precision: int | None = None
    field_id: int | None = None
    logical_type: LogicalTypeInfoUnion | None = None
    # Semantic interpretation for repeated field handling
    # (set during tree reconstruction)
    list_semantics: ListSemantics | None = None

    def _repr_extra(self) -> list[str]:
        return [
            self.repetition.name,
            self.converted_type.name if self.converted_type else str(None),
            self.type.name,
        ]

    def bytes_to_physical_type(self, value: bytes) -> Any:
        from .parsers.physical_types import parse_bytes

        return parse_bytes(
            value,
            self.type,
        )

    def physical_to_logical_type(self, value: Any) -> Any:
        from .parsers.logical_types import convert_single_value

        if value is None:
            return None

        return convert_single_value(
            value,
            self.type,
            self.get_logical_type(),
        )

    def bytes_to_logical_type(self, value: bytes) -> Any:
        """Convert raw bytes (e.g. statistics values) to the logical value."""
        return self.physical_to_logical_type(self.bytes_to_physical_type(value))


class SchemaLinked(BaseModel, frozen=True):
    """Mixin for models that reference their column's ``SchemaLeaf``.

    The reference is serialized as a string key (``schema_path``) and the
    resolved leaf object is held in a private attribute that is linked at
    parse time via :meth:`_link`. Deserializing a model does not restore the
    link; callers must re-link against a schema root.

    Subclasses expose ``schema_path`` either as a serialized field or as a
    property (e.g. ``ColumnMetadata`` reuses its existing ``path_in_schema``).
    """

    _schema_element: SchemaLeaf | None = PrivateAttr(default=None)

    if TYPE_CHECKING:
        # Provided by subclasses as a serialized field or a property.
        @property
        def schema_path(self) -> str: ...

    @property
    def schema_element(self) -> SchemaLeaf:
        if self._schema_element is None:
            raise ValueError(
                f'This {type(self).__name__} is not linked to its schema '
                'element. Parse via FileMetadata.from_reader / '
                'ParquetFile.from_reader, or link it against a schema root '
                'before accessing schema_element.',
            )
        return self._schema_element

    def link(self, leaf: SchemaLeaf) -> Self:
        """Link this model to its schema leaf, validating the path matches."""
        if leaf.full_path != self.schema_path:
            raise ValueError(
                f'Cannot link {type(self).__name__} with schema_path '
                f'{self.schema_path!r} to schema leaf with full_path '
                f'{leaf.full_path!r}',
            )
        # These models are frozen, so bypass the normal setattr guard to
        # populate the private reference.
        object.__setattr__(self, '_schema_element', leaf)
        return self

    # Parsers link at construction time via this alias; kept private since
    # it's an implementation detail of the parse path rather than public API.
    _link = link
