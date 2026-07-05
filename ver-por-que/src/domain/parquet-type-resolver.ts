/**
 * Type display helpers.
 *
 * Enum codes no longer exist on the wire — codec/encoding/physical-type/
 * converted-type all arrive as name strings, so there is nothing to look up.
 * What survives is genuine display logic: pretty-printing the logical-type
 * object and the logical > converted > physical display preference.
 */
import type { LogicalTypeInfo, SchemaLeaf } from '../types';

/**
 * Pretty-print a logical-type object, e.g. DECIMAL(7, 3) or INT(64, unsigned).
 * Returns null when there is no logical type.
 */
export function logicalTypeLabel(lt: LogicalTypeInfo | null | undefined): string | null {
    if (!lt) {
        return null;
    }
    switch (lt.logical_type) {
        case 'DECIMAL':
            return `DECIMAL(${lt.precision}, ${lt.scale})`;
        case 'INTEGER':
            return `INT(${lt.bit_width}, ${lt.is_signed ? 'signed' : 'unsigned'})`;
        case 'TIMESTAMP':
            return `TIMESTAMP(${lt.unit}${lt.is_adjusted_to_utc ? ', UTC' : ''})`;
        case 'TIME':
            return `TIME(${lt.unit}${lt.is_adjusted_to_utc ? ', UTC' : ''})`;
        default:
            return lt.logical_type ?? null;
    }
}

/** Preferred display type for a schema leaf: logical > converted > physical. */
export function displayType(leaf: SchemaLeaf): string {
    return logicalTypeLabel(leaf.logical_type) ?? leaf.converted_type ?? leaf.type;
}
