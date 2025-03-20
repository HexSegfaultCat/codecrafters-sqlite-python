from dataclasses import dataclass
from enum import Enum

from .record import SerialType, parse_records
from .utils import BytesOffsetArray


class SchemaObjectType(Enum):
    TABLE = "table"
    INDEX = "index"
    VIEW = "view"
    TRIGGER = "trigger"


@dataclass
class SchemaTable:
    type: SchemaObjectType
    name: str
    tbl_name: str
    root_page: int | None
    sql: str

    @staticmethod
    def from_payload(payload: BytesOffsetArray, encoding: str):
        object_type, object_name, table_name, root_page, sql, *rest = parse_records(
            payload
        )
        if (
            len(rest) > 0
            or object_type.type != SerialType.STRING
            or object_name.type != SerialType.STRING
            or table_name.type != SerialType.STRING
            or root_page.type != SerialType.INT8
            or sql.type != SerialType.STRING
        ):
            raise ValueError("Schema is corrupted")

        object_type = SchemaObjectType(object_type.data.decode("ascii"))
        root_page = int.from_bytes(root_page.data, byteorder="big", signed=False)

        return SchemaTable(
            type=object_type,
            name=object_name.data.decode(encoding),
            tbl_name=table_name.data.decode(encoding),
            root_page=root_page,
            sql=sql.data.decode(encoding),
        )
