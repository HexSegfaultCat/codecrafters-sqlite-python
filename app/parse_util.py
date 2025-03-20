from typing import cast

import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList, Token
from sqlparse.tokens import Wildcard


def basic_parse_sql(sql: str):
    sql_tokens: list[Token] = [
        token
        for token in cast(list[Token], sqlparse.parse(sql)[0].tokens)
        if not token.is_whitespace and not token.is_newline
    ]

    select_token = token if (token := sql_tokens[0]).value.upper() == "SELECT" else None
    if not select_token:
        raise ValueError("Only SELECT statements allowed")

    columns, count_rows = cast(list[str], []), False
    if (token := sql_tokens[1]).ttype == Wildcard:
        columns = [token.value]
    elif isinstance(token := sql_tokens[1], Identifier):
        columns.append(token.value)
    elif isinstance(tokens := sql_tokens[1], IdentifierList):
        for token in tokens.get_identifiers():
            if not isinstance(token, Identifier):
                raise ValueError(f"Token {token} in selection fields not allowed")
        columns += [token.value for token in tokens.get_identifiers()]
    elif isinstance(function := sql_tokens[1], Function):
        count_rows = True
    else:
        raise ValueError("Expected at least one column or *")

    from_token = token if (token := sql_tokens[2]).value.upper() == "FROM" else None
    if not from_token:
        raise ValueError("Missing FROM statement")

    table_name = token if isinstance(token := sql_tokens[3], Identifier) else None
    if not table_name:
        raise ValueError("Table name is required")

    return columns, count_rows, table_name
