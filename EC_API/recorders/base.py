from typing import Protocol, ClassVar
from dataclasses import dataclass

class Recorder(Protocol):
    """
    Recorder handles disk-write operations.
    
    We assume the distination is an append-only log.
    
    """
    def __init__(self): pass
    async def start(self): pass
    async def stop(self): pass
    async def record(self) -> None: pass

@dataclass(frozen=True)
class SQLSchemaTable:
    """
    A universal standard for injecting schema in the Recorder object.
    
    We expect a SQL-based DB schema here.
    """
    table_name: str
    columns: tuple[tuple[str, str, str],...]
    strict: bool = False
    no_rowid: bool = False
    
    _ALLOWED_TYPES: ClassVar[frozenset] = frozenset([
        "ANY", "INTEGER", "REAL", "TEXT", "BLOB"
        ])
    _ALLOWED_DB: ClassVar[frozenset] = frozenset([
        "aiosqlite", "sqlite3", "asyncpg", "psycopg", "pymysql", "mysqlclient"
        ])
    
    def __post_init__(self):   
        normalised_columns = []
        for col in self.columns:
            if len(col) == 2:
                (col_name, col_typ), col_extra = col, ""
            elif len(col) == 3:
                col_name, col_typ, col_extra = col
            else:
                raise ValueError("column must be a 2-tuple or a 3-tuple.")
                
            if not isinstance(col_name, str): 
                raise TypeError(f"column name: {col_name} must be a str.")
                
            if col_typ not in self._ALLOWED_TYPES:
                raise ValueError(f"column name: {col_name} not in the accepted data type: {col_typ}.")
            normalised_columns.append((col_name, col_typ, col_extra))
            
        object.__setattr__(self, "columns", tuple(normalised_columns))
        
    @property
    def column_names(self)->tuple:
        return tuple([name for name, _, _ in self.columns])
    
    def create_query(self) -> str:
        cols = ",\n".join(f"{col} {typ}" + (f" {extra}" if extra else "") for col, typ, extra in self.columns)
        
        strict_cond = "STRICT" if self.strict else ""
        no_rowid_cond = "WITHOUT ROWID" if self.no_rowid else ""           
        optional = " ".join(x for x in (strict_cond, no_rowid_cond) if x)
        
        return f"CREATE TABLE IF NOT EXISTS {self.table_name} (\n {cols}\n)" + (f" {optional}" if optional else "" )
        
    def insert_query(self, db_type: str) -> str:
        col_name = ", ".join([x for x, _, _ in self.columns])
        match db_type:
            case "aiosqlite" | "sqlite3":
                placeholder = ", ".join(["?" for _ in self.columns]) 
            case "asyncpg":
                placeholder = ", ".join([f"${i+1}" for i, _ in enumerate(self.columns)]) 
            case "psycopg" | "pymysql" | "mysqlclient":
                placeholder = ", ".join(["%s" for _ in self.columns]) 
            case _:
                raise ValueError(f"Invalid db_type: {db_type}. Only the following db are supported: {self._ALLOWED_DB}.")
                
        return f"INSERT INTO {self.table_name} ({col_name}) VALUES ({placeholder})"
    
    