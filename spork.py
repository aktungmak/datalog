from typing import Dict, List, Tuple, Optional

import pandas as pd
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

Subject = str
Predicate = str
Object = str
MatchPattern = Tuple[Subject, Predicate, Object]
Binding = Tuple[str, str]
SelectResult = list

quad_table_name = "_quads"
kv_string_table_name = "_1"
quad_table_schema: StructType(
    [StructField("subject", LongType(), False),
     StructField("subject_type", LongType(), False),
     StructField("predicate", LongType(), False),
     StructField("predicate_type", LongType(), False),
     StructField("object", LongType(), False),
     StructField("object_type", LongType(), False),
     StructField("graph", LongType(), True),
     StructField("timestamp", TimestampType(), False)])


def kv_schema(value_type) -> StructType:
    return StructType([StructField("key", LongType, False), StructField("value", value_type, False)])


class KG:

    def __init__(self, spark_session, metastore_prefix: Optional[str] = None, path: Optional[str] = None,
                 prefixes: Dict[str, str] = {},
                 property_tables: Dict[str, str] = {}):
        self.ss = spark_session
        self.metastore_prefix = metastore_prefix
        self.path = path
        self.prefixes = prefixes
        self.property_tables = property_tables

    def load(self):
        if self.metastore_prefix is not None:
            return self._load_from_metastore()
        elif self.path is not None:
            return self._load_from_path()
        else:
            raise ValueError('One of metastore_prefix or path must be provided')

    def save(self):
        if self.metastore_prefix is not None:
            return self._save_to_metastore()
        elif self.path is not None:
            return self._save_to_path()
        else:
            raise ValueError('One of metastore_prefix or path must be provided')

    def _load_from_metastore(self):
        if self.ss.catalog.tableExists(self.metastore_prefix+quad_table_name):
            self.quad = self.ss.getTable(self.metastore_prefix+quad_table_name)
        else:
            self.quad = self.ss.createTable(self.metastore_prefix+quad_table_name, source='delta', schema=quad_table_schema)
        if self.ss.catalog.tableExists(self.metastore_prefix+kv_string_table_name):
            self.kv_string = self.ss.getTable(self.metastore_prefix+kv_string_table_name)
        else:
            self.kv_string = self.ss.createTable(self.metastore_prefix+kv_string_table_name, source='delta', schema=kv_schema(StringType))


    def select(self, projection: List[str], matches: List[MatchPattern], optional_clauses: List[MatchPattern] = [],
               bindings: List[Binding] = [],
               distinct: bool = False, limit: int = 1000) -> SelectResult:

        for subject, predicate, object in matches:
            if is_var(subject):
                pass
            else:

        df.select(projection)


def manual():
    matches = [("?id", ":name", "?age")]


def is_var(name: str) -> bool:
    return type(name) is str and name.startswith('?')


def query(
        triples: pd.DataFrame, projection: list, clauses: list, distinct: bool = False
) -> pd.DataFrame:
    for clause in clauses:
        triples = apply_clause(clause, triples)
    # project out relevant columns
    triples = triples.filter(items=projection)
    # extract distince values
    if distinct:
        triples.drop_duplicates(inplace=True)
    return triples


def apply_clause(clause, triples):
    clause_type = clause_type(clause)
    if clause_type == "allvar":
        return allvar(parsed_clause, triples)
    elif clause_type == "mixed":
        return mixed(parsed_clause, triples)
    elif clause_type == "prop_path":
        return prop_path(parsed_clause, triples)
    elif clause_type == "multi_object":
        return multi_object(parsed_clause, triples)
    else:
        assert False


def clause_type(clause: list) -> str:
    if all(map(lambda s: s.startswith("?"), clause)):
        return "allvar"
    elif all(map(lambda x: isinstance(int, x) or isinstance(str, x), clause)):
        return "mixed"
    elif isinstance(list, clause[1]):
        return "prop_path"
