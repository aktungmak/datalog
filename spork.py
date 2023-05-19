from functools import reduce
from typing import Dict, List, Tuple, Optional

import delta
import pyspark.sql
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

Subject = str
Predicate = str
Object = str
MatchPattern = Tuple[Subject, Predicate, Object]
Binding = Tuple[str, str]
SelectResult = list

fact_table_name = "_facts"
kv_string_table_name = "_1"
fact_table_schema = StructType(
    [StructField("subject", LongType(), False),
     StructField("predicate", LongType(), False),
     StructField("object", LongType(), False),
     StructField("graph", LongType(), True),
     StructField("timestamp", TimestampType(), False),
     StructField("subject_type", LongType(), False),
     StructField("predicate_type", LongType(), False),
     StructField("object_type", LongType(), False)])


def kv_schema(value_type) -> StructType:
    return StructType([StructField("key", LongType(), False), StructField("value", value_type(), False)])


class KG:

    def __init__(self, spark_session,
                 metastore_prefix: Optional[str] = None,
                 path: Optional[str] = None,
                 prefixes: Dict[str, str] = {},
                 property_tables: Dict[str, str] = {}):
        self.ss = spark_session
        self.metastore_prefix = metastore_prefix
        self.path = path
        self.prefixes = prefixes
        self.property_tables = property_tables
        self.facts = None

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
        if self.ss.catalog.tableExists(self.metastore_prefix + fact_table_name):
            self.facts = self.ss.table(self.metastore_prefix + fact_table_name)
        else:
            self.facts = self.ss.createDataFrame([], spork.fact_table_schema)
        if self.ss.catalog.tableExists(self.metastore_prefix + kv_string_table_name):
            self.kv_string = self.ss.table(self.metastore_prefix + kv_string_table_name)
        else:
            self.kv_string = self.ss.createDataFrame([], schema=kv_schema(StringType))

    def select(self, projection: List[str], matches: List[MatchPattern],
               optional_clauses: List[MatchPattern] = [],
               bindings: List[Binding] = [],
               distinct: bool = False, limit: int = 1000) -> SelectResult:

        for match in matches:
            mdf = self.facts
            for i, elem in enumerate(match):
                if is_var(elem):
                    mdf = mdf.withColumnRenamed(mdf.columns[i], elem)
                else:
                    # join to kv table based on type
                    kv_table = self.type_to_table(elem)
                    mdf = mdf.join(kv_table, (kv_table['value'] == elem) & (mdf[i] == kv_table['key']))

        df.select(projection)

    def insert(self, facts: list):
        octs = []
        for f in facts:
            if len(f) == 3:
                octs.append(f + [None, 0] + [pytype_to_int(e) for e in f])
            if len(f) == 4:
                octs.append(f + [0] + [pytype_to_int(e) for e in f[:3]])
            if len(f) == 5:
                octs.append(f + [pytype_to_int(e) for e in f[:3]])
        new_df = self.ss.createDataFrame(octs, fact_table_schema)
        # extract the different types by doing a flatmap and filter over the values
        # merge into the KVs
        # semijoin to get the IDs
        # insert into the fact table
        self.facts = self.facts.unionAll(new_df).distinct()


def pytype_to_int(obj):
    if obj is None:
        return -1
    if type(obj) is int:
        return 0
    elif type(obj) is str:
        return 1
    else:
        raise ValueError(f"unknown object type {type(obj)}")


def is_var(name: str) -> bool:
    return type(name) is str and name.startswith('?')


def multiple_join(dfs: list[pyspark.sql.DataFrame]) -> pyspark.sql.DataFrame:
    def join(left, right):
        on = list(set(left.columns) & set(right.columns))
        return left.join(right, on=on, how='inner')

    return reduce(join, dfs)


def create_spark_session():
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return delta.configure_spark_with_delta_pip(builder).getOrCreate()
