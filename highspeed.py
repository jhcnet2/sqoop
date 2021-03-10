import sys

import threading
import requests
import datetime
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hi").getOrCreate()


def getSqlResult(sql):
    #querymodulr to spark df
    return sql


def getTablePathInfo(owner,table,partitionSearchStr):
    sql1 = """
    /*BLOCK Information*/
    SELECT
    ROW_NUMBER() OVER (ORDER BY O.DATA_OBJECT_ID, E.RELATIVE_FNO, E.BLOCK_ID) AS RNUM
    O.DATA_OBJECT_ID,
    E.RELATIVE_FNO,
    E.BLOCK_ID AS START_BLOCK_ID
    E.BLOCK_ID + E.BLOCKS -1 AS END_BLOCK_ID,
    E.BLOCKS,
    E.PARTITION_NAME,
    FROM DBA_OBJECTS O
    INNER JOIN DBA_EXTENTS E ON O.OWNER = E.OWNER AND O.OBJECT_NAME = E.PARTITION_NAME
    """
    sql2 = "                                      AND O.SUBOBJECT_NAME = E.PARTITION_NAME "
    sql3 = " WHERE O.OWNER = '{0}' AND O.OBJECT_NAME = '{1}' "
    sql4 = " AND O.SUBOBJECT_NAME LIKE '%{2}%' "
    sql5 = "ORDER BY RNUM"
    
    sql = None
    if partitionSearchStr is None:
        sql = '\n'.join([sql1,sql3,sql5])
    else:
        sql = '\n'.join([sql1,sql2,sql3,sql4,sql5])
    sql = sql.format(owner.upper(), table.upper(), partitionSearchStr)
    
    df = df.withColumn("RNUM", df["RNUM"].cast("int"))
    


    #sql =
    #    "SELECT data_object_id, "
    #      + "file_id, "
    #      + "relative_fno, "
    #      + "file_batch, "
    #      + "MIN (start_block_id) start_block_id, "
    #      + "MAX (end_block_id) end_block_id, "
    #      + "SUM (blocks) blocks "
    #      + "FROM (SELECT o.data_object_id, "
    #      + "e.file_id, "
    #      + "e.relative_fno, "
    #      + "e.block_id start_block_id, "
    #      + "e.block_id + e.blocks - 1 end_block_id, "
    #      + "e.blocks, "
    #      + "CEIL ( "
    #      + "   SUM ( "
    #      + "      e.blocks) "
    #      + "   OVER (PARTITION BY o.data_object_id, e.file_id "
    #      + "         ORDER BY e.block_id ASC) "
    #      + "   / (SUM (e.blocks) "
    #      + "         OVER (PARTITION BY o.data_object_id, e.file_id) "
    #      + "      / :numchunks)) "
    #      + "   file_batch "
    #      + "FROM dba_extents e, dba_objects o, dba_tab_subpartitions tsp "
    #      + "WHERE     o.owner = :owner "
    #      + "AND o.object_name = :object_name "
    #      + "AND e.owner = :owner "
    #      + "AND e.segment_name = :object_name "
    #      + "AND o.owner = e.owner "
    #      + "AND o.object_name = e.segment_name "
    #      + "AND (o.subobject_name = e.partition_name "
    #      + "     OR (o.subobject_name IS NULL AND e.partition_name IS NULL)) "
    #      + "AND o.owner = tsp.table_owner(+) "
    #      + "AND o.object_name = tsp.table_name(+) "
    #      + "AND o.subobject_name = tsp.subpartition_name(+) ";

    #if (partitionList != null && partitionList.size() > 0) {
    #  sql +=
    #      " AND case when o.object_type='TABLE SUBPARTITION' then "
    #      + "tsp.partition_name else o.subobject_name end IN ("
    #          + getPartitionBindVars(partitionList) + ") ";
    #}

    #sql +=
    #    ") " + "GROUP BY data_object_id, " + "         file_id, "
    #        + "         relative_fno, " + "         file_batch "
    #        + "ORDER BY data_object_id, " + "         file_id, "
    #        + "         relative_fno, " + "         file_batch"