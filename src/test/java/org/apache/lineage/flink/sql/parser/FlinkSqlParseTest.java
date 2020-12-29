package org.apache.lineage.flink.sql.parser;

import lombok.extern.slf4j.Slf4j;
import org.apache.lineage.flink.sql.utils.JsonUtils;
import org.apache.lineage.flink.sql.utils.Sqlutils;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.lineage.flink.sql.utils.Constants.*;

@Slf4j
public class FlinkSqlParseTest {

    @Test
    public void test() throws Exception {
        String sqls = "CREATE TABLE user_behavior ( category_id FLOAT,user_id FLOAT,item_id FLOAT,behavior string,ts TIMESTAMP(6), proctime AS PROCTIME() )" +
                " WITH ( 'connector'='kafka-0.11','format'='json','topic'='USER_BEHAVIOR_STREAMING','properties.bootstrap.servers' = 'local:9092' );" +
                "insert into Sql_1608206607668 select   buy_ts,   item_id,   user_id,   category_id,   sum(cnt) as buy_cnt from(     " +
                "SELECT unix_timestamp(date_format(ts, 'yyyy-MM-dd HH:mm:ss')) as buy_ts,item_id,user_id, category_id,       cast(1 as bigint) as cnt     FROM       user_behavior   ) " +
                "group by   buy_ts,   item_id,   user_id,   category_id order by   buy_cnt desc limit   1;";

        sqls = "CREATE TABLE tmp ( q FLOAT,a DATE,s TIME(0),d TIMESTAMP(6),e DECIMAL(38, 18),w string, proctime AS PROCTIME() ) WITH ( 'connector'='kafka-0.11','format'='json','topic'='TOPICXY1','properties.bootstrap.servers' = 'local:9092' );CREATE TABLE tmp1 ( q FLOAT,a DATE,s TIME(0),d TIMESTAMP(6),e DECIMAL(38, 18),w string, proctime AS PROCTIME() ) WITH ( 'connector'='kafka-0.11','format'='json','topic'='TOPICXY2','properties.bootstrap.servers' = 'localhost:9092' );create view Sql_1607927010362 as select tmp1.q as q,tmp.w as w from tmp inner join tmp1 on tmp.q=tmp1.q";

        sqls = "CREATE TABLE temp ( name string,id string,age FLOAT, proctime AS PROCTIME(), rowtime as TO_TIMESTAMP(name, 'yyyy-MM-dd HH:mm:ss.SSS'), WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND ) WITH ( 'connector'='kafka-0.11','format'='json','topic'='DECIMAL_TEST','properties.bootstrap.servers' = 'localhost:9092' );CREATE TABLE temp1 ( name string,id string,tag string, proctime AS PROCTIME(), rowtime as TO_TIMESTAMP(name, 'yyyy-MM-dd HH:mm:ss.SSS'), WATERMARK FOR rowtime AS rowtime - INTERVAL '5' SECOND ) WITH ( 'connector'='kafka-0.11','format'='json','topic'='DECIMAL_TEST1','properties.bootstrap.servers' = 'localhost:9092' );create view Sql_1596708533601 as select id as user_id,a.tag as name from (select a.id,a.name,age,tag from (select id,name,age from temp as ads) as a join temp1 as b on a.id=b.id ) a group by id,tag";

        sqls = "CREATE TABLE tmp ( q FLOAT,a DATE,s TIME(0),d TIMESTAMP(6),e DECIMAL(38, 18),w string, proctime AS PROCTIME() ) WITH ( 'connector'='kafka-0.11','format'='json','topic'='HJHXY1','properties.bootstrap.servers' = 'local:9092' );create database a;" +
                "CREATE VIEW a.a1 as select q,w,e,a,s,d from tmp as sdfsdf;" +
                "insert into a2 select   q as BB,   w as AA, e as CC,   e as DD,   e as EE from tmp; " +
                "insert into a3 select q,w,a,s,d from a.a1; CREATE VIEW a4 as select q as `Time`,w,q as qq,w as ww,e as ee from tmp";

        FlinkSqlParser parser = FlinkSqlParser.create(sqls);

        SqlParseInfo sqlParseInfo = parser.getSqlParseInfo();

        List<TableEdge> tableEdges = parser.getTableEdges();

        System.out.println(JsonUtils.toJSONString(tableEdges));
    }


}
