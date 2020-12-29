package org.apache.lineage.flink.sql.parser;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Table {

    private String db;
    private String tableName;
    private String columnName;
}
