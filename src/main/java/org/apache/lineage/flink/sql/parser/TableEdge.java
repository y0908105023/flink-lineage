package org.apache.lineage.flink.sql.parser;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableEdge {
    private Table fromTable;
    private Table toTable;
}
