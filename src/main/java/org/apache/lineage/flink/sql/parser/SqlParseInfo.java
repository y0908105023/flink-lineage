package org.apache.lineage.flink.sql.parser;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SqlParseInfo {

    private Map<String, SqlParseNode> nodeMap;
    private List<String> sinks;
}
