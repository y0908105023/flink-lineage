package org.apache.lineage.flink.sql.parser;

import lombok.Data;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: james
 * @date: 2020/7/29
 */
@Data
public class SqlParseNode {
    private String name;
    private SqlParseNodeTypeEnum type;
    private List<SqlParseColumn> columnList;
    private String comment;
    private List<String> parent;
    private String sql;
    private Map<String, String> properties;
    private Set<SqlParseNodeActionEnum> actions = new HashSet<>();

    private transient Object calciteSqlNode;
}