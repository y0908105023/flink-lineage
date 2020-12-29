package org.apache.lineage.flink.sql.parser;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.lineage.flink.sql.utils.JsonUtils;
import org.apache.lineage.flink.sql.utils.Sqlutils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.lineage.flink.sql.utils.Constants.*;


@Slf4j
public class FlinkSqlParser {

    private Map<String, SqlParseNode> nodeMap;
    private List<String> sinks;

    public static SqlParser.Config sqlParserConfig = SqlParser
            .configBuilder()
            .setParserFactory(FlinkSqlParserImpl.FACTORY)
            .setConformance(FlinkSqlConformance.DEFAULT)
            .setLex(Lex.JAVA)
            .setIdentifierMaxLength(256)
            .build();

    private FlinkSqlParser(Map<String, SqlParseNode> nodeMap, List<String> sinks) {
        this.nodeMap = nodeMap;
        this.sinks = sinks;
    }

    public SqlParseInfo getSqlParseInfo() {
        return new SqlParseInfo(nodeMap, sinks);
    }

    public List<TableEdge> getTableEdges() {
        List<TableEdge> tableEdges = new ArrayList<>();
        sinks.forEach(sink -> {
            System.out.println(sink);
            SqlParseNode node = nodeMap.get(sink);

            List<SqlParseColumn> sinkColumnList = node.getColumnList();

            Map<String, String> parentNames = new HashMap<>();
            for (String parentName: node.getParent()) {
                if (parentName.contains(TABLE_ALIAS_SEPARATOR)) {
                    parentNames.put(Sqlutils.combineFullTableName(parentName.split(TABLE_ALIAS_SEPARATOR)[0]), parentName.split(TABLE_ALIAS_SEPARATOR)[1]);
                } else {
                    parentNames.put(Sqlutils.combineFullTableName(parentName), parentName);
                }
            }

            sinkColumnList.forEach(column -> {
                try {
                    String columnOriginFrom = findLineageForColumn(column,
                            nodeMap.values().stream().filter(n -> parentNames.containsKey(n.getName())).collect(Collectors.toList()),
                            new ArrayList<>(nodeMap.values()));

                    log.debug("{}.{} from {}", node.getName(), column.getName(), columnOriginFrom);
                    if (columnOriginFrom.trim().equals("")) {
                        return;
                    }

                    Table targetTable = new Table();
                    targetTable.setColumnName(column.getName());
                    targetTable.setTableName(node.getName());
                    for (String oneFrom: columnOriginFrom.split(MULTY_COLUMN_SEPARATOR)) {
                        if (Arrays.asList(IGNORE_FIELDS).contains(oneFrom.substring(oneFrom.lastIndexOf(TABLE_COLUMN_SEPARATOR) + 1))) {
                            continue;
                        }

                        Table fromTable = new Table();
                        fromTable.setColumnName(oneFrom.substring(oneFrom.lastIndexOf(TABLE_COLUMN_SEPARATOR) + 1));
                        fromTable.setTableName(oneFrom.substring(0, oneFrom.lastIndexOf(TABLE_COLUMN_SEPARATOR)));
                        tableEdges.add(new TableEdge(fromTable, targetTable));
                    }
                } catch (Exception e) {
                    log.error("parse node {} column: {}, select name: {} error.", node.getName(), column.getName(), column.getSelectName());
                    log.error("ERROR MESSAGE: ", e);
                }
            });
        });

        return tableEdges;
    }

    private String findLineageForColumn(SqlParseColumn column, List<SqlParseNode> parendNodeList, List<SqlParseNode> allNodes) {

        final String[] realColumns = column.getParents().split("&");

        String columnFrom = "";
        for (String realColumn : realColumns) {
            String currentParentTableName = Sqlutils.combineFullTableName(realColumn.substring(0, realColumn.lastIndexOf(TABLE_COLUMN_SEPARATOR)));
            String currentParentColumnName = realColumn.substring(realColumn.lastIndexOf(TABLE_COLUMN_SEPARATOR) + 1);
            if (Arrays.asList(IGNORE_FIELDS).contains(currentParentColumnName) || currentParentColumnName.equals("")) {
                continue;
            }
            for (SqlParseNode node : parendNodeList) {
                try {
                    if (node.getName().equals(currentParentTableName)) {
                        SqlParseColumn filteredColumn = node.getColumnList().stream().filter(pColumn -> pColumn.getName().equals(currentParentColumnName)).findFirst().get();
                        // put Map<name, alias>
                        Map<String, String> parentNamesAliasMapping = new HashMap<>();
                        if (node.getParent() != null && node.getParent().size() > 0) {
                            for (String parentName : node.getParent()) {
                                if (parentName.contains(TABLE_ALIAS_SEPARATOR)) {
                                    parentNamesAliasMapping.put(Sqlutils.combineFullTableName(parentName.split(TABLE_ALIAS_SEPARATOR)[0]), parentName.split(TABLE_ALIAS_SEPARATOR)[1]);
                                } else {
                                    parentNamesAliasMapping.put(Sqlutils.combineFullTableName(parentName), parentName);
                                }
                            }
                        }

                        if (parentNamesAliasMapping != null && parentNamesAliasMapping.size() > 0) {
                            columnFrom += findLineageForColumn(filteredColumn, allNodes.stream().filter(n -> parentNamesAliasMapping.containsKey(n.getName())).collect(Collectors.toList()), allNodes);
                        } else {
                            columnFrom += node.getName() + TABLE_COLUMN_SEPARATOR + filteredColumn.getName() + MULTY_COLUMN_SEPARATOR;
                        }
                    }
                } catch (Exception e) {
                    log.warn("find column: {} error: ", column.getName(), e);
                }
            }
        }

        return columnFrom;
    }

    public static FlinkSqlParser create(String sql) throws Exception {
        SqlParser sqlParser = SqlParser.create(sql, sqlParserConfig);
        SqlNodeList sqlNodes = sqlParser.parseStmtList();
        Map<String, SqlParseNode> nodeMap = new HashMap<>();
        List<String> sinks = new ArrayList<>();
        for (SqlNode sqlNode : sqlNodes) {
            String splitSql = sqlNode.toSqlString(SkipAnsiCheckSqlDialect.DEFAULT).getSql();
            log.debug("current sql = " + splitSql);
            if (sqlNode instanceof SqlCreateTable) {
                SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;
                SqlParseNode node = new SqlParseNode();
                node.setSql(splitSql);
                node.setName(Sqlutils.combineFullTableName(sqlCreateTable.getTableName().getSimple()));
                node.setType(SqlParseNodeTypeEnum.TABLE);
                node.getActions().add(SqlParseNodeActionEnum.SOURCE);
                List<SqlParseColumn> sqlParseColumnList = sqlCreateTable.getColumnList().getList().stream().map(c -> {
                    SqlParseColumn sqlParseColumn = new SqlParseColumn();
                    if (c instanceof SqlTableColumn) {
                        SqlTableColumn sqlTableColumn = (SqlTableColumn) c;
                        sqlParseColumn.setName(sqlTableColumn.getName().getSimple());
                        sqlParseColumn.setType(sqlTableColumn.getType().toString());
                        sqlParseColumn.setNullable(sqlTableColumn.getType().getNullable());
                        if (sqlTableColumn.getConstraint().isPresent()) {
                            sqlParseColumn.setConstraint(sqlTableColumn.getConstraint().get().toString());
                        }
                        if (sqlTableColumn.getComment().isPresent()) {
                            sqlParseColumn.setComment(sqlTableColumn.getComment().get().toString());
                        }
                    } else if (c instanceof SqlBasicCall && ((SqlBasicCall) c).getOperator() instanceof SqlAsOperator) {
                        SqlNode[] operands = ((SqlBasicCall) c).getOperands();
                        sqlParseColumn.setName(operands[1].toString());
                        sqlParseColumn.setType(operands[0].toString());
                        sqlParseColumn.setComment(c.toString());
                        sqlParseColumn.setIsPhysical(false);
                    } else {
                        throw new RuntimeException("not support operation: " + c.getClass().getSimpleName());
                    }
                    return sqlParseColumn;
                }).collect(Collectors.toList());
                node.setColumnList(sqlParseColumnList);
                Map<String, String> properties = sqlCreateTable.getPropertyList().getList().stream().map(x -> ((SqlTableOption) x))
                        .collect(Collectors.toMap(SqlTableOption::getKeyString, SqlTableOption::getValueString));
                node.setProperties(properties);
                node.setCalciteSqlNode(sqlNode);
                if (sqlCreateTable.getComment().isPresent()) {
                    node.setComment(sqlCreateTable.getComment().get().toString());
                } else {
                    node.setComment(sqlCreateTable.getTableName().toString());
                }
                nodeMap.put(node.getName(), node);
            } else if (sqlNode instanceof SqlCreateView) {
                SqlCreateView sqlCreateView = (SqlCreateView) sqlNode;
                String viewName = Sqlutils.combineFullTableName(sqlCreateView.getViewName().toString());
                SqlParseNode node = new SqlParseNode();
                node.setSql(splitSql);
                node.setName(viewName);
                node.setComment(viewName);
                node.setType(SqlParseNodeTypeEnum.VIEW);
                node.getActions().add(SqlParseNodeActionEnum.VIEW);
                SqlSelect select = lookupSqlSelect(sqlCreateView.getQuery());
                fillMetaToParseNode(node, select, nodeMap);

            } else if (sqlNode instanceof SqlInsert) {
                SqlInsert sqlInsert = (SqlInsert) sqlNode;
                String sinkTableName = Sqlutils.combineFullTableName(sqlInsert.getTargetTable().toString());
                SqlParseNode node = new SqlParseNode();
                node.setSql(splitSql);
                node.setType(SqlParseNodeTypeEnum.INSERT);
                node.setComment(sinkTableName);
                node.setName(sinkTableName);
                SqlSelect select = lookupSqlSelect(sqlInsert.getSource());
                sinks.add(sinkTableName);
                fillMetaToParseNode(node, select, nodeMap);
            }
        }
        System.out.println(JsonUtils.toJSONString(nodeMap));
        return new FlinkSqlParser(nodeMap, sinks);
    }

    private static void fillMetaToParseNode(SqlParseNode parseNode, SqlNode sqlNode, Map<String, SqlParseNode> nodeMap) {
        AtomicInteger i = new AtomicInteger();
        SqlSelect select = lookupSqlSelect(sqlNode);
        List<SqlParseColumn> sqlParseColumnList = select.getSelectList().getList().stream().map(FlinkSqlParser::getAliasColumn).filter(tc -> !tc.equals("")).map(tableColumn -> {
            SqlParseColumn sqlParseColumn = new SqlParseColumn();
            SqlNode selectNode = select.getSelectList().get(i.get());
            if (selectNode instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) selectNode;
                SqlOperator operator = sqlBasicCall.getOperator();
                String selectName = getSqlIdentifierFromUdf(sqlBasicCall.getOperands()[0]);
                if (SqlKind.AS.equals(operator.getKind())) {
                    sqlParseColumn.setSelectName(selectName);
                    sqlParseColumn.setSelectAlias(getSqlIdentifierFromUdf(sqlBasicCall.getOperands()[1]));
                } else {
                    sqlParseColumn.setSelectName(selectName);
                    sqlParseColumn.setSelectAlias(selectName);
                }
            } else if (selectNode instanceof SqlIdentifier) {
                sqlParseColumn.setSelectName(selectNode.toString());
            } else if (selectNode instanceof SqlCharStringLiteral) {
                sqlParseColumn.setIsConstant(true);
            }

            String columnParent = findFromTableAndColumn(sqlParseColumn.getSelectName(), select.getFrom());
            sqlParseColumn.setParents(columnParent);
            sqlParseColumn.setName(tableColumn);
            if (tableColumn.contains(TABLE_COLUMN_SEPARATOR)) {
                sqlParseColumn.setName(tableColumn.split(TRANSFERRED_TABLE_COLUMN_SEPARATOR)[1]);
            }
            i.addAndGet(1);
            return sqlParseColumn;
        }).collect(Collectors.toList());

        parseNode.setColumnList(sqlParseColumnList);
        parseNode.setCalciteSqlNode(sqlNode);

        nodeMap.put(parseNode.getName(), parseNode);
        List<String> selectTableList = lookupSelectTable(select, "");
        parseNode.setParent(selectTableList);
    }

    private static String getAliasColumn(SqlNode node) {
        if (node instanceof SqlBasicCall) {
            SqlNode[] nodes = ((SqlBasicCall) node).getOperands();
            return getAliasColumn(nodes[nodes.length-1]);
        } else if (node instanceof SqlIdentifier) {
            return node.toString();
        }

        return "";
    }

    private static String findTableNameFromCurrentSqlNode(SqlNode sqlNode) {

        if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlOperator operator = sqlBasicCall.getOperator();
            if (SqlKind.AS.equals(operator.getKind())) {
                return sqlBasicCall.getOperands()[1].toString();
            } else {
                return null;
            }
        } else if (sqlNode instanceof SqlIdentifier) {
            return ((SqlIdentifier) sqlNode).toString();
        }

        return null;
    }

    private static String findParentColumnFromSelectList(List<SqlNode> sqlNodes, String key) {
        for (SqlNode column: sqlNodes) {
            if (column instanceof SqlBasicCall) {
                SqlBasicCall sqlBasicCall = (SqlBasicCall) column;
                SqlOperator operator = sqlBasicCall.getOperator();
                String realColumnName = "";
                if (SqlKind.AS.equals(operator.getKind())) {
                    realColumnName = getSqlIdentifierFromUdf(sqlBasicCall.getOperands()[1]);
                } else {
                    realColumnName = getSqlIdentifierFromUdf(sqlBasicCall.getOperands()[0]);
                }

                if (realColumnName.contains(".")) {
                    realColumnName = realColumnName.split(TRANSFERRED_TABLE_COLUMN_SEPARATOR)[1];
                }
                if (realColumnName.equals(key)) {
                    return column.toString();
                }
            } else if (column instanceof SqlIdentifier) {
                String selectName = column.toString();
                if (selectName.contains(".")) {
                    selectName = selectName.split(TRANSFERRED_TABLE_COLUMN_SEPARATOR)[1];
                }
                if (selectName.equals(key)) {
                    return column.toString();
                }
            }
        }

        return null;
    }

    private static String findFromTableAndColumn(String columnName, SqlNode sqlNode) {

        String fromTableAlias = "";
        if (columnName.contains(".")) {
            fromTableAlias = columnName.split(TRANSFERRED_TABLE_COLUMN_SEPARATOR)[0];
            columnName = columnName.split(TRANSFERRED_TABLE_COLUMN_SEPARATOR)[1];
        }
        if (sqlNode instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) sqlNode;
            SqlNode from = select.getFrom();
            if (from instanceof SqlIdentifier) {
                List<SqlNode> sqlNodes = select.getSelectList().getList();
                for (SqlNode column: sqlNodes) {
                    String realColumn = "";
                    String aliasColumn = "";
                    if (column instanceof SqlBasicCall) {
                        SqlBasicCall sqlBasicCall = (SqlBasicCall) column;
                        SqlOperator operator = sqlBasicCall.getOperator();
                        realColumn = getSqlIdentifierFromUdf(sqlBasicCall.getOperands()[0]);
                        aliasColumn = realColumn;
                        if (SqlKind.AS.equals(operator.getKind())) {
                            aliasColumn = getSqlIdentifierFromUdf(sqlBasicCall.getOperands()[1]);
                        }
                    } else if (column instanceof SqlIdentifier) {
                        realColumn = column.toString();
                        aliasColumn = realColumn;
                    }

                    if (aliasColumn.equals(columnName)) {
                        return ((SqlIdentifier) from).toString() + TABLE_COLUMN_SEPARATOR + realColumn;
                    }
                }
            } else if (from instanceof SqlBasicCall && ((SqlBasicCall) from).getOperands()[0] instanceof SqlIdentifier) {
                List<SqlNode> sqlNodes = select.getSelectList().getList();
                for (SqlNode column: sqlNodes) {
                    String realColumn = "";
                    if (column instanceof SqlBasicCall) {
                        SqlBasicCall sqlBasicCall = (SqlBasicCall) column;
                        realColumn = getSqlIdentifierFromUdf(sqlBasicCall.getOperands()[0]);
                    } else if (column instanceof SqlIdentifier) {
                        realColumn = column.toString();
                    }

                    if (realColumn.equals(columnName)) {
                        return ((SqlIdentifier) ((SqlBasicCall) from).getOperands()[0]).toString() + TABLE_COLUMN_SEPARATOR + realColumn;
                    }
                }
            } else {
                return findFromTableAndColumn(Objects.requireNonNull(findParentColumnFromSelectList(select.getSelectList().getList(), columnName)), from);
            }
        } else if (sqlNode instanceof SqlJoin) {
            SqlJoin sqlJoin = (SqlJoin) sqlNode;

            String leftTableName = findTableNameFromCurrentSqlNode(sqlJoin.getLeft());
            String rightTableName = findTableNameFromCurrentSqlNode(sqlJoin.getRight());
            if (fromTableAlias.equals("")) {
                String leftMatch = findFromTableAndColumn(columnName, sqlJoin.getLeft());
                if (leftMatch == null) {
                    return findFromTableAndColumn(columnName, sqlJoin.getRight());
                } else {
                    return leftMatch;
                }
            } else if (fromTableAlias.equals(leftTableName)) {
                return findFromTableAndColumn(columnName, sqlJoin.getLeft());
            } else if (fromTableAlias.equals(rightTableName)) {
                return findFromTableAndColumn(columnName, sqlJoin.getRight());
            }
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlOperator operator = sqlBasicCall.getOperator();
            return findFromTableAndColumn(columnName, sqlBasicCall.getOperands()[0]);
        } else if (sqlNode instanceof SqlIdentifier) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode;
            String cns = "";
            for (String cn: columnName.split("&")) {
                cns += sqlIdentifier.toString() + TABLE_COLUMN_SEPARATOR + cn + "&";
            }
            return cns.substring(0, cns.length()-1);
        } else {
            throw new RuntimeException("operator " + sqlNode.getClass() + " not support");
        }

        return null;
    }

    private static List<String> lookupSelectTable(SqlNode sqlNode, String alias) {
        List<String> list = new ArrayList<>();
        if (sqlNode instanceof SqlSelect) {
            SqlNode from = ((SqlSelect) sqlNode).getFrom();
            list.addAll(lookupSelectTable(from, alias));
        } else if (sqlNode instanceof SqlJoin) {
            SqlJoin sqlJoin = (SqlJoin) sqlNode;
            list.addAll(lookupSelectTable(sqlJoin.getLeft(), alias));
            list.addAll(lookupSelectTable(sqlJoin.getRight(), alias));
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlOperator operator = sqlBasicCall.getOperator();
            if (SqlKind.AS.equals(operator.getKind())) {
                list.addAll(lookupSelectTable(sqlBasicCall.getOperands()[0], sqlBasicCall.getOperands()[1].toString()));
            } else if (SqlKind.UNION.equals(operator.getKind())) {
                for (SqlNode operandSqlNode : sqlBasicCall.getOperands()) {
                    list.addAll(lookupSelectTable(operandSqlNode, alias));
                }
            } else if (SqlKind.LATERAL.equals(operator.getKind())
                    || SqlKind.COLLECTION_TABLE.equals(operator.getKind())
                    || SqlKind.OTHER_FUNCTION.equals(operator.getKind())) {
                list.addAll(lookupSelectTable(sqlBasicCall.getOperands()[0], alias));
            } else {
                throw new RuntimeException("operator " + operator.getKind() + " not support");
            }
        } else if (sqlNode instanceof SqlIdentifier) {
            if (!alias.equals("")) {
                list.add(((SqlIdentifier) sqlNode).toString() + TABLE_ALIAS_SEPARATOR + alias);
            } else {
                list.add(((SqlIdentifier) sqlNode).toString());
            }
        } else {
            throw new RuntimeException("operator " + sqlNode.getClass() + " not support");
        }
        return list;
    }

    private static SqlSelect lookupSqlSelect(SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            return (SqlSelect) sqlNode;
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlOperator operator = sqlBasicCall.getOperator();
            if (SqlKind.UNION.equals(operator.getKind())) {
                return lookupSqlSelect(sqlBasicCall.getOperands()[0]);
            }
        } else if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = (SqlOrderBy) sqlNode;
            return lookupSqlSelect(sqlOrderBy.query);
        }

        return null;
    }

    private static String getSqlIdentifierFromUdf(SqlNode sqlNode) {

        if (sqlNode instanceof SqlIdentifier) {
            // SqlKind.IDENTIFIER.equals(operator.getKind())
            return sqlNode.toString();
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            SqlNode[] operands = sqlBasicCall.getOperands();
            String columnName = "";
            for (SqlNode operand : operands) {
                columnName += getSqlIdentifierFromUdf(operand) + "&";
            }
            while (columnName.endsWith("&")) {
                columnName = columnName.substring(0, columnName.length()-1);
            }
            return columnName;
        } else if (sqlNode instanceof SqlIntervalLiteral) {
            return "";
        }

        return "";
    }
}
