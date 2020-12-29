package org.apache.lineage.flink.sql.parser;

import lombok.Data;

/**
 * @author: james
 * @date: 2020/7/17
 */
@Data
public class SqlParseColumn {
    private String name;
    private String selectName;
    private String selectAlias;
    private String type;
    private Boolean nullable;
    private String constraint;
    private String comment;
    private Boolean isPhysical = true; // 是否计算列
    private Boolean isConstant = false; // 是否常量列
    /** 存储的是parentTable1.parentColumn1, parentTable2.parentColumn2 */
    private String parents;
}
