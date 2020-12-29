package org.apache.lineage.flink.sql.utils;

public class Constants {

    public static final String TABLE_COLUMN_SEPARATOR = ".";
    public static final String TRANSFERRED_TABLE_COLUMN_SEPARATOR = "\\.";

    public static final String MULTY_COLUMN_SEPARATOR = ",";
    public static final String TABLE_ALIAS_SEPARATOR = "#";

    /** 默认的catalog以及database */
    public static final String DEFAULT_CATALOG = "default_catalog";
    public static final String DEFAULT_DATABASE = "default_database";
    public static final String SQL_BACK_QUOTE = "`";

    /** 忽略上报字段 */
    public static final String[] IGNORE_FIELDS = {"proctime", "rowtime"};
}
