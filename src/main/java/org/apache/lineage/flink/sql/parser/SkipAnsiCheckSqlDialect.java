package org.apache.lineage.flink.sql.parser;

import org.apache.calcite.sql.SqlDialect;

public class SkipAnsiCheckSqlDialect extends SqlDialect {
    public static final Context DEFAULT_CONTEXT;
    public static final SqlDialect DEFAULT;

    public SkipAnsiCheckSqlDialect(Context context) {
        super(context);
    }

    static {
        DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT.withDatabaseProduct(DatabaseProduct.UNKNOWN).withIdentifierQuoteString("`");
        DEFAULT = new SkipAnsiCheckSqlDialect(DEFAULT_CONTEXT);
    }

    /**
     * 重写该方法以支持中文
     *
     * @param buf
     * @param charsetName
     * @param val
     */
    public void quoteStringLiteral(StringBuilder buf, String charsetName, String val) {
        buf.append(this.literalQuoteString);
        buf.append(val.replace(this.literalEndQuoteString, this.literalEscapedQuote));
        buf.append(this.literalEndQuoteString);
    }
}
