package org.apache.lineage.flink.sql.utils;


import java.util.ArrayList;
import java.util.List;

public class Sqlutils {

    public static List<String> splitSemiColon(String line) {
        boolean inSingleQuotes = false;
        boolean inDoubleQuotes = false;
        boolean escape = false;

        // normalize
        line = line.replaceAll("--[^\r\n]*", ""); // remove single-line comments
        line = line.replaceAll("/\\*[\\w\\W]*?(?=\\*/)\\*/", ""); // remove double-line comments
        line = line.trim();

        List<String> ret = new ArrayList<>();
        int beginIdx = 0;
        for (int idx = 0; idx < line.length(); idx++) {
            char c = line.charAt(idx);
            switch (c) {
                case ';':
                    if (!inSingleQuotes && !inDoubleQuotes) {
                        ret.add(line.substring(beginIdx, idx));
                        beginIdx = idx + 1;
                    }
                    break;
                case '"':
                    if (!escape) {
                        inDoubleQuotes = !inDoubleQuotes;
                    }
                    break;
                case '\'':
                    if (!escape) {
                        inSingleQuotes = !inSingleQuotes;
                    }
                    break;
                default:
                    break;
            }

            if (escape) {
                escape = false;
            } else if (c == '\\') {
                escape = true;
            }
        }

        if (beginIdx < line.length()) {
            ret.add(line.substring(beginIdx));
        }

        return ret;
    }

    public static String combineFullTableName(String originTableName) {
        int length = originTableName.split("\\.").length;
        String fullTableName = "";
        switch (length) {
            case 1:
                fullTableName = String.join(".",
                        Constants.DEFAULT_CATALOG, Constants.DEFAULT_DATABASE, originTableName);
                break;
            case 2:
                fullTableName = String.join(".",
                        Constants.DEFAULT_CATALOG, originTableName);
                break;
            case 3:
                fullTableName = originTableName;
                break;
        }

        return fullTableName.replaceAll(Constants.SQL_BACK_QUOTE, "");
    }
}
