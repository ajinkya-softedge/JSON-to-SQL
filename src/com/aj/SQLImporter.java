package com.aj;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class SQLImporter {

    private static final HashMap<String, String> sqlTokens;
    private static Pattern sqlTokenPattern;
    int MAX_QUERY_SIZE = 1000;
    String sourcePath = "/home/ajinkyadeshmukh/amber/java/console/src/jsonsqlimport/json/";

    public SQLImporter(String sourcePath) {
        if(!sourcePath.endsWith(File.separator)) {
            sourcePath += File.separator;
        }
        this.sourcePath = sourcePath;

        System.out.println("Started for source " + sourcePath);
        try {
            readJSON();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static {
        //MySQL escape sequences: http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html
        String[][] search_regex_replacement = new String[][]
                {
                        //search string     search regex        sql replacement regex
                        {"\u0000", "\\x00", "\\\\0"},
                        {"'", "'", "\\\\'"},
                        {"\"", "\"", "\\\\\""},
                        {"\b", "\\x08", "\\\\b"},
                        {"\n", "\\n", "\\\\n"},
                        {"\r", "\\r", "\\\\r"},
                        {"\t", "\\t", "\\\\t"},
                        {"\u001A", "\\x1A", "\\\\Z"},
                        {"\\", "\\\\", "\\\\\\\\"}
                };

        sqlTokens = new HashMap<String, String>();
        String patternStr = "";
        for (String[] srr : search_regex_replacement) {
            sqlTokens.put(srr[0], srr[2]);
            patternStr += (patternStr.isEmpty() ? "" : "|") + srr[1];
        }
        sqlTokenPattern = Pattern.compile('(' + patternStr + ')');
    }


    public static String escape(String s) {
        Matcher matcher = sqlTokenPattern.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, sqlTokens.get(matcher.group(1)));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }


    public void readJSON() throws IOException {
        System.out.println("***************************************************START*********************************");
        File sourceFolder = new File(sourcePath);
        if (sourceFolder.isDirectory()) {
            Arrays.stream(sourceFolder.list()).filter(fileName -> fileName.toLowerCase().endsWith(".json")).forEach(fileName -> {
                File file = new File(sourcePath + fileName);
                fileName = fileName.substring(0, fileName.lastIndexOf("."));
                try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
                    StringBuilder buff = new StringBuilder("{\"").append(fileName).append("\":");
                    while (bis.available() > 0) {
                        buff.append((char) bis.read());
                    }

                    buff.append("}");

                    JSONObject jsonObject = new JSONObject(buff.toString());

                    StringBuilder sqlBuff = new StringBuilder("TRUNCATE TABLE ").append(fileName).append(";\n");
                    sqlBuff.append("INSERT INTO ").append(fileName).append(" (");

                    Iterator<String> iterator = ((JSONObject) ((JSONArray) jsonObject.get(fileName)).get(0)).keys();
                    List<String> keyList = new ArrayList<>();

                    while (iterator.hasNext()) {
                        String str = iterator.next();
                        sqlBuff.append(str).append(",");
                        keyList.add(str);
                    }
                    sqlBuff.setLength(sqlBuff.length() - 1);
                    sqlBuff.append(") VALUES ");

                    JSONArray jsonArray = ((JSONArray) jsonObject.get(fileName));
                    int jsonCount = jsonArray.length();

                    String insertSQL = sqlBuff.toString();

                    for (int i = 0; i < jsonCount; i++) {
                        JSONObject jsonObject1 = (JSONObject) jsonArray.get(i);
                        sqlBuff.append("(");
                        for (int j = 0; j < keyList.size(); j++) {
                            String key = keyList.get(j);
                            Object object = jsonObject1.get(key);

                            if (object instanceof String) {
                                String val = (String) object;
                                if (Stream.of("last_update_ts", "ts").anyMatch(tsKey -> tsKey.equalsIgnoreCase(key))) {
                                    val = "str_to_date('" + val.substring(0, val.lastIndexOf(".")) + "', '%Y-%m-%dT%H:%i:%s')";
                                } else {
                                    val = "\"" + escape((String) object) + "\"";
                                }

                                object = val;
                                //str = str.replaceAll("\"", "\"")
                            }

                            sqlBuff.append("").append(object).append(",");
                        }
                        sqlBuff.setLength(sqlBuff.length() - 1);
                        sqlBuff.append(")\n,");

                        if (i > 0 && i % MAX_QUERY_SIZE == 0) {
                            sqlBuff.setLength(sqlBuff.length() - 2);
                            sqlBuff.append(";\n\n").append(insertSQL);
                        }
                    }

                    sqlBuff.setLength(sqlBuff.length() - 2);
                    if (sqlBuff.charAt(sqlBuff.length() - 1) == ';') {
                        sqlBuff.setLength(sqlBuff.length() - 1);
                    }
                    sqlBuff.append(";");

                    writeOutput(fileName, sqlBuff);

                    System.out.println(sqlBuff);

                } catch (IOException | JSONException e) {
                    System.out.println("\nEXCEPTION while processing " + fileName);
                    e.printStackTrace();
                }
            });
        }


        System.out.println("***************************************************END*********************************");
    }

    private void writeOutput(String fileName, StringBuilder sqlBuff) throws IOException {
        File outputFile = new File(sourcePath + fileName + ".sql");
        if (outputFile.exists()) {
            outputFile.delete();
        }
        outputFile.createNewFile();
        try (FileOutputStream fos = new FileOutputStream(outputFile);) {
            fos.write(sqlBuff.toString().getBytes());
        }
    }
}
