package lusaisai;

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVParser;
import com.opencsv.ICSVWriter;
import com.opencsv.ResultSetHelperService;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class DB {
    private final Connection con;
    private final String dateFormat = "yyyy-MM-dd";
    private final String timestampFormat = "yyyy-MM-dd HH:mm:ss";
    private String delimiter = ",";


    public DB(String DBURL) throws SQLException {
        this.con = DriverManager.getConnection(DBURL);
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public void queryToCSV(String query, String filePath, boolean withHeader) throws IOException, SQLException {
        ResultSetHelperService service = new ResultSetHelperService();
        service.setDateFormat(dateFormat);
        service.setDateTimeFormat(timestampFormat);
        Writer writer = Files.newBufferedWriter(Paths.get(filePath), StandardCharsets.UTF_8);
        ICSVWriter csvWriter = new CSVWriterBuilder(writer)
                .withSeparator(getDelimiter().charAt(0))
                .withQuoteChar(ICSVParser.DEFAULT_QUOTE_CHARACTER)
                .withEscapeChar(ICSVParser.DEFAULT_ESCAPE_CHARACTER)
                .withLineEnd(ICSVWriter.DEFAULT_LINE_END)
                .withResultSetHelper(service)
                .build();

        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        csvWriter.writeAll(rs, withHeader);

        writer.flush();
        writer.close();
    }

    public void queryToText(String query, String filePath, boolean withHeader) throws IOException, SQLException {
        ArrayList<ArrayList<String>> table = this.query(query, withHeader);
        Writer writer = Files.newBufferedWriter(Paths.get(filePath), StandardCharsets.UTF_8);
        for (ArrayList<String> row :
                table) {
            writer.write(String.join(getDelimiter(), row));
            writer.write("\n");
        }
        writer.flush();
        writer.close();
    }

    public ArrayList<ArrayList<String>> query(String query, boolean withHeader) throws SQLException {
        ArrayList<ArrayList<String>> table = new ArrayList<>();
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();

        if (withHeader) {
            ArrayList<String> row = new ArrayList<>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                row.add(rsmd.getColumnName(i).toUpperCase());
            }
            table.add(row);
        }

        while (rs.next()) {
            ArrayList<String> row = new ArrayList<>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                if (rsmd.getColumnType(i) == Types.DATE) {
                    row.add(new SimpleDateFormat(dateFormat).format(rs.getDate(i)));
                } else if (rsmd.getColumnType(i) == Types.TIMESTAMP) {
                    row.add(new SimpleDateFormat(timestampFormat).format(rs.getTimestamp(i)));
                } else {
                    row.add(rs.getString(i));
                }
            }
            table.add(row);
        }

        return table;
    }
}
