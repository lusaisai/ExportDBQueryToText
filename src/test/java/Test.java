import lusaisai.DB;
import org.junit.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

public class Test {
    private DB db;

    @Before
    public void setUp() throws SQLException {
        this.db = new DB("jdbc:postgresql://localhost/ifund?user=ifund&password=ifund");
    }

    @org.junit.Test
    public void testQuery() throws SQLException {
        ArrayList<ArrayList<String>> table = this.db.query("select * from stock_estimate_cache limit 10", true);
        System.out.println(table);
    }

    @org.junit.Test
    public void testText() throws SQLException, IOException {
        this.db.queryToText("select * from stock_estimate_cache limit 10", "C:\\Users\\lusaisai\\Downloads\\funds.txt", true);
        this.db.setDelimiter("|");
        this.db.queryToText("select * from stock_estimate_cache limit 20", "C:\\Users\\lusaisai\\Downloads\\funds_2.txt", false);
        this.db.queryToText("select to_char(UPDATE_TS, 'DD-MON-YYYY HH24:MI:SS') as update_ts from stock_estimate_cache limit 20", "C:\\Users\\lusaisai\\Downloads\\funds_3.txt", false);
    }

    @org.junit.Test
    public void testCSV() throws SQLException, IOException {
        this.db.queryToCSV("select * from stock_estimate_cache",
                "C:\\Users\\lusaisai\\Downloads\\estimates.csv",
                true);
        this.db.setDelimiter("|");
        this.db.queryToCSV("select * from stock_estimate_cache",
                "C:\\Users\\lusaisai\\Downloads\\estimates2.csv",
                true);
        }
}
