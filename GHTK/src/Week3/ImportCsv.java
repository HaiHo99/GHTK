package Week3;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ImportCsv {
    public static Connection getConnect() throws SQLException, ClassNotFoundException {
        final String DB_URL = "jdbc:mysql://localhost:3306/ghtk";
        final String USER = "root";
        final String PASS = "";
        Connection connection=DriverManager.getConnection(DB_URL,USER,PASS);
        return connection;
    }
    public static void main(String[] agrs) throws ClassNotFoundException, SQLException, IOException, CsvValidationException {

        int batch_size=1000;
        CSVReader reader = new CSVReader(new FileReader("C:\\Users\\Hai\\IdeaProjects\\GHTK\\ghtk.csv"));
        String[] nextLine;
        String[] firstLine = reader.readNext();

        Connection conn=getConnect();
        Statement statement=conn.createStatement();
        String sql = "INSERT INTO customers_packages(shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created,pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";


        int count=0;

        while ((nextLine = reader.readNext()) != null) {
            count++;
            sql += "('" + nextLine[0] + "','" + nextLine[1] + "','" + nextLine[2] + "',\""
                    + nextLine[3] +"\",'" + nextLine[4] + "','" + nextLine[5]+"'," + nextLine[6]
                    + "," + nextLine[7] + ","
                    + nextLine[8] + "," + nextLine[9] + ",'"
                    + nextLine[10] + "','" + nextLine[11] + "'," + nextLine[12] +"," +nextLine[13] +")";
            if (count%batch_size==0 ) {
                sql+=";";
                System.out.println(count);
                statement.executeUpdate(sql);
                sql = "INSERT INTO customers_packages(shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created, pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) VALUES";
            }
            else {
                sql+=',';
            }
        }
    }
}