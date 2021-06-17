package Week3;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;

public class ImportCsv {

    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/ghtk";
        String username = "root";
        String password = "";

        String csvFilePath = "C:\\Users\\Hai\\IdeaProjects\\GHTK\\ghtk.csv";

        int batchSize = 10000;

        Connection connection = null;

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        try {

            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO `customers_packages`(`shop_code`, `customer_tel`, `customer_tel_normalize`, `fullname`, `pkg_created`, `pkg_modified`, `package_status_id`, `customer_province_id`, `customer_district_id`, `customer_ward_id`, `created`, `modified`, `is_cancel`, `ightk_user_id`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;

            int count = 0;

            lineReader.readLine(); // skip header line

            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");

                String shop_code = data[0];
                String customer_tel = data[1];
                String customer_tel_normalize = data[2];
                String fullname = data[3];
                String pkg_created = data[4];
                String pkg_modified = data[5];
                String package_status_id = data[6];
                String customer_province_id = data[7];
                String customer_district_id = data[8];
                String customer_ward_id = data[9];
                String created = data[10];
                String modified = data[11];
                String is_cancel = data[12];
                String ightk_user_id = data[13];


//                Integer packagestatusid = Integer.parseInt(package_status_id);
//                Integer customerprovinceid = Integer.parseInt(customer_province_id);
//                Integer customerdistrictid = Integer.parseInt(customer_district_id);
//                Integer customerwardid = Integer.parseInt(customer_ward_id);
//                Integer iscancel = Integer.parseInt(is_cancel);
//                Integer ightkuserid = Integer.parseInt(ightk_user_id);



                statement.setString(1, shop_code);
                statement.setString(2,customer_tel);
                statement.setString(3, customer_tel_normalize);
                statement.setString(4, fullname);
                statement.setString(5, pkg_created);
                statement.setString(6, pkg_modified);
                statement.setString(7, package_status_id);
                statement.setString(8,customer_province_id);
                statement.setString(9, customer_district_id);
                statement.setString(10, customer_ward_id);
                statement.setString(11,created);
                statement.setString(12, modified);
                statement.setString(13, is_cancel);
                statement.setString(14,ightk_user_id);

                statement.addBatch();

                if (++count % batchSize == 0) {
                    statement.executeBatch();
                    System.out.println(count);
                }
            }

            lineReader.close();

            // execute the remaining queries
            statement.executeBatch();

            connection.commit();
            connection.close();
            System.out.println("Success");
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

    }
}
