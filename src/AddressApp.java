import java.sql.Connection;
import java.sql.Date;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AddressApp {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/addressDB";
    private static final String USER = "username";
    private static final String PASS = "password";

    public static void main(String[] args) {
        try {
            // Задача 1
            List<String> objectIds = Arrays.asList("1", "2", "3");
            Date date = new Date(2022 - 1 - 1);
            List<String> descriptions = getAddressDescriptions(objectIds, date);
            System.out.println("Address descriptions: " + descriptions);

            // Задача 2
            List<String> actualAddresses = getActualAddressesWithType("проезд");
            System.out.println("Actual addresses with type 'проезд': " + actualAddresses);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static List<String> getAddressDescriptions(List<String> objectIds, Date date) throws SQLException {
        List<String> descriptions = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             Statement stmt = conn.createStatement()) {

            for (String objectId : objectIds) {
                String query = "SELECT NAME, TYPENAME " +
                        "FROM AS_ADDR_OBJ " +
                        "WHERE OBJECTID = ? " +
                        "AND STARTDATE <= ? " +
                        "AND (ENDDATE >= ? OR ENDDATE IS NULL)";
                try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                    pstmt.setString(1, objectId);
                    pstmt.setDate(2, new java.sql.Date(date.getTime()));
                    pstmt.setDate(3, new java.sql.Date(date.getTime()));
                    ResultSet rs = pstmt.executeQuery();
                    if (rs.next()) {
                        descriptions.add(rs.getString("NAME") + " (" + rs.getString("TYPENAME") + ")");
                    }
                }
            }
        }
        return descriptions;
    }

    public static List<String> getActualAddressesWithType(String type) throws SQLException {
        List<String> addresses = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
             Statement stmt = conn.createStatement()) {

            String query = "SELECT a.NAME, a.OBJECTID " +
                    "FROM AS_ADDR_OBJ a " +
                    "JOIN AS_ADM_HIERARCHY h " +
                    "ON a.OBJECTID = h.OBJECTID " +
                    "WHERE a.TYPENAME = ? " +
                    "AND h.ISACTIVE = 1 " +
                    "AND (h.STARTDATE <= CURRENT_DATE " +
                    "OR h.STARTDATE IS NULL) " +
                    "AND (h.ENDDATE >= CURRENT_DATE " +
                    "OR h.ENDDATE IS NULL)";
            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, type);
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    StringBuilder address = new StringBuilder();
                    while (rs.next()) {
                        if (address.length() > 0) {
                            address.append(", ");
                        }
                        address.append(rs.getString("NAME"));
                    }
                    addresses.add(address.toString());
                }
            }
        }
        return addresses;
    }
}
