package operations;

import java.sql.*;

public class ReadWholeTable implements DatabaseOperation {

    private static final String SUM_WHOLE_TABLE =
            "SELECT SUM(data_value) AS sum FROM RestartBlocking_data";

    @Override
    public String execute(Connection connection) throws SQLException {

        try (PreparedStatement statement =
                     connection.prepareStatement(SUM_WHOLE_TABLE);
             ResultSet rs = statement.executeQuery()) {

            if (!rs.next()) {
                throw new SQLException("Full table SUM returned no result!");
            }

            double sum = rs.getDouble("sum");

            return "Sum whole table: " + sum;
        }
    }
}