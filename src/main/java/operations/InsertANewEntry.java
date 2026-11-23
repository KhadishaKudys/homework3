package operations;

import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

@AllArgsConstructor
public class InsertANewEntry implements DatabaseOperation {

    private static final String INSERT_QUERY =
            "INSERT INTO RestartBlocking_data " +
                    "(data_id, data_value, modified_by) " +
                    "VALUES (?, ?, ?) RETURNING data_id";

    private final int task;

    @Override
    public String execute(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_QUERY)) {

            statement.setInt(1, 50000 + task);
            statement.setDouble(2, ThreadLocalRandom.current().nextDouble(0, 100));
            statement.setInt(3, ThreadLocalRandom.current().nextInt(1, 100));

            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("Insert returned no data!");
                }

                return "Inserted tuple: ID = " + rs.getInt("data_id");
            }
        }
    }
}