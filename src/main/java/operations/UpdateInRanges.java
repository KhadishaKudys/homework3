package operations;

import lombok.AllArgsConstructor;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@AllArgsConstructor
public class UpdateInRanges implements DatabaseOperation {

    private static final String UPDATE_TUPLE =
            "UPDATE RestartBlocking_data " +
                    "SET data_value = ?, modified_by = ? " +
                    "WHERE data_id = (" +
                    "SELECT data_id FROM RestartBlocking_data " +
                    "WHERE data_value >= ? AND data_value <= ? " +
                    "AND data_id != ALL(?) " +
                    "LIMIT 1" +
                    ") RETURNING data_id";

    private static final double MAX_VALUE = 50.0;
    private static final int NUM_ENTRIES_TO_UPDATE = 3;

    private final int task;
    private final double[] startRange;
    private final double[] stopRange;

    @Override
    public String execute(Connection connection) throws SQLException {
        List<Integer> updatedIds = new ArrayList<>();

        while (updatedIds.size() < NUM_ENTRIES_TO_UPDATE) {

            Array sqlArray = connection.createArrayOf(
                    "INTEGER",
                    updatedIds.toArray(Integer[]::new)
            );

            try (PreparedStatement statement =
                         connection.prepareStatement(UPDATE_TUPLE)) {

                statement.setDouble(1,
                        ThreadLocalRandom.current().nextDouble() * MAX_VALUE);

                int q = ThreadLocalRandom.current().nextInt(startRange.length);

                statement.setInt(2, task);
                statement.setDouble(3, startRange[q]);
                statement.setDouble(4, stopRange[q]);
                statement.setArray(5, sqlArray);

                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException(
                                "No matching tuple found for update. Updated IDs: " + updatedIds
                        );
                    }

                    updatedIds.add(rs.getInt("data_id"));
                }

            } finally {
                sqlArray.free();
            }
        }

        return "Update entries: " + updatedIds;
    }
}