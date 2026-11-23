package operations;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

@AllArgsConstructor
public class ReadMultipleTuples implements DatabaseOperation {

    private static final String READ_ONLY_QUERY = "SELECT data_value FROM RestartBlocking_data WHERE data_id = ?";

    private static final int NUM_TUPLE = 50000;

    @Override
    public String execute(Connection connection) throws SQLException {
        List<Pair<Integer, Double>> results = new ArrayList<>();
        final var selectedIds = new HashSet<>();

        try (PreparedStatement statement = connection.prepareStatement(READ_ONLY_QUERY)) {
            while (selectedIds.size() < 3) {
                int id = ThreadLocalRandom.current().nextInt(NUM_TUPLE);

                if (!selectedIds.add(id)) {
                    continue;
                }

                statement.setInt(1, id);
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new SQLException("No result found for id: " + id);
                    }
                    results.add(Pair.of(id, rs.getDouble("data_value")));
                }
            }
        }

        double sum = results.stream()
                .mapToDouble(Pair::getValue)
                .sum();

        final var output = new StringBuilder("Sum 3 Tuples:\n");
        results.forEach(result ->
                output.append(" id: ").append(result.getLeft())
                        .append(", value: ").append(result.getRight())
                        .append("\n")
        );
        output.append("Sum: ").append(sum);

        return output.toString();
    }
}