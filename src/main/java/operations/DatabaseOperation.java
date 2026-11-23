package operations;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface DatabaseOperation {

    String execute(Connection connection) throws SQLException;

}