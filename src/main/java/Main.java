import enums.OperationType;
import operations.InsertANewEntry;
import operations.ReadMultipleSingleTuplesAndSum;
import operations.SumWholeTableInSQL;
import operations.UpdateThreeTableEntries;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    private static final int NUM_TUPLE = 50000;
    private static final double MAX_VALUE = 50.0f;
    public static final int NUMBER_OF_THREADS = 16;
    private static final AtomicInteger SUM_INDIVIDUAL_TUPLES = new AtomicInteger();
    private static final AtomicInteger INSERT = new AtomicInteger();
    private static final AtomicInteger SUM_WHOLE_TABLE_SQL = new AtomicInteger();
    private static final AtomicInteger UPDATE_3_ENTRIES = new AtomicInteger();
    private static final AtomicInteger TASKS_FINISHED = new AtomicInteger();

    void executeSQL(String sql, Connection con) {
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing " + sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void cerateDB() {
        final var rand = new Random(123456);

        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS RestartBlocking_data;", con);
            executeSQL("CREATE TABLE RestartBlocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO RestartBlocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for (int i = 0; i < NUM_TUPLE; i++) {
                ps.setInt(1, i);
                ps.setDouble(2, rand.nextDouble() * MAX_VALUE);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            System.out.println(Arrays.toString(results));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        //Create DB
        Main b = new Main();
        b.cerateDB();

        // Thread safe methods of storing running state and the current task. Essentially an ID.
        AtomicBoolean isRunning = new AtomicBoolean(true);
        AtomicInteger currentTaskNumber = new AtomicInteger(0);

        //Execute tasks
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(NUMBER_OF_THREADS);//XXX - Do not change the number of threads used

        final var start = System.currentTimeMillis();

        // create a certain number of workers and each continues to create new transactions
        // until isRunning is false (about 30 seconds).
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            executor.execute(() -> {
                while (isRunning.get()) {
                    int task = currentTaskNumber.getAndIncrement();
                    b.new Blocker(task).run();
                }
            });
        }

        // Run benchmark for 30 seconds
        Thread.sleep(30_000);

        isRunning.set(false);
        executor.shutdown();

        // we wait for the current transactions to complete
        while (!executor.isTerminated()) {
            Thread.sleep(100);
        }

        double stop = System.currentTimeMillis();
        System.out.println("[DONE] Finished " + TASKS_FINISHED + " transactions in " + (stop - start) / 1000.0 + "s");
        System.out.println("Operation distribution:");
        System.out.println("Sum individual tuples: " + SUM_INDIVIDUAL_TUPLES.get());
        System.out.println("Insert: " + INSERT.get());
        System.out.println("Sum whole table (SQL): " + SUM_WHOLE_TABLE_SQL.get());
        System.out.println("Update entries: " + UPDATE_3_ENTRIES.get());
    }

    /**
     * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
     *
     */
    public class Blocker implements Runnable {
        private static final int NUM_QUERIES_PER_TASK = 3;
        private static final long BASE_BACKOFF_MS = 50;
        private static final String SERIALIZATION_FAILURE = "40001";
        private static final String DEADLOCK_DETECTED = "40P01";
        private static final String LOCK_NOT_AVAILABLE = "55P03";

        final int task;
        final double[] startRange;
        final double[] stopRange;
        final double p;

        public Blocker(int id) {
            this.task = id;
            this.startRange = new double[NUM_QUERIES_PER_TASK];
            this.stopRange = new double[NUM_QUERIES_PER_TASK];

            // Using ThreadLocalRandom as "The random number obtained
            // by one thread is not affected by the other thread
            // whereas java.util.Random provides random numbers globally"
            // From: https://www.baeldung.com/java-thread-local-random
            this.p = ThreadLocalRandom.current().nextDouble(0, 1);

            for (int q = 0; q < NUM_QUERIES_PER_TASK; q++) {
                stopRange[q] = ThreadLocalRandom.current().nextDouble() * MAX_VALUE;
                startRange[q] = ThreadLocalRandom.current().nextDouble() * stopRange[q];
            }
        }

        // 40001 - Serialization failure, 40P01 - Deadlock detected, 55P03 - Lock not available
        private static boolean isRetryable(SQLException e) {
            final var state = e.getSQLState();
            return SERIALIZATION_FAILURE.equals(state)
                    || DEADLOCK_DETECTED.equals(state)
                    || LOCK_NOT_AVAILABLE.equals(state);
        }

        @Override
        public void run() {
            int attempt = 0;
            while (true) {
                attempt++;
                Connection con = null;
                try {
                    con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                    con.setAutoCommit(false);
                    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                    // choose operation to run
                    final var operationType = OperationType.fromProbability(this.p);
                    final var operation = switch (operationType) {
                        case READ_MULTIPLE_TUPLES -> new ReadMultipleSingleTuplesAndSum();
                        case INSERT_ENTRY -> new InsertANewEntry(task);
                        case READ_WHOLE_TABLE -> new SumWholeTableInSQL();
                        case UPDATE_IN_RANGES -> new UpdateThreeTableEntries(task, startRange, stopRange);
                    };

                    // execute and commit
                    final var result = operation.execute(con);
                    con.commit();


                    // increment global value & print result
                    switch (operationType) {
                        case READ_MULTIPLE_TUPLES -> SUM_INDIVIDUAL_TUPLES.incrementAndGet();
                        case INSERT_ENTRY -> INSERT.incrementAndGet();
                        case READ_WHOLE_TABLE -> SUM_WHOLE_TABLE_SQL.incrementAndGet();
                        case UPDATE_IN_RANGES -> UPDATE_3_ENTRIES.incrementAndGet();
                        default -> throw new RuntimeException("Invalid operation");
                    }
                    final var output = "Done task " +
                            this.task +
                            " after " +
                            attempt +
                            " attempt(s). I am finisher " +
                            TASKS_FINISHED.incrementAndGet() +
                            "\n" +
                            result +
                            "\n";

                    System.out.println(output);
                    return;
                } catch (SQLException e) {
                    // roll back on error
                    if (con != null) {
                        try {
                            con.rollback();
                        } catch (SQLException rollbackError) {
                            // don't lose what reason was
                            e.addSuppressed(rollbackError);
                        }
                    }

                    // set ceiling to not forever re-try
                    if (isRetryable(e)) {
                        try {
                            Thread.sleep(BASE_BACKOFF_MS * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    } else {
                        System.err.println("Task " + task + " failed: " + e.getMessage());
                        return;
                    }
                } finally {
                    if (con != null) {
                        try {
                            con.close();
                        } catch (SQLException ignored) {
                        }
                    }
                }
            }
        }
    }
}