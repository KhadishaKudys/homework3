import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 *
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
 */

public class StreamTransactions {
    public static int num_queries_per_task = 3;
    public static int num_threads = 4;
    public static int duration_sec = 30;
    public static boolean print = true;
    private static final long BASE_BACKOFF_MS = 0;

    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished;
    AtomicInteger next_insert_id;
    AtomicInteger task_counter;
    AtomicInteger retries;
    AtomicInteger[] finished_by_type;
    volatile boolean running = true;

    public StreamTransactions() {
        tasks_finished = new AtomicInteger(0);
        next_insert_id = new AtomicInteger(num_tuple);
        task_counter = new AtomicInteger(0);
        retries = new AtomicInteger(0);
        finished_by_type = new AtomicInteger[4];
        for (int i = 0; i < 4; i++) {
            finished_by_type[i] = new AtomicInteger(0);
        }
    }

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
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for (int i = 0; i < num_tuple; i++) {
                ps.setInt(1, i);
                ps.setDouble(2, rand.nextDouble() * max_value);
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

    void worker() {
        Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
        try {
            con.setAutoCommit(false);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            while (running) {
                new Blocker(task_counter.incrementAndGet()).runTransaction(con);
            }
        } catch (SQLException e) {
            System.err.println("Worker stopped: " + e.getMessage());
        } finally {
            try {
                con.close();
            } catch (SQLException ignored) {
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length > 0) num_threads = Integer.parseInt(args[0]);
        if (args.length > 1) duration_sec = Integer.parseInt(args[1]);
        if (args.length > 2) print = Boolean.parseBoolean(args[2]);
        if (args.length > 3) num_queries_per_task = Integer.parseInt(args[3]);

        //Create DB
        StreamTransactions st = new StreamTransactions();
        st.cerateDB();

        //Create task and their queries
//        ArrayList<Blocking.Blocker> my_tasks = new ArrayList<Blocking.Blocker>(num_taks);
//        for(int task=0;task<num_taks;task++){
//            my_tasks.add(st.new Blocker(task));
//        }

        //Execute tasks
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);//XXX - Do not change the number of threads used
        double start, stop;
        start = System.currentTimeMillis();
        for (int i = 0; i < num_threads; i++) {
            executor.execute(st::worker);
        }

        int last = 0;
        for (int s = 1; s <= duration_sec; s++) {
            Thread.sleep(1000);
            int now = st.tasks_finished.get();
            System.out.println("[t=" + s + "s] " + (now - last) + " committed tx/s, restarts so far " + st.retries);
            last = now;
        }

        st.running = false;
        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.MINUTES);
        double sec = (System.currentTimeMillis() - start) / 1000.0;
        System.out.printf("[DONE] threads=%d committed=%d in %.1f s -> %.1f tx/s, restarts=%d%n",
                num_threads, st.tasks_finished.get(), sec, st.tasks_finished.get() / sec, st.retries.get());
        System.out.println(" point reads: " + st.finished_by_type[0] + ", inserts: " + st.finished_by_type[1] + ", full scans: " + st.finished_by_type[2] + ", writes: " + st.finished_by_type[3]);

        //wait for the tasks to finish
//        while (!executor.isTerminated()) {
//            try {
//                Thread.sleep(100);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        stop = System.currentTimeMillis();
//        System.out.println("[DONE] Task execution after roughly " + (stop - start) + " ms finished: " + st.tasks_finished + " of " + num_taks);
    }

    /**
     * This is simply a task attempting to hold many write locks for a long time and updating major parts of the database.
     *
     */
    public class Blocker {
        static final int POINT_READ = 0, INSERT = 1, FULL_SCAN = 2, WRITE = 3;
        final int task;
        final int type;
        final int[] ids;
        final int insert_id;

        public Blocker(int id) {
            this.task = id;
            double p = rand.nextDouble();
            if (p < 0.6) {
                type = POINT_READ;
            } else if (p < 0.7) {
                type = INSERT;
            } else if (p < 0.8) {
                type = FULL_SCAN;
            } else {
                type = WRITE;
            }
            if (type == POINT_READ || type == WRITE) {
                Set<Integer> chosen = new LinkedHashSet<>();
                while (chosen.size() < num_queries_per_task) {
                    chosen.add(rand.nextInt(num_tuple));
                }
                ids = chosen.stream().mapToInt(Integer::intValue).toArray();
            } else {
                ids = null;
            }
            insert_id = (type == INSERT) ? next_insert_id.getAndIncrement() : -1;
        }

        private String execute(Connection con) throws SQLException {
            switch (type) {
                case POINT_READ: {
                    double sum = 0;
                    PreparedStatement ps = con.prepareStatement("SELECT data_value FROM blocking_data WHERE data_id = ?");
                    for (int id : ids) {
                        ps.setInt(1, id);
                        ResultSet rs = ps.executeQuery();
                        if (rs.next()) {
                            sum += rs.getDouble(1);
                        }
                        rs.close();
                    }
                    ps.close();
                    return "Task " + task + " point read: sum = " + sum;
                }
                case INSERT: {
                    PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)");
                    ps.setInt(1, insert_id);
                    ps.setDouble(2, rand.nextDouble() * max_value);
                    ps.setInt(3, task);
                    ps.execute();
                    ps.close();
                    return "Task " + task + " inserted tuple " + insert_id;
                }
                case FULL_SCAN: {
                    PreparedStatement ps = con.prepareStatement("SELECT sum(data_value) FROM blocking_data");
                    ResultSet rs = ps.executeQuery();
                    rs.next();
                    double sum = rs.getDouble(1);
                    rs.close();
                    ps.close();
                    return "Task " + task + " full scan: sum = " + sum;
                }
                default: {
                    PreparedStatement ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?");
                    for (int id : ids) {
                        ps.setDouble(1, rand.nextDouble() * max_value);
                        ps.setInt(2, task);
                        ps.setInt(3, id);
                        ps.execute();
                    }
                    ps.close();
                    return "Task " + task + " updated tuples " + Arrays.toString(ids);
                }
            }
        }

        private boolean isRetryable(SQLException e) {
            String state = e.getSQLState();
            if (state == null) return false;
            // 40001 - Serialization failure, 40P01 - Deadlock detected, 55P03 - Lock not available
            return state.equals("40001") || state.equals("40P01") || state.equals(("55P03"));
        }

        public void runTransaction(Connection con) throws SQLException {
            int attempt = 0;
            while (true) {
                attempt++;
                try {
                    String output = execute(con);
                    con.commit();
                    int num_finished = tasks_finished.incrementAndGet();
                    finished_by_type[type].incrementAndGet();
                    if (print) {
                        System.out.println(output);
                        System.out.println("Done task " + task + " after " + attempt + " attempt(s). I am finisher number " + num_finished);
                    }
                    return;
                } catch (SQLException e) {
                    con.rollback();
                    if (!isRetryable(e)) {
                        throw e;
                    }
                    retries.incrementAndGet();
                    if (BASE_BACKOFF_MS > 0) {
                        try {
                            Thread.sleep(BASE_BACKOFF_MS * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        }
    }
}
