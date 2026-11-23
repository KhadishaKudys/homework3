import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OptimizedBlocking {
    public static int num_threads = 4;
    public static int duration_seconds = 30;
    public static int num_queries_per_task = 3;
    private static final long BASE_BACKOFF_MS = 0;

    // optimizations, can be switched on/off with the 3rd argument, e.g. "conn,stmt,out"
    public static boolean reuse_connection = true;
    public static boolean reuse_statements = true;
    public static boolean buffered_output = true;
    public static boolean async_commit = false;

    static final String SQL_READ = "SELECT data_value FROM blocking_data WHERE data_id = ?;";
    static final String SQL_INSERT = "INSERT INTO blocking_data VALUES (?, ?, ?);";
    static final String SQL_SCAN = "SELECT sum(data_value) FROM blocking_data;";
    static final String SQL_WRITE = "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?;";

    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    long end_time;
    AtomicInteger tasks_finished;
    AtomicInteger task_counter;
    AtomicInteger next_insert_id;
    AtomicInteger restarts;
    AtomicInteger read_finished;
    AtomicInteger insert_finished;
    AtomicInteger scan_finished;
    AtomicInteger write_finished;

    public OptimizedBlocking(){
        tasks_finished = new AtomicInteger(0);
        task_counter = new AtomicInteger(0);
        next_insert_id = new AtomicInteger(num_tuple);
        restarts = new AtomicInteger(0);
        read_finished = new AtomicInteger(0);
        insert_finished = new AtomicInteger(0);
        scan_finished = new AtomicInteger(0);
        write_finished = new AtomicInteger(0);
    }

    void executeSQL(String sql, Connection con){
        java.sql.Statement stmt;
        try {
            stmt = con.createStatement();
            stmt.execute(sql);
            System.out.println("Executing "+sql);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void cerateDB(){
        try {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);

            PreparedStatement ps;
            con.setAutoCommit(false);
            ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            for(int i=0;i<num_tuple;i++){
                ps.setInt(1, i);
                ps.setDouble(2,rand.nextDouble()*max_value);
                ps.setInt(3, 0);
                ps.addBatch();
                ps.clearParameters();
            }

            int[] results = ps.executeBatch();
            con.commit();
            con.close();
            System.out.println("Inserted "+results.length+" tuples");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static void readOptions(String options) {
        if(options.equals("all")) {
            options = "conn,stmt,out,async";
        }
        reuse_connection = options.contains("conn");
        reuse_statements = options.contains("stmt");
        buffered_output = options.contains("out");
        async_commit = options.contains("async");
    }

    public static void main(String[] args){
        if(args.length > 0) {
            num_threads = Integer.parseInt(args[0]);
        }
        if(args.length > 1) {
            duration_seconds = Integer.parseInt(args[1]);
        }
        if(args.length > 2) {
            readOptions(args[2]);
        }

        //Create DB
        OptimizedBlocking b = new OptimizedBlocking();
        b.cerateDB();
        System.out.println("Threads: "+num_threads+", duration: "+duration_seconds+" s");
        System.out.println("Options: reuse_connection="+reuse_connection+", reuse_statements="+reuse_statements
                +", buffered_output="+buffered_output+", async_commit="+async_commit);

        //Execute tasks
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);
        double start, stop;
        start = System.currentTimeMillis();
        b.end_time = (long) start + duration_seconds * 1000L;

        for(int t=0;t<num_threads;t++) {
            executor.execute(b.new Worker());
        }
        executor.shutdown();

        //wait for the tasks to finish
        int last_finished = 0;
        int second = 0;
        while(!executor.isTerminated()) {
            try {
                if(executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            second++;
            int finished = b.tasks_finished.get();
            System.out.println("[STATS] second "+second+": "+(finished-last_finished)+" tx/s, total "+finished+", restarts "+b.restarts.get());
            last_finished = finished;
        }
        stop = System.currentTimeMillis();

        double seconds = (stop-start)/1000;
        System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms with "+num_threads+" threads");
        System.out.println("Finished: "+b.tasks_finished+" (READ "+b.read_finished+", INSERT "+b.insert_finished+", SCAN "+b.scan_finished+", WRITE "+b.write_finished+")");
        System.out.println("Restarts: "+b.restarts);
        System.out.println("Throughput: "+(b.tasks_finished.get()/seconds)+" tx/s");
    }

    public class Worker implements Runnable {
        Connection con = null;
        PreparedStatement ps_read = null;
        PreparedStatement ps_insert = null;
        PreparedStatement ps_scan = null;
        PreparedStatement ps_write = null;
        StringBuilder output = new StringBuilder();

        @Override
        public void run() {
            while(System.currentTimeMillis() < end_time) {
                Blocker task = new Blocker(task_counter.getAndIncrement());
                task.run(this);
            }
            closeConnection();
            flushOutput();
        }

        void openConnection() throws SQLException {
            con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            con.setAutoCommit(false);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            if(async_commit) {
                Statement stmt = con.createStatement();
                stmt.execute("SET synchronous_commit = off;");
                stmt.close();
                con.commit();
            }
            if(reuse_statements) {
                ps_read = con.prepareStatement(SQL_READ);
                ps_insert = con.prepareStatement(SQL_INSERT);
                ps_scan = con.prepareStatement(SQL_SCAN);
                ps_write = con.prepareStatement(SQL_WRITE);
            }
        }

        void closeConnection() {
            try {
                if(con != null) {
                    con.close();
                }
            } catch (SQLException ignored) {}
            con = null;
            ps_read = null;
            ps_insert = null;
            ps_scan = null;
            ps_write = null;
        }

        PreparedStatement getStatement(String type) throws SQLException {
            if(reuse_statements) {
                if(type.equals("READ")) return ps_read;
                if(type.equals("INSERT")) return ps_insert;
                if(type.equals("SCAN")) return ps_scan;
                return ps_write;
            }
            if(type.equals("READ")) return con.prepareStatement(SQL_READ);
            if(type.equals("INSERT")) return con.prepareStatement(SQL_INSERT);
            if(type.equals("SCAN")) return con.prepareStatement(SQL_SCAN);
            return con.prepareStatement(SQL_WRITE);
        }

        void print(String line) {
            if(buffered_output) {
                output.append(line).append('\n');
                if(output.length() > 64 * 1024) {
                    flushOutput();
                }
            } else {
                System.out.println(line);
            }
        }

        void flushOutput() {
            if(output.length() > 0) {
                System.out.print(output);
                output.setLength(0);
            }
        }
    }

    public class Blocker {
        final int task;
        final String type;
        final int [] ids;
        final double [] values;

        public Blocker(int id){
            this.task = id;
            this.ids = new int[num_queries_per_task];
            this.values = new double[num_queries_per_task];

            double p = rand.nextDouble();
            if(p < 0.6) {
                type = "READ";
            } else if(p < 0.7) {
                type = "INSERT";
            } else if(p < 0.8) {
                type = "SCAN";
            } else {
                type = "WRITE";
            }

            if(type.equals("READ") || type.equals("WRITE")) {
                int q = 0;
                while(q < num_queries_per_task) {
                    int new_id = rand.nextInt(num_tuple);
                    boolean already_used = false;
                    for(int k=0;k<q;k++) {
                        if(ids[k] == new_id) {
                            already_used = true;
                        }
                    }
                    if(!already_used) {
                        ids[q] = new_id;
                        values[q] = rand.nextDouble()*max_value;
                        q++;
                    }
                }
            }
            if(type.equals("INSERT")) {
                ids[0] = next_insert_id.getAndIncrement();
                values[0] = rand.nextDouble()*max_value;
            }
        }

        private boolean isRetryable(SQLException e) {
            String state = e.getSQLState();
            if (state == null) return false;
            // 40001 - Serialization failure, 40P01 - Deadlock detected, 55P03 - Lock not available
            return state.equals("40001") || state.equals("40P01") || state.equals(("55P03"));
        }

        public void run(Worker w) {
            int attempt = 0;
            while (true) {
                attempt++;
                PreparedStatement ps = null;
                try {
                    if (w.con == null) {
                        w.openConnection();
                    }
                    ps = w.getStatement(type);

                    if (type.equals("READ")) {
                        double sum = 0;
                        int q = 0;
                        while (q < num_queries_per_task) {
                            ps.setInt(1, ids[q]);
                            ResultSet rs = ps.executeQuery();
                            if (rs.next()) {
                                sum = sum + rs.getDouble(1);
                            }
                            rs.close();
                            q++;
                        }
                        w.con.commit();
                        read_finished.incrementAndGet();
                        w.print("Task " + task + " READ " + Arrays.toString(ids) + " sum = " + sum);
                    }
                    else if (type.equals("INSERT")) {
                        ps.setInt(1, ids[0]);
                        ps.setDouble(2, values[0]);
                        ps.setInt(3, task);
                        ps.execute();
                        w.con.commit();
                        insert_finished.incrementAndGet();
                        w.print("Task " + task + " INSERT id " + ids[0]);
                    }
                    else if (type.equals("SCAN")) {
                        double sum = 0;
                        ResultSet rs = ps.executeQuery();
                        if (rs.next()) {
                            sum = rs.getDouble(1);
                        }
                        rs.close();
                        w.con.commit();
                        scan_finished.incrementAndGet();
                        w.print("Task " + task + " SCAN sum = " + sum);
                    }
                    else {
                        int q = 0;
                        while (q < num_queries_per_task) {
                            ps.setDouble(1, values[q]);
                            ps.setInt(2, task);
                            ps.setInt(3, ids[q]);
                            ps.execute();
                            q++;
                        }
                        w.con.commit();
                        write_finished.incrementAndGet();
                        w.print("Task " + task + " WRITE " + Arrays.toString(ids));
                    }

                    int num_finished = tasks_finished.incrementAndGet();
                    w.print("Done task " + task + " after " + attempt + " attempt(s). I am finisher number " + num_finished);
                    if (!reuse_connection) {
                        w.closeConnection();
                    }
                    return;
                } catch (SQLException e) {
                    try {
                        if (w.con != null) {
                            w.con.rollback();
                        }
                    } catch (SQLException ignored) {
                        w.closeConnection();
                    }
                    if (!reuse_connection) {
                        w.closeConnection();
                    }
                    if (isRetryable(e)) {
                        restarts.incrementAndGet();
                        try {
                            Thread.sleep(BASE_BACKOFF_MS * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    else {
                        System.err.println("Task " + task + " failed: " + e.getMessage());
                        return;
                    }
                } finally {
                    if (!reuse_statements && ps != null) {
                        try {
                            ps.close();
                        } catch (SQLException ignored) {}
                    }
                }
            }
        }
    }
}
