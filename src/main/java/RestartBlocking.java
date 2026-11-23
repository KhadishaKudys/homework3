import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RestartBlocking {
    public static int num_threads = 4;
    public static int duration_seconds = 30;
    public static int num_queries_per_task = 3;
    private static final long BASE_BACKOFF_MS = 0;

    Connection con;
    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;
    AtomicInteger tasks_finished;
    AtomicInteger task_counter;
    AtomicInteger next_insert_id;
    AtomicInteger restarts;
    AtomicInteger read_finished;
    AtomicInteger insert_finished;
    AtomicInteger scan_finished;
    AtomicInteger write_finished;

    public RestartBlocking(){
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

    public static void main(String[] args){
        if(args.length > 0) {
            num_threads = Integer.parseInt(args[0]);
        }
        if(args.length > 1) {
            duration_seconds = Integer.parseInt(args[1]);
        }

        //Create DB
        RestartBlocking b = new RestartBlocking();
        b.cerateDB();
        System.out.println("Threads: "+num_threads+", duration: "+duration_seconds+" s");

        //Execute tasks
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor) Executors.newFixedThreadPool(num_threads);
        double start, stop;
        start = System.currentTimeMillis();
        long end_time = (long) start + duration_seconds * 1000L;

        for(int t=0;t<num_threads;t++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    while(System.currentTimeMillis() < end_time) {
                        Blocker task = b.new Blocker(b.task_counter.getAndIncrement());
                        task.run();
                    }
                }
            });
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

    public class Blocker implements Runnable  {
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

        @Override
        public void run() {
            int attempt = 0;
            while (true) {
                attempt++;
                Connection con = null;
                PreparedStatement ps = null;
                try {
                    con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
                    con.setAutoCommit(false);
                    con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                    if (type.equals("READ")) {
                        ps = con.prepareStatement("SELECT data_value FROM blocking_data WHERE data_id = ?;");
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
                        con.commit();
                        read_finished.incrementAndGet();
                        System.out.println("Task " + task + " READ " + Arrays.toString(ids) + " sum = " + sum);
                    }
                    else if (type.equals("INSERT")) {
                        ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?);");
                        ps.setInt(1, ids[0]);
                        ps.setDouble(2, values[0]);
                        ps.setInt(3, task);
                        ps.execute();
                        con.commit();
                        insert_finished.incrementAndGet();
                        System.out.println("Task " + task + " INSERT id " + ids[0]);
                    }
                    else if (type.equals("SCAN")) {
                        ps = con.prepareStatement("SELECT sum(data_value) FROM blocking_data;");
                        double sum = 0;
                        ResultSet rs = ps.executeQuery();
                        if (rs.next()) {
                            sum = rs.getDouble(1);
                        }
                        rs.close();
                        con.commit();
                        scan_finished.incrementAndGet();
                        System.out.println("Task " + task + " SCAN sum = " + sum);
                    }
                    else {
                        ps = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = " + task + " WHERE data_id = ?;");
                        int q = 0;
                        while (q < num_queries_per_task) {
                            ps.setDouble(1, values[q]);
                            ps.setInt(2, ids[q]);
                            ps.execute();
                            q++;
                        }
                        con.commit();
                        write_finished.incrementAndGet();
                        System.out.println("Task " + task + " WRITE " + Arrays.toString(ids));
                    }

                    int num_finished = tasks_finished.incrementAndGet();
                    System.out.println("Done task " + task + " after " + attempt + " attempt(s). I am finisher number " + num_finished);
                    return;
                } catch (SQLException e) {
                    try {
                        if (con != null) {
                            con.rollback();
                        }
                    } catch (SQLException ignored) {}
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
                    try {
                        if (ps != null) {
                            ps.close();
                        }
                        if (con != null) {
                            con.close();
                        }
                    } catch (SQLException ignored) {}
                }
            }
        }
    }
}