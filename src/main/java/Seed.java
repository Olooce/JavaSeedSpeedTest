import com.github.javafaker.Faker;

import java.sql.*;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Seed {
    private static final int NUM_THREADS = 4; // Number of threads
    static final int BATCH_SIZE = 1; // Batch size
    static final long TARGET_RECORDS = 1_000_000; // Target number of records
    static final AtomicLong populatedRecords = new AtomicLong(); // Populated records counter
    static final AtomicLong activeThreads = new AtomicLong(); // Active threads counter
    static ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS); // Thread pool executor

    public static void main(String[] args) throws InterruptedException {
        long startTime = System.currentTimeMillis(); // Start time for the process

        // Status update thread
        new Thread(() -> {
            try {
                long lastTime = startTime;
                long lastRecords = 0;

                while (!executor.isTerminated()) {
                    long currentTime = System.currentTimeMillis();
                    long elapsedTime = currentTime - startTime;
                    long timeTaken = currentTime - lastTime;

                    double totalTransactionsPerMinute = (Seed.populatedRecords.get() / (elapsedTime / 1000.0 / 60));
                    double intervalTransactionsPerMinute = ((Seed.populatedRecords.get() - lastRecords) / (timeTaken / 1000.0 / 60));

                    long elapsedSeconds = elapsedTime / 1000;
                    long hours = elapsedSeconds / 3600;
                    long minutes = (elapsedSeconds % 3600) / 60;
                    long seconds = elapsedSeconds % 60;

                    System.out.println("\nInserted " + Seed.populatedRecords.get() + " records");
                    System.out.println("Transactions Per Minute (Total): " + totalTransactionsPerMinute);
                    System.out.println("Transactions Per Minute (Interval): " + intervalTransactionsPerMinute);
                    System.out.println("Active Threads: " + Seed.activeThreads.get());
                    System.out.println("Elapsed Time: " + hours + "H " + minutes + "M " + seconds + "S");

                    lastTime = currentTime;
                    lastRecords = Seed.populatedRecords.get();
                    Thread.sleep(5000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();



        // Submit tasks to the executor
        for (int i = 0; i < TARGET_RECORDS / BATCH_SIZE; i++) {
            executor.execute(new Worker());
            TimeUnit.MICROSECONDS.sleep(500);
        }

        // Shutdown the executor and await termination
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.DAYS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Final statistics
        long endTime = System.currentTimeMillis();
        long elapsedTime = endTime - startTime;
        double transactionsPerMinute = (populatedRecords.get() / (elapsedTime / 1000.0 / 60));
        long elapsedSeconds = elapsedTime / 1000;
        long hours = elapsedSeconds / 3600;
        long minutes = (elapsedSeconds % 3600) / 60;
        long seconds = elapsedSeconds % 60;

        System.out.println("\nSuccessfully Populated " + populatedRecords.get() + " records");
        System.out.println("Elapsed Time: " + hours + "H " + minutes + "M " + seconds + "S");
        System.out.println("Transactions Per Minute: " + transactionsPerMinute);
    }
}

class Worker implements Runnable {
    private static final Random random = new Random();
    private static final Faker faker = new Faker();

    @Override
    public void run() {
        task();
    }

    public static void task() {
        Seed.activeThreads.incrementAndGet();
        try (Connection conn = Data_Source.getConnection()) {
            conn.setAutoCommit(false);

            String insertEmp = "INSERT INTO employees (full_name, phone_number, email_address, gender,job_role_id,employment_date, emp_status_code) VALUES (?,?,?,?,?,?,?)";
            PreparedStatement pstmtEmp = conn.prepareStatement(insertEmp);
            for(int i =0; i < Seed.BATCH_SIZE; i++) {
                pstmtEmp.setString(1, faker.name().fullName());
                pstmtEmp.setString(2, faker.phoneNumber().cellPhone());
                pstmtEmp.setString(3, faker.internet().emailAddress());
                pstmtEmp.setString(4, faker.demographic().sex());
                pstmtEmp.setInt(5, random.nextInt(100));
                pstmtEmp.setDate(6, new java.sql.Date(faker.date().past(10 * 365, TimeUnit.DAYS).getTime()));
                pstmtEmp.setString(7, String.valueOf(random.nextInt(5)));
                pstmtEmp.addBatch();
            }

            pstmtEmp.executeBatch();

            Seed.populatedRecords.getAndAdd(Seed.BATCH_SIZE);
            Seed.activeThreads.decrementAndGet();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

