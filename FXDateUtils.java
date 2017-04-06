package sg.sparksystems.ats.awesomelib.datetime;

import sg.sparksystems.ats.awesomelib.application.AwesomeSystemProperties;
import sg.sparksystems.ats.awesomelib.application.Disposable;
import sg.sparksystems.ats.awesomelib.application.DisposableRegistry;
import sg.sparksystems.ats.awesomelib.collections.CollectionFactory;
import sg.sparksystems.ats.awesomelib.lang.Procedure;
import sg.sparksystems.ats.awesomelib.logging.AwesomeLogger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import sg.sparksystems.ats.awesomelib.logging.LoggingUtils;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static sg.sparksystems.ats.awesomelib.logging.LoggingUtils.r;

/**
 * Class FXDateUtils
 *
 */
public class FXDateUtils implements Disposable {
    private static final AwesomeLogger LOGGER = AwesomeLogger.getLogger(FXDateUtils.class);

    public static final String DEFAULT_EOD_WINDOW = "DEFAULT_EOD_WINDOW";
    public static final String DEFAULT_SOD_WINDOW = "DEFAULT_SOD_WINDOW";
    public static final long MAX_TIME_TO_RUN = (long) (1000 * 60 * 2); // 2 Minutes

    // Relative Millis Calculated From The Start Of A Trading Day
    private static final long TRADE_DAY_END_MILLIS = DateTime.now()
                                                             .withHourOfDay(17)
                                                             .withMinuteOfHour(0)
                                                             .withSecondOfMinute(0)
                                                             .withMillisOfSecond(0)
                                                             .getMillisOfDay();

    private static final FXDateUtils INSTANCE = new FXDateUtils();

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, Task> scheduledTasks = Collections.synchronizedMap(CollectionFactory.<String, Task>newLinkedHashMap());

    private FXDate todayTradeDate = calculateTodayTradingDate();
    private long todayCutOver = calculateTradingDateEndTime(todayTradeDate);
    private volatile boolean disposed;

    private FXDateUtils() {
        DisposableRegistry.getInstance().register(this);
    }

    public static FXDateUtils getInstance() {
        return INSTANCE;
    }

    public static FXDate parse(String dateString) {
        return dateString == null ? null : new FXDate(dateString);
    }

    public static FXDate todayTradingDate() {
        return getInstance().getTodayTradingDate();
    }

    public long getTodayCutOver() {
        return todayCutOver;
    }

    public FXDate getTodayTradingDate() {
        if (DateTimeUtils.currentTimeMillis() >= todayCutOver) {
            synchronized (this) {
                if (DateTimeUtils.currentTimeMillis() >= todayCutOver) {
                    todayTradeDate = calculateTodayTradingDate();
                    todayCutOver = calculateTradingDateEndTime(todayTradeDate);
                }
            }
        }

        return todayTradeDate;
    }

    public void addEodTask(FXDate tradeDate, String name, long timeBeforeEod, boolean repeat, Procedure<FXDate> eodTask) {
        addEodTask(tradeDate, name, timeBeforeEod, repeat, false, eodTask);
    }

    public void addEodTask(FXDate tradeDate, String name, long timeBeforeEod, boolean repeat, boolean weekend, Procedure<FXDate> eodTask) {
        if (timeBeforeEod < getDefaultEodWindow()) {
            throw new IllegalArgumentException("Scheduled Time Before EOD MUST Be Greater Than The EOD Window: " +
                                                       TimeUnit.MINUTES.convert(getDefaultEodWindow(), TimeUnit.MILLISECONDS) + " Minutes"
            );
        }
        addTask(true, tradeDate, name, timeBeforeEod, repeat, weekend, eodTask);
    }

    public void addSodTask(FXDate tradeDate, String name, long timeAfterSod, boolean repeat, Procedure<FXDate> sodTask) {
        addSodTask(tradeDate, name, timeAfterSod, repeat, false, sodTask);
    }

    public void addSodTask(FXDate tradeDate, String name, long timeAfterSod, boolean repeat, boolean weekend, Procedure<FXDate> sodTask) {
        if (timeAfterSod < getDefaultSodWindow()) {
            throw new IllegalArgumentException("Scheduled Time After SOD MUST Be Greater Than The SOD Window: " +
                                                       TimeUnit.MINUTES.convert(getDefaultSodWindow(), TimeUnit.MILLISECONDS) + " Minutes"
            );
        }

        addTask(false, tradeDate, name, timeAfterSod, repeat, weekend, sodTask);
    }

    private void addTask(boolean eod, FXDate tradeDate, String name, long time, boolean repeat, boolean weekend, Procedure<FXDate> task) {
        if (time <= 0) {
            throw new IllegalArgumentException("Relative Time MUST Be Greater Than 0! Found: " + time);
        }

        if (scheduledTasks.containsKey(name)) {
            throw new IllegalArgumentException("Task: " + name + " Has Already Been Scheduled! " + scheduledTasks.get(name));
        }

        String type = eod ? "EOD" : "SOD";
        long relativeTime = eod ? -time : time;
        long taskStartTime = todayCutOver + relativeTime;
        DateTime scheduledTime = new DateTime(taskStartTime);
        Task scheduledTask = new Task(name, type, task, repeat, weekend);

        if (tradeDate.equals(todayTradeDate)) {
            if (DateTimeUtils.currentTimeMillis() > taskStartTime - 30000) {
                scheduledTime = scheduledTime.dayOfYear().addToCopy(1);
                LOGGER.warn("Current Scheduled Time Has Passed! " + name + " Will Be Run In The Next Cycle At: " + scheduledTime);
            }

            scheduleTask(scheduledTask, scheduledTime);
        } else {
            throw new IllegalArgumentException("Illegal Trading Date! " + tradeDate + " Is Different From Today: " + todayTradeDate);
        }
    }

    private synchronized void scheduleTask(Task scheduledTask, DateTime scheduledTime) {
        LOGGER.info("Scheduling " + scheduledTask.type + " Task To Run At " + scheduledTime + ": " + scheduledTask.name);
        executorService.schedule(
                new TaskRunnable(scheduledTask, scheduledTime),
                scheduledTime.getMillis() - DateTimeUtils.currentTimeMillis(),
                TimeUnit.MILLISECONDS
        );

        if (scheduledTasks.containsKey(scheduledTask.name)) {
            scheduledTask.status = Task.Status.RESCHEDULED;
        } else {
            scheduledTask.status = Task.Status.SCHEDULED;
            scheduledTasks.put(scheduledTask.name, scheduledTask);
        }
    }

    private FXDate calculateTodayTradingDate() {
        return calculateTradingDate(DateTime.now(AwesomeTimeZone.NYK.jodaTimeZone()));
    }

    public FXDate calculateTradingDate(DateTime referenceTime) {
        DateTime referenceTimeNY = AwesomeTimeZone.NYK.timeFromDateTime(referenceTime);
        if (referenceTimeNY.getMillisOfDay() > TRADE_DAY_END_MILLIS) {
            referenceTimeNY = referenceTimeNY.dayOfYear().addToCopy(1);
        }

        return new FXDate(referenceTimeNY.getYear(), referenceTimeNY.getMonthOfYear(), referenceTimeNY.getDayOfMonth());
    }

    public final long getTradingStartTime(FXDate tradingDate) {
        if (tradingDate.getDayOfWeek() > 5) {
            throw new IllegalArgumentException("Trading Date Cannot Be At Weekend!");
        }

        FXDate previousTradeDateNY = tradingDate.add(-1);
        DateTime previousTradeDateStartNY = new DateTime(previousTradeDateNY.getYear(),
                                        previousTradeDateNY.getMonthOfYear(),
                                        previousTradeDateNY.getDayOfMonth(),
                                        0,
                                        0,
                                        AwesomeTimeZone.NYK.jodaTimeZone()
        );

        return previousTradeDateStartNY.withMillisOfDay((int) TRADE_DAY_END_MILLIS).getMillis();
    }

    /**
     * @param todayTradeDate
     * @return absolute time in milliseconds
     */
    public long calculateTradingDateStartTime(FXDate todayTradeDate) {
        return calculateTradingDateEndTime(todayTradeDate) - TimeUnit.DAYS.toMillis(1);
    }

    /**
     * @param todayTradeDate
     * @return absolute time in milliseconds
     */
    public long calculateTradingDateEndTime(FXDate todayTradeDate) {
        DateTime todayStartTimeNY = new DateTime(
                todayTradeDate.getYear(),
                todayTradeDate.getMonthOfYear(),
                todayTradeDate.getDayOfMonth(),
                0,
                0,
                AwesomeTimeZone.NYK.jodaTimeZone()
        );

        DateTime todayEndTimeNY = todayStartTimeNY.withMillisOfDay((int) TRADE_DAY_END_MILLIS);
        return todayEndTimeNY.getMillis();
    }

    public synchronized String dumpScheduledTasksDetails() {
        StringBuilder sb = new StringBuilder();
        sb.append("\nTrading Day: ").append(todayTradingDate()).append(", Scheduled Tasks:\n");

        LinkedList<Task> tasks = new LinkedList<>(scheduledTasks.values());
        Collections.sort(tasks);

        sb.append(LoggingUtils.r("#", 5))
          .append(LoggingUtils.r("Name", 50))
          .append(LoggingUtils.r("Type", 8))
          .append(LoggingUtils.r("Status", 15))
          .append(LoggingUtils.r("ScheduledTime", 32))
          .append("\n");

        sb.append(LoggingUtils.m("*", 98, '-'))
          .append("\n");

        int count = 0;
        for (Task task : tasks) {
            count++;
            sb.append(LoggingUtils.r(count, 5))
              .append(LoggingUtils.r(task.name, 50))
              .append(LoggingUtils.r(task.type, 8))
              .append(LoggingUtils.r(task.status, 15))
              .append(r(task.scheduledTime, 32))
              .append("\n");
        }

        sb.append(LoggingUtils.m("*", 98, '-')).append("\n");

        sb.append("Trading Day: ").append(todayTradingDate()).append(", Scheduled Tasks Report Generated On ").append(new DateTime());

        return sb.toString();
    }

    @Override
    public void dispose() {
        LOGGER.info("Disposing Financial Date Scheduled Tasks.");
        disposed = true;
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted While Waiting For Tasks To Finish.");
        }

        if (executorService.isTerminated()) {
            LOGGER.info("Finished Disposing Financial Date Scheduled Tasks.");
            scheduledTasks.clear();
        } else {
            LOGGER.warn("Issues Found When Disposing Financial Date Scheduled Tasks! There May Be Tasks Still Running!");
        }
    }

    public static long getDefaultEodWindow() {
        return Long.valueOf(AwesomeSystemProperties.getProperty(DEFAULT_EOD_WINDOW, "0"));
    }
    public static long getDefaultSodWindow() {
        return Long.valueOf(AwesomeSystemProperties.getProperty(DEFAULT_SOD_WINDOW, "0"));
    }

    private static class Task implements Runnable, Comparable<Task> {
        enum Status {
            CREATED,
            SCHEDULED,
            RESCHEDULED,
            SUCCESSFULLY_COMPLETED,
            ERROR,
            FORCED_STOP,
            RUNNING,
            ;
        }
        
        private final String name;
        private final String type;
        private final Procedure<FXDate> procedure;
        private final boolean repeat;
        private final boolean weekend;
        private final AtomicBoolean running = new AtomicBoolean();
        private Status status = Status.CREATED;
        public DateTime scheduledTime;

        private Task(String name, String type, Procedure<FXDate> procedure, boolean repeat, boolean weekend) {
            this.name = name;
            this.type = type;
            this.procedure = procedure;
            this.repeat = repeat;
            this.weekend = weekend;
        }

        public void run() {
            try {
                if (running.compareAndSet(false, true)) {
                    this.status = Status.RUNNING;
                    LOGGER.info("Running " + type + " Task: " + name);
                    procedure.invoke(todayTradingDate());
                    reset(Status.SUCCESSFULLY_COMPLETED);
                    LOGGER.info("Finished Running " + type + " Task: " + name + " Successfully!");
                } else {
                    LOGGER.fatal("Trying To Run Scheduled Task: " + name + " While It's Already Running!");
                }
            } catch (Exception ex) {
                LOGGER.fatal("Failed To Run Scheduled Task: " + name + " Due To " + ex.getMessage(), ex);
                running.set(false);
                this.status = Status.ERROR;
            }
        }

        public void reset(Status status) {
            this.scheduledTime = null;
            this.status = status;
            this.running.set(false);
        }

        @Override
        public int compareTo(Task another) {
            if (this.scheduledTime == null) {
                return -1;
            }

            if (another.scheduledTime == null) {
                return 1;
            }

            return this.scheduledTime.compareTo(another.scheduledTime);
        }

        @Override
        public String toString() {
            return "Task{" +
                    "Name='" + name + "'" +
                    ", Type='" + type + "'" +
                    ", Repeat=" + repeat +
                    ", Status=" + status +
                    ", Scheduled Time=" + scheduledTime +
                    '}';
        }
        
        public boolean isRunning() {
            return running.get();
        }
    }

    private class TaskRunnable implements Runnable {
        private final Task task;
        private final DateTime scheduledTime;
        private Thread taskThread;

        private TaskRunnable(Task task, DateTime scheduledTime) {
            this.task = task;
            this.scheduledTime = scheduledTime;
            this.task.scheduledTime = scheduledTime;
        }

        @Override
        public void run() {
            try {
                this.taskThread = new Thread(task, "FXDateUtils Scheduled Task - " + task.name);
                this.taskThread.start();
                this.taskThread.join(MAX_TIME_TO_RUN);
                if (task.isRunning()) {
                    LOGGER.fatal("Scheduled Task: " + task.name + " Is Still Running After "
                                         + TimeUnit.SECONDS.convert(MAX_TIME_TO_RUN, TimeUnit.MILLISECONDS) + " Seconds!" +
                                         (task.repeat ? " We Will Not Reschedule This Task To Run Again!" : "")
                    );
                    forceStopRunningTask();
                } else {
                    if (task.status == Task.Status.SUCCESSFULLY_COMPLETED && task.repeat) {
                        DateTime nextScheduledTime = scheduledTime.dayOfYear().addToCopy(1);
                        FXDate nextTradeDate = calculateTradingDate(nextScheduledTime);
                        if (nextTradeDate.getDayOfWeek() < 6 || task.weekend) {
                            LOGGER.info("Scheduling Repeated Task: " + task.name);
                            scheduleTask(task, nextScheduledTime);
                        } else {
                            LOGGER.info("Stop Scheduling Repeated Task: " + task.name + ", Tomorrow Is Weekend!");
                        }
                    }
                }
            } catch (InterruptedException ex) {
                LOGGER.error("Interrupted the task scheduler while waiting for running task to finish! " +
                                     " We will force the running task to stop immediately!", ex);
                forceStopRunningTask();
            }

            this.taskThread = null;
        }

        private void forceStopRunningTask() {
            this.taskThread.interrupt();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                // ignore
            }

            try {
                LOGGER.error("Forcing Running Task: " + task.name + " To Stop!");
                this.taskThread.stop();
            } catch (Exception ex) {
                LOGGER.error("Caught exception while forcing running task: " + task.name + " to stop!", ex);
            }

            task.reset(Task.Status.FORCED_STOP);
        }
    }
}
