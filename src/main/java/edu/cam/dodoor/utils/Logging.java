package edu.cam.dodoor.utils;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Random;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.google.common.base.Joiner;

public class Logging {
  public final static String AUDIT_LOGGER_NAME = "audit";
  public final static String AUDIT_LOG_FILENAME_FORMAT = "sparrow_audit.%d.%d.log";
  public final static String AUDIT_LOG_FORMAT = "%c\t%m%n";

  private static Joiner _paramJoiner = Joiner.on(",").useForNull("null");
  private static Joiner _auditParamJoiner = Joiner.on("\t");
  private static Joiner _auditEventParamJoiner = Joiner.on(":");

  /**
   * Sets up audit logging to log to a file named based on the current time (in
   * ms).
   *
   * The logger is configured to effectively ignore the log level.
   *
   * @throws IOException
   *           if the audit log file could not be opened for writing.
   */
  public static void configureAuditLogging() throws IOException {
    PatternLayout layout = new PatternLayout(AUDIT_LOG_FORMAT);
    // This assumes that no other daemon will be started within 1 millisecond.
    String filename = String.format(AUDIT_LOG_FILENAME_FORMAT,
        System.currentTimeMillis(), new Random().nextInt(Integer.MAX_VALUE));
    FileAppender fileAppender = new FileAppender(layout, filename);
    Logger auditLogger = Logger.getLogger(Logging.AUDIT_LOGGER_NAME);
    auditLogger.addAppender(fileAppender);
    auditLogger.setLevel(Level.ALL);
    /*
     * We don't want audit messages to be appended to the main appender, which
     * is intended for potentially user-facing messages.
     */
    auditLogger.setAdditivity(false);
  }

  /** Returns the total count of garbage collections. */
  public static long getGCCount() {
    long totalGarbageCollections = 0;

    for (GarbageCollectorMXBean gc : ManagementFactory
        .getGarbageCollectorMXBeans()) {
      long count = gc.getCollectionCount();
      if (count >= 0) {
        totalGarbageCollections += count;
      }
    }
    return totalGarbageCollections;
  }

  /** Returns the total time that has been spent on garbage collection. */
  public static long getGCTime() {
    long garbageCollectionTime = 0;

    for (GarbageCollectorMXBean gc : ManagementFactory
        .getGarbageCollectorMXBeans()) {
      long time = gc.getCollectionTime();
      if (time >= 0) {
        garbageCollectionTime += time;
      }
    }
    return garbageCollectionTime;
  }

  /**
   * Returns a log string for the given event, starting with the epoch time.
   */
  public static String auditEventString(Object... params) {
    return _auditParamJoiner.join(System.currentTimeMillis(),
        _auditEventParamJoiner.join(params));
  }

  /**
   * Returns a logger to be used for audit logging messages for the given class.
   */
  @SuppressWarnings("rawtypes")
  public static Logger getAuditLogger(Class clazz) {
    return Logger.getLogger(String.format("%s.%s", AUDIT_LOGGER_NAME,
        clazz.getName()));
  }

  /**
   * Return a function name (determined via reflection) and all its parameters
   * (passed) in a consistent stringformat. Very helpful in logging function
   * calls throughout our program.
   */
  public static String functionCall(Object... params) {
    String name = Thread.currentThread().getStackTrace()[2].getMethodName();
    return name + ": [" + _paramJoiner.join(params) + "]";
  }
}
