import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import org.scoja.client.LoggingException;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CloudwatchConsumer {

  private static AWSLogs logsClient;

  private static long timestamp;
  private static final SimpleDateFormat sdf = new SimpleDateFormat("dd-MM" +
      "-yyyy hh:mm:ss");


  final static String host = "collector-eu.devo.io";
  final static String jks = "/Users/dgmoran/Documents/logtrust/git/jSender" +
      "/src/test/resources/olacaradebola.jks";
  final static int secPort = 443;
  final static String jksPass = "ca2f7";
  final static String tag = "my.app.dani.test";

  private static LTSender sender;

  static {
    AWSLogsClientBuilder builder = AWSLogsClientBuilder.standard();
    logsClient = builder.withCredentials
        (new EnvironmentVariableCredentialsProvider())
        .withRegion(Regions.EU_WEST_1)
        .build();

    ZonedDateTime zonedDateTimeNow = ZonedDateTime.now(ZoneId.of("Europe" +
        "/Madrid"));
    zonedDateTimeNow = zonedDateTimeNow.minusMinutes(5);
    timestamp = zonedDateTimeNow.toInstant().toEpochMilli();
    System.out.println("Init at " + zonedDateTimeNow.toString());
  }

  public static void main(String[] args) {
    try {
      sender = new LTSender(host, secPort, tag, jks, jksPass);
      ScheduledExecutorService executorService =
          Executors.newSingleThreadScheduledExecutor();
      Runnable runnable = () -> getLastEventsBulk();
      executorService.scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void getLastEventsBulk() {

    try {
      System.out.println("Getting events at " + sdf.format(new Date(timestamp)));
      GetLogEventsRequest request = new GetLogEventsRequest()
          .withStartTime(timestamp)
          .withLogGroupName("/var/log/messages")
          .withLogStreamName("i-0e2b7120944613352");
      GetLogEventsResult result = logsClient.getLogEvents(request);

      List<OutputLogEvent> events = result.getEvents();

      sentEventsToDevo(events);
      if (events.size() > 0) {
        timestamp =
            result.getEvents().get(events.size() - 1).getTimestamp() + 1;
      }

    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  private static void sentEventsToDevo(List<OutputLogEvent> events) {

    events.forEach(outputLogEvent -> {
      try {
        System.out.println(outputLogEvent.getMessage());
        sender.log(outputLogEvent.getMessage());
      } catch (LoggingException e) {
        e.printStackTrace();
      }
    });
  }

}
