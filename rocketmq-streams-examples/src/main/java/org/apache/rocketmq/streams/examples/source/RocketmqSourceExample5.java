import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.json.JSONObject;

public class FileSourceExample {
 public static void main(String[] args) {
  DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");
  source.fromFile("data.txt", false)
          .map(message -> message)
          .toPrint(1)
          .start();
                .map(message -> message)
          .toPrint(1)
          .start();

  source.fromFile("studentScores.txt", true)
          .map(message -> message)
          .filter(message -> ((JSONObject) message).getInt("score") > 90)
          .selectFields("name","subject")
          .toFile("./rocketmq-streams-examples/src/main/resources/results.txt")
          .start();
 }
}