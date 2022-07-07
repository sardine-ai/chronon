package ai.chronon.online;

import scala.collection.JavaConverters;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class JavaResponse {
    public JavaRequest request;
    public Map<String, Object> values;

    public JavaResponse(Fetcher.Response scalaResponse){
        this.request = new JavaRequest(scalaResponse.request());
        if (scalaResponse.values().isFailure()) {
            values = new HashMap<>();
            Throwable t = scalaResponse.values().failed().get();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos, true);
            t.printStackTrace(ps);
            String data = baos.toString();
            values.put(request.name + "_exception", data);
        } else if (scalaResponse.values().get() == null || scalaResponse.values().get().isEmpty()) {
            this.values = Collections.emptyMap();
        } else {
            this.values = JavaConverters.mapAsJavaMapConverter(scalaResponse.values().get()).asJava();
        }
    }
}