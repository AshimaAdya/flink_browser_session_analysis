package com.example.flink;

import com.example.flink.datasource.KafkaStreamDataGenerator;
import com.example.flink.model.BrowserSessionEvent;
import com.example.flink.model.BrowserSessionEventDeserializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class BrowserSessionAnalyzer {

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:19092");
        properties.setProperty("group.id", "flink-browser-session");

        // Create a Kafka consumer for browser session data
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "browser-session-events",
                new SimpleStringSchema(),
                properties
        );
        kafkaConsumer.setStartFromLatest();

        /****************************************************************************
         *                  By User / By Action 10 second Summary
         ****************************************************************************/

        DataStream<BrowserSessionEvent> browserSessionData = env.addSource(kafkaConsumer)
                .map(new BrowserSessionEventDeserializer());
        DataStream<Tuple3<String, String, Integer>> summaries = browserSessionData
                .map(x -> new Tuple3<>(x.getUserName(), x.getAction(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT))
                .keyBy(0,1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce((x, y) -> new Tuple3<>(x.f0, x.f1, x.f2 + y.f2));// Configure event time and assign timestamps from the data
        summaries
                .map(new MapFunction<Tuple3<String, String, Integer>, Object>() {
                    @Override
                    public Object map(Tuple3<String, String, Integer> summary) throws Exception {
                        System.out.println("User Action Summary : "
                                + " User : " + summary.f0
                                + ", Action : " + summary.f1
                                + ", Total : " + summary.f2);
                        return null;
                    }
                });
        /****************************************************************************
         *                  Find Duration of Each User Action
         ****************************************************************************/
        DataStream<Tuple3<String, String, Long>> activityDurations = browserSessionData
                .keyBy(event -> event.getUserName())
                .process(new KeyedProcessFunction<String, BrowserSessionEvent, Tuple3<String, String, Long>>() {
                     private transient MapState<String, BrowserSessionEvent> activityStartMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, BrowserSessionEvent> descriptor =
                                new MapStateDescriptor<>("activityStartMapState", String.class, BrowserSessionEvent.class);
                        activityStartMapState = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public void processElement(
                            BrowserSessionEvent event,
                            Context ctx,
                            Collector<Tuple3<String, String, Long>> out) throws Exception {

                        String userName = event.getUserName();
                        String action = event.getAction();

                        // Check if it's a logout event
                        if (action.equals("logout")) {
                            // Clear all state for the user when a logout event occurs
                            activityStartMapState.remove(userName);
                        } else if (!action.equals("login")) {
                            // Check if this event indicates the start of an activity
                            if (!activityStartMapState.contains(userName)) {
                                // This event starts an activity
                                activityStartMapState.put(userName, event);
                            } else {
                                // There's a previous start event for this user
                                BrowserSessionEvent startEvent = activityStartMapState.get(userName);

                                // Calculate duration and emit the result
                                long duration = event.getTimestamp() - startEvent.getTimestamp();
                                out.collect(Tuple3.of(userName, startEvent.getAction(), duration));

                                // Remove the start event to prepare for the next activity
                                activityStartMapState.remove(userName);
                            }
                        }
                    }
                });

        activityDurations.map(new MapFunction<Tuple3<String, String, Long>, Object>() {
            @Override
            public Object map(Tuple3<String, String, Long> summary) throws Exception {
                System.out.println("User Activity Duration : "
                        + " User : " + summary.f0
                        + ", Action : " + summary.f1
                        + ", Duration : " + summary.f2);
                return null;
            }
        });

        Thread genThread = new Thread(new KafkaStreamDataGenerator());
        genThread.start();
        env.execute();
    }
}
