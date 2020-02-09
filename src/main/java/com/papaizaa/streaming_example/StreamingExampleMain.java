package com.papaizaa.streaming_example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.papaizaa.streaming_example.generated_pb.Events;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.transforms.windowing.AfterProcessingTime.pastFirstElementInPane;

public class StreamingExampleMain {

    public static final int ALLOWED_LATENESS_SEC = 0;
    public static final int SESSION_WINDOW_GAP_DURATION = 10;

    public static void main(String[] args) {
        StreamingPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamingPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        pipeline.getCoderRegistry().registerCoderForClass(Events.Event.class, ProtoCoder.of(Events.Event.class));

        Trigger trigger = Repeatedly.forever(pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(15))
                .orFinally(AfterWatermark.pastEndOfWindow()));

        Window<KV<String, Events.Event>> sessionWindow =
                Window.<KV<String, Events.Event>>into(Sessions.withGapDuration(
                        Duration.standardMinutes(SESSION_WINDOW_GAP_DURATION)))
                        .triggering(trigger)
                        .withAllowedLateness(Duration.standardSeconds(ALLOWED_LATENESS_SEC))
                        .accumulatingFiredPanes();

        PCollection<Events.Event> books = pipeline.apply("Read events",
                PubsubIO.readProtos(Events.Event.class).fromSubscription(options.getBookSubscriptionName()));

        PCollection<Events.Event> groceries = pipeline.apply("Read events",
                PubsubIO.readProtos(Events.Event.class).fromSubscription(options.getGrocerySubscriptionName()));

        final TupleTag<TableRow> eventsTag = new TupleTag<TableRow>(){};

        // Flatten two PubSub streams into one collection
        PCollection<Events.Event> events = PCollectionList.of(books).and(groceries).apply(Flatten.pCollections());

        PCollection<KV<String, Events.Event>> tuple =
                events.apply("Key Value Split", ParDo.of(new KeyValueSplit.Parse()));

        tuple.apply("Apply to Session Window", sessionWindow)
                .apply("Group By Key", GroupByKey.create())
                .apply("Parse And Format Pub Sub Message", ParDo.of(new CombineEvents.Combine()))
                .apply("Send to Backend", PubsubIO.writeProtos(Events.Output.class).to(options.getOutputTopic()));


        pipeline.run();
    }

    public interface StreamingPipelineOptions extends PipelineOptions {

        @Description("pubsub books subscription name")
        @Default.String("")
        String getBookSubscriptionName();

        void setBookSubscriptionName(String value);

        @Description("pubsub groceries subscription name")
        @Default.String("")
        String getGrocerySubscriptionName();

        @Description("PubSub Topic to write the output to")
        @Default.String("")
        ValueProvider<String> getOutputTopic();

        void setOutputTopic(String value);


    }

    // BigQuery table schema for the DoorEvents Table.
    public static TableSchema getSessionSalesSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("UserID").setType("STRING"));
        fields.add(new TableFieldSchema().setName("TotalSalesInWeek").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("StartOfWeek").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("EndOfWeek").setType("TIMESTAMP"));

        return new TableSchema().setFields(fields);
    }

}
