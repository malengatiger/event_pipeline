package org.apache.beam.examples;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.examples.models.Event;
import org.apache.beam.examples.models.FlatEvent;
import org.apache.beam.examples.util.E;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class PubSubToGCS {
    static final Logger LOGGER = Logger.getLogger(PubSubToGCS.class.getSimpleName());
    static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    /*
     * Define your own configuration options. Add your own arguments to be processed
     * by the command-line parser, and specify default values for them.
     */
    public interface PubSubToGcsOptions extends PipelineOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Required
        String getInputTopic();

        void setInputTopic(String value);

        @Description("Output file's window size in number of minutes.")
        @Default.Integer(2)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("Path of the output file including its filename prefix.")
        @Required
        String getOutput();

        void setOutput(String value);

//        @Description("GCP Region")
//        @Default.String("europe-west1")
//        String getRegion();
//
//        void setRegion(String value);

        @Description("BigQuery Data Set")
        @Default.String("events_dataset.events_table")
        String getBigQueryDS();

        void setBigQueryDS(String value);
//        @Description("Output Topic")
//        @Default.String("=projects/thermal-effort-366015/topics/flatEventTopic")
//        String getOutputTopic();
//        void setOutputTopic(String value);
    }

    // The DoFn to count length of each element in the input PCollection.
    static class FlattenEventRecord extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String json,
                                   OutputReceiver<String> out) {

            Event e = GSON.fromJson(json, Event.class);
            FlatEvent f = new FlatEvent();
            if (e.getCityPlace() != null) {
                f.setCityId(e.getCityPlace().cityId);
                f.setCityName(e.getCityPlace().cityName);
                f.setLatitude(e.getCityPlace().geometry.location.lat);
                f.setLongitude(e.getCityPlace().geometry.location.lng);
                f.setPlaceId(e.getCityPlace().place_id);
                f.setPlaceName(e.getCityPlace().name);
                f.setTypes(e.getCityPlace().types);
                f.setVicinity(e.getCityPlace().vicinity);
            } else {
                LOGGER.info(E.RED_DOT + E.RED_DOT + " CityPlace is NULL");
                LOGGER.info(json);
            }
            f.setAmount(e.getAmount());
            f.setDate(e.getDate());
            f.setEventId(e.getEventId());
            f.setLongDate(e.getLongDate());
            f.setRating(e.getRating());

            LOGGER.info(E.LEAF + E.LEAF + E.ORANGE_HEART
                    + " Event flattened: " + f.getRating() + " " + f.getPlaceName() + ", " + f.getCityName());
            String outJson = GSON.toJson(f);
            if (f.getCityName().equalsIgnoreCase(f.getPlaceName()))  {
                LOGGER.info(E.AMP + E.AMP + E.AMP + " Record ignored: " + f.getCityName()
                + ", " + f.getPlaceName());
            } else {
                out.output(outJson);
            }
        }
    }

    static class WriteToBigQuery extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String json,
                                   OutputReceiver<String> out) {
            BigQueryIO.Write<TableRow> o = BigQueryIO.writeTableRows()
                    .to(biqQueryName)
                    .withCreateDisposition(CREATE_IF_NEEDED)
                    .withWriteDisposition(WRITE_APPEND)
                    .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                    .withSchema(getTableSchema());
            try {
                LOGGER.info(E.YELLOW_STAR + " Table: " + Objects.requireNonNull(o.getTable()).get().toPrettyString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            FlatEvent f = GSON.fromJson(json, FlatEvent.class);
            LOGGER.info(E.RED_APPLE + E.RED_APPLE +
                    " BigQuery Record, rating: " + f.getRating() + " - " + f.getPlaceName());

            out.output(json);
        }
    }

    static PubSubToGcsOptions options;
    static String biqQueryName;

    public static void main(String[] args) throws IOException {
        // The maximum number of shards when writing output.
        int numShards = 1;

        options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToGcsOptions.class);
        biqQueryName = "thermal-effort-366015" + "." + options.getBigQueryDS();
        LOGGER.info(E.RED_APPLE + E.RED_APPLE + " output: " + options.getOutput());

        options.setStreaming(true);
        //options.setRunner(DataflowRunner.class);

        Pipeline pipeline = Pipeline.create(options);
//        pipeline.apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));
//        PCollection<String> p1 = pipeline.apply("Read PubSub Messages", PubsubIO.readStrings()
//                .fromTopic(options.getInputTopic()));
//        PCollection<String> p2 =  p1.apply("Flatten Event Record", ParDo.of(new FlattenEventRecord()));

        pipeline
                // 1) Read string messages from a Pub/Sub topic.
                .apply("Read PubSub Messages", PubsubIO.readStrings()
                        .fromTopic(options.getInputTopic()))
                .apply("Flatten Event Record", ParDo.of(new FlattenEventRecord()))
                // 2) Group the messages into fixed-sized minute intervals.
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
//                .apply("Write Event to BigQuery",ParDo.of(new WriteToBigQuery()))
//                .apply("Sending Pub/sub",ParDo.of(new WriteToPubSub()))
                // 3) Write one file to GCS for every window of messages.
                .apply("Write file to GCS: "+options.getOutput(), new WriteOneFilePerWindow(
                        options.getOutput(), numShards));

        // Execute the pipeline and wait until it finishes running.
        pipeline.run().waitUntilFinish();
    }

    private static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("eventId").setType("STRING"));
        fields.add(new TableFieldSchema().setName("placeId").setType("STRING"));
        fields.add(new TableFieldSchema().setName("placeName").setType("STRING"));
        fields.add(new TableFieldSchema().setName("cityId").setType("STRING"));
        fields.add(new TableFieldSchema().setName("cityName").setType("STRING"));
        fields.add(new TableFieldSchema().setName("rating").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("date").setType("STRING"));
        fields.add(new TableFieldSchema().setName("amount").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("latitude").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("longitude").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("vicinity").setType("STRING"));
        return new TableSchema().setFields(fields);
    }


    private static PubsubIO.Read<String> readPubSub(PubSubToGcsOptions options) {
        PubsubIO.Read<String> io = PubsubIO.readStrings()
                .fromTopic(options.getInputTopic());
        LOGGER.info(E.RED_APPLE + E.RED_APPLE +
                " PubsubIO.Read<String> topic = "
                + options.getInputTopic() + " output: " + options.getOutput());

        return io;
    }


}
