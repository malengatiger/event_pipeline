package org.apache.beam.examples;


import java.io.IOException;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.examples.models.Event;
import org.apache.beam.examples.models.FlatEvent;
import org.apache.beam.examples.util.E;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

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
        @Default.Integer(1)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("Path of the output file including its filename prefix.")
        @Required
        String getOutput();

        void setOutput(String value);

        @Description("GCP Region")
        @Default.String("europe-west1")
        String getRegion();

        void setRegion(String value);
    }

    // The DoFn to count length of each element in the input PCollection.
    static class FlattenEventRecord extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String word,
                                   OutputReceiver<String> out) {

           Event e = GSON.fromJson(word,Event.class);
           FlatEvent  f = new FlatEvent();
           f.setAmount(e.getAmount());
           f.setCityId(e.getCityPlace().cityId);
           f.setCityName(e.getCityPlace().cityName);
           f.setDate(e.getDate());
           f.setEventId(e.getEventId());
           f.setLatitude(e.getCityPlace().geometry.location.lat);
           f.setLongDate(e.getLongDate());
           f.setLongitude(e.getCityPlace().geometry.location.lng);
           f.setPlaceId(e.getCityPlace().place_id);
           f.setPlaceName(e.getCityPlace().name);
           f.setRating(e.getRating());
           f.setTypes(e.getCityPlace().types);
           f.setVicinity(e.getCityPlace().vicinity);
           String outJson = GSON.toJson(f);
            LOGGER.info(E.CHECK + E.CHECK +
                    " Flattened Record, rating: " + f.getRating() + " - " + f.getPlaceName());

            out.output(outJson);
        }
    }

    public static void main(String[] args) throws IOException {
        // The maximum number of shards when writing output.
        int numShards = 1;

        PubSubToGcsOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToGcsOptions.class);

        options.setStreaming(true);
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                // 1) Read string messages from a Pub/Sub topic.
                .apply("Read PubSub Messages", readPubSub(options))
                // 2) Group the messages into fixed-sized minute intervals.
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
                .apply(ParDo.of(new FlattenEventRecord()))
                // 3) Write one file to GCS for every window of messages.
                .apply("Write File to GCS", writeToGCS(numShards, options));

        // Execute the pipeline and wait until it finishes running.
        pipeline.run().waitUntilFinish();
    }

    private static WriteOneFilePerWindow writeToGCS(int numShards, PubSubToGcsOptions options) {
        LOGGER.info(E.LEAF + " Writing FlatEvent to GCS ...");
        return new WriteOneFilePerWindow(
                options.getOutput(), numShards);
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
