package org.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read;
import static org.apache.beam.sdk.transforms.Watch.Growth.afterTimeSinceNewOutput;

public class Main {
    public static void main(String[] args) {

        ScenarioOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(ScenarioOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("create PCollections from input ", TextIO.read().from(options.getSource()).watchForNewFiles(Duration.standardMinutes(1),
                        afterTimeSinceNewOutput(Duration.standardHours(1))))
                .apply("Write in Pub/Sub", PubsubIO.writeStrings().to(options.getInputTopic()));
        Read<String> messageRead = PubsubIO.readStrings().fromSubscription("projects/" + options.getProjectName()+ "/subscriptions/" + options.getSubscriptionId());

        pipeline.apply("Read from subscription", messageRead)
                .apply("Mapping to TableRow", ParDo.of(new saveToBigquery()))
                .apply("Save in BigQuery", BigQueryIO.writeTableRows().to(options.getProjectName() + "." + "streamingtest.pubsubtest")  //name of the table in bigQuery
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER) // avoid recreating the table
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        pipeline.run();

    }
    public interface ScenarioOptions extends PipelineOptions {

        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        String getInputTopic();

        void setInputTopic(String input);
        @Description("subscriptionId")
        @Validation.Required
        String getSubscriptionId();

        void setSubscriptionId(String value);

        @Description("project name")
        @Validation.Required
        String getProjectName();

        void setProjectName(String value);

        @Description("Source to Read")
        @Validation.Required
        String getSource();

        void setSource(String source);
    }

    private static class saveToBigquery extends DoFn<String, TableRow> {
        @ProcessElement
        public void processing(@Element String elem, ProcessContext pc) {
            TableRow tableRow = new TableRow();
            tableRow.set("messageid", elem + ":" + pc.timestamp().toString());
            tableRow.set("message", elem);
            tableRow.set("messageprocessingtime", pc.timestamp().toString());
            pc.output(tableRow);
        }
    }
}

