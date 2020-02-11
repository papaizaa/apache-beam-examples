package com.papaizaa.batch_example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public class BatchExampleMain {

    public static final Logger LOG = getLogger(BatchExampleMain.class);
    public static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss zz");
    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");

    public static void main(String[] args) {
        BatchPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(BatchPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        LocalDateTime endDate = LocalDateTime.now()
                .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
        LocalDateTime startDate = endDate.minusDays(7);

        final PCollectionView<LocalDateTime> dateStartView = pipeline
                .apply("DateStart to Pcollection", Create.of(startDate))
                .apply(View.asSingleton());
        final PCollectionView<LocalDateTime> dateEndView = pipeline
                .apply("DateEnd to Pcollection", Create.of(endDate))
                .apply(View.asSingleton());


        PCollection<TableRow> bookData = pipeline.apply("Read Book Table", BigQueryIO.readTableRows()
                .fromQuery(
                        String.format("SELECT UserID, Price FROM `%1$s` WHERE date_time >= \"%2$s\" AND date_time < \"%3$s\"",
                                options.getBookSalesInputTable(),
                                startDate.toString(),
                                endDate.toString())
                )
                .usingStandardSql()).setCoder(TableRowJsonCoder.of());

        PCollection<TableRow> groceryData = pipeline.apply("Read Groceries Table", BigQueryIO.readTableRows()
                .fromQuery(
                        String.format("SELECT UserID, Price FROM `%1$s` WHERE date_time >= \"%2$s\" AND date_time < \"%3$s\"",
                                options.getGrocerySalesInputTable(),
                                startDate.toString(),
                                endDate.toString())
                )
                .usingStandardSql()).setCoder(TableRowJsonCoder.of());

        PCollection<KV<String, Iterable<Double>>> bookPurchases =
                bookData
                        .apply("Parse Sound Events", ParDo.of(new ParseTableData.Parse()))
                        .apply("Group Books", GroupByKey.create());

        PCollection<KV<String, Iterable<Double>>> groceryPurchases =
                groceryData
                        .apply("Parse Grocery Purchases", ParDo.of(new ParseTableData.Parse()))
                        .apply("Group Groceries", GroupByKey.create());

        TupleTag<Iterable<Double>> booksTag = new TupleTag<>();
        TupleTag<Iterable<Double>> groceryTag = new TupleTag<>();

        KeyedPCollectionTuple.of(booksTag, bookPurchases)
                .and(groceryTag, groceryPurchases)
                .apply("CoGroupByKey", CoGroupByKey.create())
                .apply("Join Data", ParDo.of(new JoinEvents.Join(booksTag, groceryTag, dateStartView, dateEndView))
                        .withSideInputs(dateStartView, dateEndView))
                .apply("WriteToBigQuery",
                        BigQueryIO.writeTableRows().to(options.getWeeklyAggregatedSalesInputTable())
                                .withSchema(getWeeklySalesSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    }

    // BigQuery table schema for the DoorEvents Table.
    public static TableSchema getWeeklySalesSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("UserID").setType("STRING"));
        fields.add(new TableFieldSchema().setName("TotalSalesInWeek").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("StartOfWeek").setType("TIMESTAMP"));
        fields.add(new TableFieldSchema().setName("EndOfWeek").setType("TIMESTAMP"));

        return new TableSchema().setFields(fields);
    }

    public interface BatchPipelineOptions extends PipelineOptions {

        @Description("The location of the BigQuery table for Book Sales data")
        ValueProvider<String> getBookSalesInputTable();

        void setBookSalesInputTable(ValueProvider<String> value);

        @Description("The location of the BigQuery table for Grocery Sales data")
        ValueProvider<String> getGrocerySalesInputTable();

        void setGrocerySalesInputTable(ValueProvider<String> value);

        @Description("The location of the BigQuery table for Weekly Aggregated Sales data")
        ValueProvider<String> getWeeklyAggregatedSalesInputTable();

        void setWeeklyAggregatedSalesInputTable(ValueProvider<String> value);
    }
}
