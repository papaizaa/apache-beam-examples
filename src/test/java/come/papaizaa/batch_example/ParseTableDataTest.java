package come.papaizaa.batch_example;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.api.services.bigquery.model.TableRow;
import com.papaizaa.batch_example.ParseTableData;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class ParseTableDataTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void testFailedTableRowData(){

        ImmutableList<TableRow> inputDoorRow = ImmutableList.of(
                new TableRow()
                        .set("UserID", "22222"),
                new TableRow()
                        .set("Price", 20.0));

        PCollection<KV<String, Double>> out = testPipeline
                .apply("Create Stream", Create.of(inputDoorRow))
                .apply("Parse pipeline",
                        ParDo.of(new ParseTableData.Parse()));

        PAssert.that(out).empty();

        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testParseTableRowDataSuccess(){

        ImmutableList<TableRow> inputDoorRow = ImmutableList.of(
                new TableRow()
                        .set("UserID", "22222")
                        .set("Price", 20.1),
                new TableRow()
                        .set("UserID", "22222")
                        .set("Price", 20.0));

        PCollection<KV<String, Double>> out = testPipeline
                .apply("Create Stream", Create.of(inputDoorRow))
                .apply("Parse pipeline",
                        ParDo.of(new ParseTableData.Parse()));

        PAssert.that(out).containsInAnyOrder(KV.of("22222", 20.1), KV.of("22222", 20.0));

        testPipeline.run().waitUntilFinish();
    }

}
