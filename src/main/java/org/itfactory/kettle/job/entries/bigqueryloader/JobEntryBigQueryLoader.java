package org.itfactory.kettle.job.entries.bigqueryloader;

import com.google.cloud.bigquery.*;
import org.itfactory.kettle.google.bigquery.SupportedLoadFormat;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@JobEntry(
        id = "DemoJobEntry",
        name = "DemoJobEntry.Name",
        description = "DemoJobEntry.TooltipDesc",
        image = "org/pentaho/di/sdk/samples/jobentries/demo/resources/demo.svg",
        categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.Conditions",
        i18nPackageName = "org.pentaho.di.sdk.samples.jobentries.demo",
        documentationUrl = "DemoJobEntry.DocumentationURL",
        casesUrl = "DemoJobEntry.CasesURL",
        forumUrl = "DemoJobEntry.ForumURL"
)
public class JobEntryBigQueryLoader extends JobEntryBase implements Cloneable, JobEntryInterface {

    private static Class<?> PKG = JobEntryBigQueryLoader.class;

    private String dataset;
    private String tableName;
    private String sourceUri;
    private boolean createTable;
    private Map<String, String> tableFields;


    BigQuery bigquery;

    public JobEntryBigQueryLoader(String name) {
        super(name, "");

        bigquery = BigQueryOptions.getDefaultInstance().getService();
        tableFields = new HashMap<String, String>();
    }

    @Override
    public String getDialogClassName() {
        return JobEntryBigQueryLoaderDialog.class.getName();
    }

    public Result execute(Result previousResult, int nr) throws KettleException {
        Result result = previousResult;
        result.setResult(false);
        result.setNrErrors(0);

        TableId tableId = TableId.of(dataset, tableName);
        Table table = bigquery.getTable(tableId);

        if (table == null) {
            logDebug("Table not found...");

            if(createTable) {
                logDebug("Creating table based on mapping specification");
                //TODO: Set the field list based on the mapping
                //Schema schema = Schema.newBuilder().build();
                //table = bigquery.create(TableInfo.of(tableId, StandardTableDefinition.of(schema)));
            } else {
                result.setNrErrors(1);
                logError( "Table doesn't exist" );
                return result;
            }
        }

        Job loadJob = table.load(FormatOptions.csv(), sourceUri);
        try {
            loadJob = loadJob.waitFor();

            if (loadJob.getStatus().getError() != null) {
                result.setNrErrors(1);
                result.setResult(false);
                logError("Error while loading table.");
            } else {
                result.setResult(true);
            }
        } catch (InterruptedException e) {
            result.setNrErrors(1);
            result.setResult(false);
            logError( "An error occurred executing this job entry : " + e.getMessage() );
        }

        return result;
    }
}
