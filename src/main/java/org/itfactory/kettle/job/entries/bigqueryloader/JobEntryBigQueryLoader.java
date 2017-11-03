package org.itfactory.kettle.job.entries.bigqueryloader;

import com.google.cloud.bigquery.*;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;

import java.util.HashMap;
import java.util.Map;

import com.google.auth.oauth2.GoogleCredentials;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.resource.ResourceEntry;
import org.pentaho.di.resource.ResourceEntry.ResourceType;
import org.pentaho.di.resource.ResourceReference;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

@JobEntry(
        id = "GoogleBigQueryStorageLoad",
        name = "GoogleBigQueryStorageLoad.Name",
        description = "GoogleBigQueryStorageLoad.TooltipDesc",
        image = "org/itfactory/kettle/job/entries/bigqueryloader/bigqueryloader.svg",
        categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
        i18nPackageName = "org.itfactory.kettle.job.entries.bigqueryloader",
        documentationUrl = "GoogleBigQueryStorageLoad.DocumentationURL",
        casesUrl = "GoogleBigQueryStorageLoad.CasesURL",
        forumUrl = "GoogleBigQueryStorageLoad.ForumURL"
)
public class JobEntryBigQueryLoader extends JobEntryBase implements Cloneable, JobEntryInterface {

    private static Class<?> PKG = JobEntryBigQueryLoader.class;

    private boolean useContainerSecurity = false;
    private String credentialsPath = "C:/Users/afowler/Documents/Apps/google-cloud/BigQueryTesting-fd7a2959ea0f.json";
    private String projectId = "bigquery-testing-184814";
    private String datasetName = "salesdata";
    private String tableName = "ages";
    private String sourceUri = "gs://adam-csv-data/sample-age-data.csv";
    private boolean createDataset = true;
    private boolean createTable = true;
    private String delimiter = ",";
    private String quote = "\"";
    private int leadingRowsToSkip = 1;

    private Map<String, String> tableFields;


    BigQuery bigquery;

    public JobEntryBigQueryLoader() {
        super("BigQueryLoader","Loads Google Cloud Storage files in to Google BigQuery");
        
        tableFields = new HashMap<String, String>();
    }

    public JobEntryBigQueryLoader(String name) {
        super(name, "Loads Google Cloud Storage files in to Google BigQuery");

        tableFields = new HashMap<String, String>();
    }
    
    public Object clone() {
        JobEntryBigQueryLoader je = (JobEntryBigQueryLoader) super.clone();
        return je;
    }






    @Override
    public String getDialogClassName() {
        return JobEntryBigQueryLoaderDialog.class.getName();
    }
    
    public boolean evaluates() {
        return true;
    }
    
    public String getXML() {
        StringBuilder retval = new StringBuilder( 650 ); // 528 chars in spaces and tags alone

        retval.append( super.getXML() );
        retval.append( "      " ).append( XMLHandler.addTagValue( "useContainerSecurity", useContainerSecurity?"Y":"N" ) );
        retval.append( "      " ).append( XMLHandler.addTagValue( "projectId", projectId ) );
        retval.append( "      " ).append( XMLHandler.addTagValue( "credentialsPath", credentialsPath ) );
        retval.append( "      " ).append( XMLHandler.addTagValue( "datasetName", datasetName ) );
        retval.append( "      " ).append( XMLHandler.addTagValue( "tableName", tableName ) );
        retval.append( "      " ).append( XMLHandler.addTagValue( "sourceUri", sourceUri ) );
        retval.append( "      " ).append( XMLHandler.addTagValue( "delimiter", delimiter ) );
        retval.append( "      " ).append( XMLHandler.addTagValue( "quote", quote ) );
    
        return retval.toString();
    }
    
    public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers,
      Repository rep, IMetaStore metaStore ) throws KettleXMLException {
        try {
            super.loadXML( entrynode, databases, slaveServers );
            useContainerSecurity = "Y".equals(XMLHandler.getTagValue( entrynode, "useContainerSecurity" ));
            projectId = XMLHandler.getTagValue( entrynode, "projectId" );
            credentialsPath = XMLHandler.getTagValue( entrynode, "credentialsPath" );
            datasetName = XMLHandler.getTagValue( entrynode, "datasetName" );
            tableName = XMLHandler.getTagValue( entrynode, "tableName" );
            sourceUri = XMLHandler.getTagValue( entrynode, "sourceUri" );
            delimiter = XMLHandler.getTagValue( entrynode, "delimiter" );
            quote = XMLHandler.getTagValue( entrynode, "quote" );
        } catch ( KettleXMLException xe ) {
            throw new KettleXMLException( "Unable to load job entry of type 'ftp' from XML node", xe );
        }
    }
    
    public void loadRep( Repository rep, IMetaStore metaStore, ObjectId id_jobentry, List<DatabaseMeta> databases,
      List<SlaveServer> slaveServers ) throws KettleException {
          
        try {
            useContainerSecurity = rep.getJobEntryAttributeBoolean( id_jobentry, "useContainerSecurity" );
            projectId = rep.getJobEntryAttributeString( id_jobentry, "projectId" );
            credentialsPath = rep.getJobEntryAttributeString( id_jobentry, "credentialsPath" );
            datasetName = rep.getJobEntryAttributeString( id_jobentry, "datasetName" );
            tableName = rep.getJobEntryAttributeString( id_jobentry, "tableName" );
            sourceUri = rep.getJobEntryAttributeString( id_jobentry, "sourceUri" );
            delimiter = rep.getJobEntryAttributeString( id_jobentry, "delimiter" );
            quote = rep.getJobEntryAttributeString( id_jobentry, "quote" );

        } catch ( KettleException dbe ) {
            throw new KettleException( "Unable to load job entry of type 'bigquery-gcs-load' from the repository for id_jobentry="
                + id_jobentry, dbe );
        }
    }
    
    public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_job ) throws KettleException {
        
        try {
            rep.saveJobEntryAttribute( id_job, getObjectId(), "useContainerSecurity", useContainerSecurity );
            rep.saveJobEntryAttribute( id_job, getObjectId(), "projectId", projectId );
            rep.saveJobEntryAttribute( id_job, getObjectId(), "credentialsPath", credentialsPath );
            rep.saveJobEntryAttribute( id_job, getObjectId(), "datasetName", datasetName );
            rep.saveJobEntryAttribute( id_job, getObjectId(), "tableName", tableName );
            rep.saveJobEntryAttribute( id_job, getObjectId(), "sourceUri", sourceUri );
            rep.saveJobEntryAttribute( id_job, getObjectId(), "delimiter", delimiter );
            rep.saveJobEntryAttribute( id_job, getObjectId(), "quote", quote );

        } catch ( KettleDatabaseException dbe ) {
            throw new KettleException(
              "Unable to save job entry of type 'bigquery-gcs-load' to the repository for id_job=" + id_job, dbe );
        }
    }

    public Result execute(Result previousResult, int nr) throws KettleException {
        // 1. Login
try {
        // switch on auth type
        if (useContainerSecurity) {
            bigquery = BigQueryOptions.getDefaultInstance().getService();
        } else {
            
            BigQueryOptions options = BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(GoogleCredentials.fromStream(
              new FileInputStream(credentialsPath))
            ).build();
            bigquery = options.getService();
        }

        // 2. Get table or create it
        Result result = previousResult;
        result.setResult(false);
        result.setNrErrors(0);

        DatasetInfo datasetInfo = Dataset.of(datasetName);
        Dataset dataset = bigquery.getDataset(datasetInfo.getDatasetId());

        if (dataset == null) {
            if (createDataset) {
                dataset = bigquery.create(datasetInfo);
            } else {
                logError("Failed to automatically create the dataset. Please create the dataset: " + datasetName);
            }
        }

        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
        Table table = bigquery.getTable(tableId);

        if (table == null) {
            logDebug("Table not found: " + tableName);

            if(createTable) {
                logDebug("Creating table based on mapping specification");
                //TODO: Set the field list based on the mapping
                //Schema schema = Schema.newBuilder().build();
                //table = bigquery.create(TableInfo.of(tableId, StandardTableDefinition.of(schema)));
            } else {
                result.setNrErrors(1);
                logError( "Table doesn't exist: " + tableName );
                return result;
            }
        }

        //Job loadJob = table.load(FormatOptions.csv(), sourceUri);
        Job loadJob = table.load(CsvOptions.newBuilder().setFieldDelimiter(delimiter).setQuote(quote).setSkipLeadingRows(leadingRowsToSkip).build(), sourceUri);

        try {
            loadJob = loadJob.waitFor();

            if (loadJob.getStatus().getError() != null) {
                result.setNrErrors(1);
                result.setResult(false);
                logError("Error while loading table: " + loadJob.getStatus().getError().toString());
            } else {
                result.setResult(true);
            }
        } catch (InterruptedException e) {
            result.setNrErrors(1);
            result.setResult(false);
            logError( "An error occurred executing this job entry : " + e.getMessage() );
        }

        return result;
    } catch (IOException ioe) {
        logError("An error occurred while loading source data: " + ioe.getMessage());
    }
    return null;
    }

    public void setDatasetName(String dsn) {
        datasetName = dsn;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setTableName(String tn) {
        tableName = tn;
    }

    public String getTableName() {
        return tableName;
    }

    public void setSourceUri(String uri) {
        sourceUri = uri;
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public void setCreateDataset(boolean doit) {
        createDataset = doit;
    }

    public boolean getCreateDataset() {
        return createDataset;
    }

    public boolean getCreateTable() {
        return createTable;
    }

    public void setCreateTable(boolean doit) {
        createTable = doit;
    }

    public void setDelimiter(String delim) {
        delimiter = delim;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setQuote(String q) {
        quote = q;
    }

    public String getQuote() {
        return quote;
    }

    public void setRowsToSkip(int skip) {
        leadingRowsToSkip = skip;
    }

    public int getRowsToSkip() {
        return leadingRowsToSkip;
    }

    public void setTableFields(Map<String, String> fs) {
        tableFields = fs;
    }

    public Map<String, String> getTableFields() {
        return tableFields;
    }

    public void setUseContainerSecurity(boolean use) {
        useContainerSecurity = use;
    }

    public boolean getUseContainerSecurity() {
        return useContainerSecurity;
    }

    public void setProjectId(String pid) {
        projectId = pid;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setCredentialsPath(String cp) {
        credentialsPath = cp;
    }

    public String getCredentialsPath() {
        return credentialsPath;
    }
}