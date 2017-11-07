/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.itfactory.kettle.job.entries.bigqueryloader;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
/**
 * Custom Job Entry that enables a bulk upload from a file on Google Cloud Storage to a table in Google BigQuery.
 * 
 * @author afowler
 * @author asimoes
 * @since 02-11-2017
 */
public class JobEntryBigQueryLoader extends JobEntryBase implements Cloneable, JobEntryInterface {

  private static Class<?> PKG = JobEntryBigQueryLoader.class;

  public static final String TYPE_JSON = "JSON";
  public static final String TYPE_CSV = "CSV";
  public static final String TYPE_AVRO = "Avro";

  private boolean useContainerSecurity = true;
  private String credentialsPath = null;
  private String projectId = null;
  private String datasetName = null;
  private String tableName = null;
  private String sourceUri = null;
  private boolean createDataset = false;
  private boolean createTable = false;
  private String fileType = TYPE_CSV;
  private String delimiter = ",";
  private String quote = "\"";
  private String leadingRowsToSkip = "1";

  private Map<String, String> tableFields;

  private String[] fieldNames = new String[] {};
  private String[] fieldTypes = new String[] {};

  BigQuery bigquery;

  /**
   * Default constructor
   */
  public JobEntryBigQueryLoader() {
    super( "BigQueryLoader", "Loads Google Cloud Storage files in to Google BigQuery" );

    tableFields = new HashMap<String, String>();
  }

  /**
   * Constructor including the name of the Job Entry
   */
  public JobEntryBigQueryLoader( String name ) {
    super( name, "Loads Google Cloud Storage files in to Google BigQuery" );

    tableFields = new HashMap<String, String>();
  }

  /**
   * Clones this Job Entry configuration
   */
  public Object clone() {
    JobEntryBigQueryLoader je = (JobEntryBigQueryLoader) super.clone();
    return je;
  }

  @Override
  /**
   * Returns the class name of this custom Job Entry's Dialog box
   */
  public String getDialogClassName() {
    return JobEntryBigQueryLoaderDialog.class.getName();
  }

  public boolean evaluates() {
    return true;
  }

  /**
   * Returns this Job Entry instances configuration as XML to be saved to a kjb file
   */
  public String getXML() {
    StringBuilder retval = new StringBuilder( 650 ); // 528 chars in spaces and tags alone

    retval.append( super.getXML() );
    retval.append( "      " )
      .append( XMLHandler.addTagValue( "useContainerSecurity", useContainerSecurity ? "Y" : "N" ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "projectId", projectId ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "credentialsPath", credentialsPath ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "datasetName", datasetName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "tableName", tableName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "sourceUri", sourceUri ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "delimiter", delimiter ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "quote", quote ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "createDataset", createDataset ? "Y" : "N" ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "createTable", createTable ? "Y" : "N" ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "fileType", fileType ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "leadingRowsToSkip", leadingRowsToSkip ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < fieldNames.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", fieldNames[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "type", fieldTypes[ i ] ) );
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  /**
   * Loads the configuration in to this object for this job entry from a kjb file
   */
  public void loadXML( Node entrynode, List<DatabaseMeta> databases, List<SlaveServer> slaveServers,
                       Repository rep, IMetaStore metaStore ) throws KettleXMLException {
    try {
      super.loadXML( entrynode, databases, slaveServers );
      String ucs = XMLHandler.getTagValue( entrynode, "useContainerSecurity" );
      if ( null == ucs || "".equals( ucs.trim() ) ) {
        useContainerSecurity = true;
      } else {
        useContainerSecurity = "Y".equals( ucs );
      }
      projectId = XMLHandler.getTagValue( entrynode, "projectId" );
      credentialsPath = XMLHandler.getTagValue( entrynode, "credentialsPath" );
      datasetName = XMLHandler.getTagValue( entrynode, "datasetName" );
      tableName = XMLHandler.getTagValue( entrynode, "tableName" );
      sourceUri = XMLHandler.getTagValue( entrynode, "sourceUri" );

      String d = XMLHandler.getTagValue( entrynode, "delimiter" );
      if ( null == d ) { // no empty check as sometimes you don't want a delimiter (single field only)
        delimiter = ",";
      } else {
        delimiter = d;
      }

      String q = XMLHandler.getTagValue( entrynode, "quote" );
      if ( null == q ) { // no empty check as sometimes you don't want a quote field (quotes not in csv file)
        quote = "\"";
      } else {
        quote = q;
      }

      String cd = XMLHandler.getTagValue( entrynode, "createDataset" );
      if ( null == cd || "".equals( cd.trim() ) ) {
        createDataset = true;
      } else {
        createDataset = "Y".equals( cd );
      }

      String ct = XMLHandler.getTagValue( entrynode, "createTable" );
      if ( null == ct || "".equals( ct.trim() ) ) {
        createTable = true;
      } else {
        createTable = "Y".equals( ct );
      }

      String ft = XMLHandler.getTagValue( entrynode, "fileType" );
      if ( null == ct || "".equals( ct.trim() ) ) {
        fileType = TYPE_JSON;
      } else {
        fileType = ft;
      }

      leadingRowsToSkip = XMLHandler.getTagValue( entrynode, "leadingRowsToSkip" );

      int i, nrfields;
      String type;

      Node fields = XMLHandler.getSubNode( entrynode, "fields" );
      nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        fieldNames[ i ] = XMLHandler.getTagValue( fnode, "name" );
        fieldTypes[ i ] = XMLHandler.getTagValue( fnode, "type" );
      }

    } catch ( KettleXMLException xe ) {
      throw new KettleXMLException( "Unable to load job entry of type 'ftp' from XML node", xe );
    }
  }

  /**
   * Loads configuration for this job entry from a repository file
   */
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
      createDataset = rep.getJobEntryAttributeBoolean( id_jobentry, "createDataset" );
      createTable = rep.getJobEntryAttributeBoolean( id_jobentry, "createTable" );
      fileType = rep.getJobEntryAttributeString( id_jobentry, "fileType" );
      leadingRowsToSkip = rep.getJobEntryAttributeString( id_jobentry, "leadingRowsToSkip" );

      int nrfields = rep.countNrStepAttributes( id_jobentry, "field_name" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        fieldNames[ i ] = rep.getStepAttributeString( id_jobentry, i, "field_name" );
        fieldTypes[ i ] = rep.getStepAttributeString( id_jobentry, i, "field_type" );
      }

    } catch ( KettleException dbe ) {
      throw new KettleException(
        "Unable to load job entry of type 'bigquery-gcs-load' from the repository for id_jobentry="
          + id_jobentry, dbe );
    }
  }

  /**
   * Saves this job entry instance's configuration to a repository
   */
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
      rep.saveJobEntryAttribute( id_job, getObjectId(), "createDataset", createDataset );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "createTable", createTable );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "fileType", fileType );
      rep.saveJobEntryAttribute( id_job, getObjectId(), "leadingRowsToSkip", leadingRowsToSkip );

      for ( int i = 0; i < fieldNames.length; i++ ) {
        rep.saveStepAttribute( id_job, getObjectId(), i, "field_name", fieldNames[ i ] );
        rep.saveStepAttribute( id_job, getObjectId(), i, "field_type", fieldTypes[ i ] );
      }

    } catch ( KettleDatabaseException dbe ) {
      throw new KettleException(
        "Unable to save job entry of type 'bigquery-gcs-load' to the repository for id_job=" + id_job, dbe );
    }
  }

  /**
   * Executes this job entry instance (Imports the file(s) to BigQuery)
   */
  public Result execute( Result previousResult, int nr ) throws KettleException {

    Result result = previousResult;

    // 1. Login
    try {
      // switch on auth type
      if ( useContainerSecurity ) {
        bigquery = BigQueryOptions.getDefaultInstance().getService();
      } else {

        BigQueryOptions options = BigQueryOptions.newBuilder()
          .setProjectId( environmentSubstitute( projectId ) )
          .setCredentials( GoogleCredentials.fromStream(
            new FileInputStream( environmentSubstitute( credentialsPath ) ) )
          ).build();
        bigquery = options.getService();
      }

      // 2. Get table or create it
      result.setResult( false );
      result.setNrErrors( 0 );

      DatasetInfo datasetInfo = Dataset.of( environmentSubstitute( datasetName ) );
      Dataset dataset = bigquery.getDataset( datasetInfo.getDatasetId() );

      if ( dataset == null ) {
        if ( createDataset ) {
          dataset = bigquery.create( datasetInfo );
        } else {
          logError( "Failed to automatically create the dataset. Please create the dataset: " + environmentSubstitute(
            datasetName ) );
        }
      }

      TableId tableId = TableId.of( dataset.getDatasetId().getDataset(), environmentSubstitute( tableName ) );
      Table table = bigquery.getTable( tableId );

      if ( table == null ) {
        logDebug( "Table not found: " + environmentSubstitute( tableName ) );

        if ( createTable ) {
          logDebug( "Creating table based on mapping specification" );
          // Set the field list based on the mapping

          List<Field> fieldList = new ArrayList<Field>();
          int nrfields = getFieldNames().length;
          for ( int i = 0; i < nrfields; i++ ) {
            fieldList.add( Field.of( fieldNames[ i ], LegacySQLTypeName.valueOf( fieldTypes[ i ] ) ) );
          }

          Schema schema = Schema.of( fieldList );

          table = bigquery.create( TableInfo.of( tableId, StandardTableDefinition.of( schema ) ) );
        } else {
          result.setNrErrors( 1 );
          logError( "Table doesn't exist: " + environmentSubstitute( tableName ) );
          return result;
        }
      }

      // 3. configure type and load the data
      Job loadJob;
      if ( TYPE_CSV.equals( fileType ) ) {
        loadJob = table.load( CsvOptions.newBuilder().setFieldDelimiter( environmentSubstitute( delimiter ) )
          .setQuote( environmentSubstitute( quote ) )
          .setSkipLeadingRows( Integer.valueOf( environmentSubstitute( leadingRowsToSkip ) ) )
          .build(), sourceUri );
      } else if ( TYPE_AVRO.equals( fileType ) ) {
        loadJob = table.load( FormatOptions.avro(), environmentSubstitute( sourceUri ) );
      } else {
        // JSON
        loadJob = table.load( FormatOptions.json(), environmentSubstitute( sourceUri ) );
      }

      try {
        loadJob = loadJob.waitFor();

        if ( loadJob.getStatus().getError() != null ) {
          result.setNrErrors( 1 );
          result.setResult( false );
          logError( "Error while loading table: " + loadJob.getStatus().getError().toString() );
        } else {
          result.setResult( true );
        }
      } catch ( InterruptedException e ) {
        result.setNrErrors( 1 );
        result.setResult( false );
        logError( "An error occurred executing this job entry : " + e.getMessage() );
      }
    } catch ( IOException ioe ) {
      result.setNrErrors( 1 );
      result.setResult( false );
      logError( "An error occurred while loading source data: " + ioe.getMessage() );
    }

    return result;
  }

  /**
   * Sets the Google BigQuery dataset name
   */
  public void setDatasetName( String dsn ) {
    datasetName = dsn;
  }

  /**
   * Returns the Google BigQuery dataset name
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Sets the Google BigQuery table name
   */
  public void setTableName( String tn ) {
    tableName = tn;
  }

  /**
   * Returns the Google BigQuery table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Sets the Google Cloud Storage source URL pattern (may include wildcards)
   */
  public void setSourceUri( String uri ) {
    sourceUri = uri;
  }

  /**
   * Returns the Google Cloud Storage source URL pattern (may include wildcards)
   */
  public String getSourceUri() {
    return sourceUri;
  }

  /**
   * Sets whether to create the dataset if it does not exist in BigQuery
   */
  public void setCreateDataset( boolean doit ) {
    createDataset = doit;
  }

  /**
   * Returns whether this job will attempt to create the dataset in BigQuery if it does not exist
   */
  public boolean getCreateDataset() {
    return createDataset;
  }

  /**
   * Sets whether to create the table if it does not exist in this BigQuery dataset
   */
  public boolean getCreateTable() {
    return createTable;
  }

  /**
   * Returns whether this job will attempt to create the table in the BigQuery dataset if it does not exist
   */
  public void setCreateTable( boolean doit ) {
    createTable = doit;
  }

  /**
   * Returns the file type (JSON,CSV,Avro)
   */
  public String getFileType() {
    return fileType;
  }

  /**
   * Sets the file type (JSON,CSV,Avro)
   */
  public void setFileType( String ft ) {
    fileType = ft;
  }

  /**
   * Sets the delimiter character/string (CSV files only)
   */
  public void setDelimiter( String delim ) {
    delimiter = delim;
  }

  /**
   * Returns the CSV delimiter character/string
   */
  public String getDelimiter() {
    return delimiter;
  }

  /**
   * Sets the quote character/string (CSV files only)
   */
  public void setQuote( String q ) {
    quote = q;
  }

  /**
   * Returns the CSV quote character/string
   */
  public String getQuote() {
    return quote;
  }

  /**
   * Sets the number of rows to skip in the CSV file. Actually an Integer.
   * 
   * E.g. if the CSV has a header row, this would be 1. If no header row is present, this would be 0.
   */
  public void setLeadingRowsToSkip( String skip ) {
    leadingRowsToSkip = skip;
  }

  /**
   * Returns the number of leading rows to skip. Actually an Integer.
   */
  public String getLeadingRowsToSkip() {
    return leadingRowsToSkip;
  }

  /**
   * Sets whether to use container security (true) or a specified google cloud JSON authentication file (false)
   */
  public void setUseContainerSecurity( boolean use ) {
    useContainerSecurity = use;
  }

  /**
   * Returns whether to use container security (true) or a specified google cloud JSON authentication file (false)
   */
  public boolean getUseContainerSecurity() {
    return useContainerSecurity;
  }

  /**
   * Sets the Google Cloud project ID for the dataset/table
   * 
   * Note: Not used in google container security (set by the container instead)
   */
  public void setProjectId( String pid ) {
    projectId = pid;
  }

  /**
   * Returns the Google Cloud project ID for the dataset/table
   * 
   * Note: Not used in google container security (set by the container instead)
   */
  public String getProjectId() {
    return projectId;
  }

  /**
   * Sets the Google Cloud JSON credentials file path
   * 
   * Note: Doesn't apply if using container security
   */
  public void setCredentialsPath( String cp ) {
    credentialsPath = cp;
  }

  /**
   * Returns the Google Cloud JSON credentials file path
   * 
   * Note: Doesn't apply if using container security
   */
  public String getCredentialsPath() {
    return credentialsPath;
  }

  /**
   * Sets the fields configured in the BigQuery table
   * 
   * Note: Only used when creating a new table in BigQuery
   */
  public void setTableFields( Map<String, String> fs ) {
    tableFields = fs;
  }

  /**
   * Returns the fields configured in the BigQuery table
   * 
   * Note: Only used when creating a new table in BigQuery
   */
  public Map<String, String> getTableFields() {
    return tableFields;
  }

  /**
   * Internal working method that creates a number of fields for the BigQuery table
   */
  public void allocate( int nrfields ) {
    fieldNames = new String[ nrfields ];
    fieldTypes = new String[ nrfields ];
  }

  /**
   * Internal worker method that returns the array of field names for fields in the BigQuery table
   */
  public String[] getFieldNames() {
    return fieldNames;
  }

  /**
   * Internal worker method that sets the array of field names for fields in the BigQuery table
   */
  public void setFieldNames( String[] fn ) {
    fieldNames = fn;
  }

  /**
   * Internal worker method that returns the array of field types for fields in the BigQuery table
   */
  public String[] getFieldTypes() {
    return fieldTypes;
  }

  /**
   * Internal worker method that sets the array of field types for fields in the BigQuery table
   */
  public void setFieldTypes( String[] t ) {
    fieldTypes = t;
  }
}
