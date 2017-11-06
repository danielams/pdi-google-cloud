
package org.itfactory.kettle.steps.bigquerystream;

import java.util.List;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step( id = "BigQueryStreamOutput",
image = "BigQueryStreamOutput.svg",
 i18nPackageName = "org.itfactory.kettle.steps.bigquerystream", name = "BigQueryStream.Name",
 description = "BigQueryStream.Description",
 categoryDescription = "i18n:org.pentaho.di.steps:StepCategory.Category.BigData" )
public class BigQueryStreamMeta extends BaseStepMeta implements StepMetaInterface {
    private static Class<?> PKG = BigQueryStreamMeta.class; // for i18n purposes, needed by Translator2!!

    private boolean useContainerSecurity = false;
    private String credentialsPath = "C:/Users/afowler/Documents/Apps/google-cloud/BigQueryTesting-fd7a2959ea0f.json";
    private String projectId = "bigquery-testing-184814";
    private String datasetName = "salesdata";
    private String tableName = "ages";
    private boolean createDataset = true;
    private boolean createTable = true;

    @Override
    public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
      readData( stepnode );
    }
    
  @Override
  public Object clone() {
    BigQueryStreamMeta retval = (BigQueryStreamMeta) super.clone();
/*
    int nrfields = fieldName.length;

    retval.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      retval.fieldName[i] = fieldName[i];
      retval.fieldNewName[i] = fieldNewName[i];
      retval.aggregateType[i] = aggregateType[i];
    }
    */
    return retval;
  }

  private void readData( Node entrynode ) throws KettleXMLException {
    try {
        /*
      int i, nrfields;
      String type;

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        fieldName[i] = XMLHandler.getTagValue( fnode, "name" );
        fieldNewName[i] = XMLHandler.getTagValue( fnode, "rename" );
        type = XMLHandler.getTagValue( fnode, "type" );
        aggregateType[i] = getType( type );
      }
      */
      
      String ucs = XMLHandler.getTagValue( entrynode, "useContainerSecurity" );
      if (null == ucs || "".equals(ucs.trim())) {
          useContainerSecurity = true;
      } else {
          useContainerSecurity = "Y".equals(ucs);
      }
      projectId = XMLHandler.getTagValue( entrynode, "projectId" );
      credentialsPath = XMLHandler.getTagValue( entrynode, "credentialsPath" );
      datasetName = XMLHandler.getTagValue( entrynode, "datasetName" );
      tableName = XMLHandler.getTagValue( entrynode, "tableName" );
      

      String cd = XMLHandler.getTagValue( entrynode, "createDataset" );
      if (null == cd || "".equals(cd.trim())) {
          createDataset = true;
      } else {
          createDataset = "Y".equals(cd);
      }
      String ct = XMLHandler.getTagValue( entrynode, "createTable" );
      if (null == ct || "".equals(ct.trim())) {
          createTable = true;
      } else {
          createTable = "Y".equals(ct);
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString(
        PKG, "FieldAnalysisMeta.Exception.UnableToLoadStepInfo" ), e );
    }
  }

  @Override
  public void setDefault() {
    int i, nrfields;

    nrfields = 0;
/*
    allocate( nrfields );

    for ( i = 0; i < nrfields; i++ ) {
      fieldName[i] = BaseMessages.getString( PKG, "FieldAnalysisMeta.Fieldname.Label" );
      fieldNewName[i] = BaseMessages.getString( PKG, "FieldAnalysisMeta.NewName.Label" );
      aggregateType[i] = TYPE_AGGREGATE_SUM;
    }
    */
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer( 300 );
    retval.append( "      " ).append( XMLHandler.addTagValue( "useContainerSecurity", useContainerSecurity?"Y":"N" ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "projectId", projectId ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "credentialsPath", credentialsPath ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "datasetName", datasetName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "tableName", tableName ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "createDataset", createDataset?"Y":"N" ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "createTable", createTable?"Y":"N" ) );
/*
    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < fieldName.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "name", fieldName[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "rename", fieldNewName[i] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "type", getTypeDesc( aggregateType[i] ) ) );
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" ).append( Const.CR );
*/

    return retval.toString();
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws KettleException {

    try {
        /*
      int nrfields = rep.countNrStepAttributes( id_step, "field_name" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        fieldName[i] = rep.getStepAttributeString( id_step, i, "field_name" );
        fieldNewName[i] = rep.getStepAttributeString( id_step, i, "field_rename" );
        aggregateType[i] = getType( rep.getStepAttributeString( id_step, i, "field_type" ) );
      }*/
      useContainerSecurity = rep.getJobEntryAttributeBoolean( id_step, "useContainerSecurity" );
      projectId = rep.getJobEntryAttributeString( id_step, "projectId" );
      credentialsPath = rep.getJobEntryAttributeString( id_step, "credentialsPath" );
      datasetName = rep.getJobEntryAttributeString( id_step, "datasetName" );
      tableName = rep.getJobEntryAttributeString( id_step, "tableName" );
      createDataset = rep.getJobEntryAttributeBoolean( id_step, "createDataset" );
      createTable = rep.getJobEntryAttributeBoolean( id_step, "createTable" );
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString(
        PKG, "FieldAnalysisMeta.Exception.UnexpectedErrorWhileReadingStepInfo" ), e );
    }

  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    try {
        /*
      for ( int i = 0; i < fieldName.length; i++ ) {
        rep.saveStepAttribute( id_transformation, id_step, i, "field_name", fieldName[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_rename", fieldNewName[i] );
        rep.saveStepAttribute( id_transformation, id_step, i, "field_type", getTypeDesc( aggregateType[i] ) );
      }*/
      rep.saveJobEntryAttribute( id_transformation, getObjectId(), "useContainerSecurity", useContainerSecurity );
      rep.saveJobEntryAttribute( id_transformation, getObjectId(), "projectId", projectId );
      rep.saveJobEntryAttribute( id_transformation, getObjectId(), "credentialsPath", credentialsPath );
      rep.saveJobEntryAttribute( id_transformation, getObjectId(), "datasetName", datasetName );
      rep.saveJobEntryAttribute( id_transformation, getObjectId(), "tableName", tableName );
      rep.saveJobEntryAttribute( id_transformation, getObjectId(), "createDataset", createDataset );
      rep.saveJobEntryAttribute( id_transformation, getObjectId(), "createTable", createTable );
    } catch ( KettleException e ) {
      throw new KettleException( BaseMessages.getString( PKG, "FieldAnalysisMeta.Exception.UnableToSaveStepInfo" )
        + id_step, e );
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {

    CheckResult cr;
    String message = "";
/*
    if ( fieldName.length > 0 ) {
      boolean error_found = false;
      // See if all fields are available in the input stream...
      message =
        BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.FieldsNotFound.DialogMessage" ) + Const.CR;
      for ( int i = 0; i < fieldName.length; i++ ) {
        if ( prev.indexOfValue( fieldName[i] ) < 0 ) {
          message += "  " + fieldName[i] + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, message, stepMeta );
      } else {
        message = BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.AllFieldsOK.DialogMessage" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, message, stepMeta );
      }
      remarks.add( cr );

      // See which fields are dropped: comment on it!
      message =
        BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.IgnoredFields.DialogMessage" ) + Const.CR;
      error_found = false;

      for ( int i = 0; i < prev.size(); i++ ) {
        ValueMetaInterface v = prev.getValueMeta( i );
        boolean value_found = false;
        for ( int j = 0; j < fieldName.length && !value_found; j++ ) {
          if ( v.getName().equalsIgnoreCase( fieldName[j] ) ) {
            value_found = true;
          }
        }
        if ( !value_found ) {
          message += "  " + v.getName() + " (" + v.toStringMeta() + ")" + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        cr = new CheckResult( CheckResult.TYPE_RESULT_COMMENT, message, stepMeta );
      } else {
        message = BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.AllFieldsUsed.DialogMessage" );
        cr = new CheckResult( CheckResult.TYPE_RESULT_OK, message, stepMeta );
      }
      remarks.add( cr );
    } else {
      message = BaseMessages.getString( PKG, "FieldAnalysisMeta.CheckResult.NothingSpecified.DialogMessage" );
      cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING, message, stepMeta );
      remarks.add( cr );
    }*/

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "FieldAnalysisMeta.CheckResult.StepReceiveInfo.DialogMessage" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "FieldAnalysisMeta.CheckResult.NoInputReceived.DialogMessage" ), stepMeta );
      remarks.add( cr );
    }

  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new BigQueryStream( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new BigQueryStreamData();
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