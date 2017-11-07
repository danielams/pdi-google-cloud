

package org.itfactory.kettle.steps.bigquerystream;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class BigQueryStreamDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = BigQueryStreamMeta.class; // for i18n purposes, needed by Translator2!!

  private BigQueryStreamMeta input;

  private Label wlName;
  private Text wName;

  private FormData fdlName, fdName;

  private Label wlUseContainerAuth;
  private Button wUseContainerAuth;
  private FormData fdlUseContainerAuth,fdUseContainerAuth;
  
  private Label cplName;
  private Text cpName;
  private FormData fcplName,fcpName;
  
  private Label plName;
  private Text pName;
  private FormData fplName,fpName;

  private Label dslName;
  private Text dsName;
  private FormData fdslName,fdsName;

  private Label wlCreateDataset;
  private Button wCreateDataset;
  private FormData fdlCreateDataset,fdCreateDataset;
  
  private Label tlName;
  private Text tName;
  private FormData ftlName,ftName;

  private Label wlCreateTable;
  private Button wCreateTable;
  private FormData fdlCreateTable,fdCreateTable;

  
  private Label qlName;
  private Text qName;
  private FormData fqlName,fqName;
/*
  private Label wlFields;
  private TableView wFields;
  private FormData fdlFields,fdFields;
  */

  private Button wOK, wCancel;

  private Listener lsOK, lsCancel, lsResize;
  
  private Shell shell;

  private SelectionAdapter lsDef;


  private boolean changed = false;

  public BigQueryStreamDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    input = (BigQueryStreamMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );


    ModifyListener lsMod = new ModifyListener() {
        public void modifyText( ModifyEvent e ) {
          //sftpclient = null;
          input.setChanged();
        }
      };
      changed = input.hasChanged();
  
      FormLayout formLayout = new FormLayout();
      formLayout.marginWidth = Const.FORM_MARGIN;
      formLayout.marginHeight = Const.FORM_MARGIN;
  
      shell.setLayout( formLayout );
      shell.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Title" ) );
  
      int middle = props.getMiddlePct();
      int margin = Const.MARGIN;
  
      // Step Name
      wlName = new Label( shell, SWT.RIGHT );
      wlName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Name.Label" ) );
      props.setLook( wlName );
      fdlName = new FormData();
      fdlName.left = new FormAttachment( 0, 0 );
      fdlName.right = new FormAttachment( middle, -margin );
      fdlName.top = new FormAttachment( 0, margin );
      wlName.setLayoutData( fdlName );
      wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER  );
      wName.setToolTipText(BaseMessages.getString(PKG, "GoogleBigQueryStorageLoad.Name.Tooltip"));
      props.setLook( wName );
      wName.addModifyListener( lsMod );
      fdName = new FormData();
      fdName.left = new FormAttachment( middle, 0 );
      fdName.top = new FormAttachment( 0, margin );
      fdName.right = new FormAttachment( 100, 0 );
      wName.setLayoutData( fdName );


      // use container auth checkbox
      wlUseContainerAuth = new Label( shell, SWT.RIGHT );
      wlUseContainerAuth.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.UseContainerAuth.Label" ) );
      props.setLook( wlUseContainerAuth );
      fdlUseContainerAuth = new FormData();
      fdlUseContainerAuth.left = new FormAttachment( 0, 0 );
      fdlUseContainerAuth.top = new FormAttachment( wName, margin );
      fdlUseContainerAuth.right = new FormAttachment( middle, -margin );
      wlUseContainerAuth.setLayoutData( fdlUseContainerAuth );
      wUseContainerAuth = new Button( shell, SWT.CHECK );
      props.setLook( wUseContainerAuth );
      wUseContainerAuth.setToolTipText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.UseContainerAuth.Tooltip" ) );
      fdUseContainerAuth = new FormData();
      fdUseContainerAuth.left = new FormAttachment( middle, 0 );
      fdUseContainerAuth.top = new FormAttachment( wName, margin );
      fdUseContainerAuth.right = new FormAttachment( 100, 0 );
      wUseContainerAuth.setLayoutData( fdUseContainerAuth );

      // Credentials path
      cplName = new Label( shell, SWT.RIGHT );
      cplName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.CredentialsPath.Label" ) );
      props.setLook( cplName );
      fcplName = new FormData();
      fcplName.left = new FormAttachment( 0, 0 );
      fcplName.right = new FormAttachment( middle, -margin );
      fcplName.top = new FormAttachment( wUseContainerAuth, margin );
      cplName.setLayoutData( fcplName );
      cpName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      cpName.setToolTipText(BaseMessages.getString(PKG, "GoogleBigQueryStorageLoad.CredentialsPath.Tooltip"));
      props.setLook( cpName );
      cpName.addModifyListener( lsMod );
      fcpName = new FormData();
      fcpName.left = new FormAttachment( middle, 0 );
      fcpName.top = new FormAttachment( wUseContainerAuth, margin );
      fcpName.right = new FormAttachment( 100, 0 );
      cpName.setLayoutData( fcpName );

      // BigQuery Project Id
      plName = new Label( shell, SWT.RIGHT );
      plName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Project.Label" ) );
      props.setLook( plName );
      fplName = new FormData();
      fplName.left = new FormAttachment( 0, 0 );
      fplName.right = new FormAttachment( middle, -margin );
      fplName.top = new FormAttachment( cpName, margin );
      plName.setLayoutData( fplName );
      pName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      pName.setToolTipText(BaseMessages.getString(PKG, "GoogleBigQueryStorageLoad.Project.Tooltip"));
      props.setLook( pName );
      pName.addModifyListener( lsMod );
      fpName = new FormData();
      fpName.left = new FormAttachment( middle, 0 );
      fpName.top = new FormAttachment( cpName, margin );
      fpName.right = new FormAttachment( 100, 0 );
      pName.setLayoutData( fpName );

      // BigQuery dataset name
      dslName = new Label( shell, SWT.RIGHT );
      dslName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.DataSet.Label" ) );
      props.setLook( dslName );
      fdslName = new FormData();
      fdslName.left = new FormAttachment( 0, 0 );
      fdslName.right = new FormAttachment( middle, -margin );
      fdslName.top = new FormAttachment( pName, margin );
      dslName.setLayoutData( fdslName );
      dsName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      dsName.setToolTipText(BaseMessages.getString(PKG, "GoogleBigQueryStorageLoad.DataSet.Tooltip"));
      props.setLook( dsName );
      dsName.addModifyListener( lsMod );
      fdsName = new FormData();
      fdsName.left = new FormAttachment( middle, 0 );
      fdsName.top = new FormAttachment( pName, margin );
      fdsName.right = new FormAttachment( 100, 0 );
      dsName.setLayoutData( fdsName );

      // create dataset checkbox
      wlCreateDataset = new Label( shell, SWT.RIGHT );
      wlCreateDataset.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.CreateDataset.Label" ) );
      props.setLook( wlCreateDataset );
      fdlCreateDataset = new FormData();
      fdlCreateDataset.left = new FormAttachment( 0, 0 );
      fdlCreateDataset.top = new FormAttachment( dsName, margin );
      fdlCreateDataset.right = new FormAttachment( middle, -margin );
      wlCreateDataset.setLayoutData( fdlCreateDataset );
      wCreateDataset = new Button( shell, SWT.CHECK );
      props.setLook( wCreateDataset );
      wCreateDataset.setToolTipText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.CreateDataset.Tooltip" ) );
      fdCreateDataset = new FormData();
      fdCreateDataset.left = new FormAttachment( middle, 0 );
      fdCreateDataset.top = new FormAttachment( dsName, margin );
      fdCreateDataset.right = new FormAttachment( 100, 0 );
      wCreateDataset.setLayoutData( fdCreateDataset );

      // table name
      tlName = new Label( shell, SWT.RIGHT );
      tlName.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.Table.Label" ) );
      props.setLook( tlName );
      ftlName = new FormData();
      ftlName.left = new FormAttachment( 0, 0 );
      ftlName.right = new FormAttachment( middle, -margin );
      ftlName.top = new FormAttachment( wlCreateDataset, margin );
      tlName.setLayoutData( ftlName );
      tName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
      tName.setToolTipText(BaseMessages.getString(PKG, "GoogleBigQueryStorageLoad.Table.Tooltip"));
      props.setLook( tName );
      tName.addModifyListener( lsMod );
      ftName = new FormData();
      ftName.left = new FormAttachment( middle, 0 );
      ftName.top = new FormAttachment( wlCreateDataset, margin );
      ftName.right = new FormAttachment( 100, 0 );
      tName.setLayoutData( ftName );


      // create table checkbox
      wlCreateTable = new Label( shell, SWT.RIGHT );
      wlCreateTable.setText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.CreateTable.Label" ) );
      props.setLook( wlCreateTable );
      fdlCreateTable = new FormData();
      fdlCreateTable.left = new FormAttachment( 0, 0 );
      fdlCreateTable.top = new FormAttachment( tName, margin );
      fdlCreateTable.right = new FormAttachment( middle, -margin );
      wlCreateTable.setLayoutData( fdlCreateTable );
      wCreateTable = new Button( shell, SWT.CHECK );
      props.setLook( wCreateTable );
      wCreateTable.setToolTipText( BaseMessages.getString( PKG, "GoogleBigQueryStorageLoad.CreateTable.Tooltip" ) );
      fdCreateTable = new FormData();
      fdCreateTable.left = new FormAttachment( middle, 0 );
      fdCreateTable.top = new FormAttachment( tName, margin );
      fdCreateTable.right = new FormAttachment( 100, 0 );
      wCreateTable.setLayoutData( fdCreateTable );


      
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, wCreateTable );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    //activeCopyFromPrevious();
    //activeUseKey();

    BaseStepDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "GoogleBigQueryStreamDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;

  }

  
  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wName.setText( Const.nullToEmpty( stepname) );
    wUseContainerAuth.setSelection( input.getUseContainerSecurity() );
    cpName.setText( Const.nullToEmpty( input.getCredentialsPath() ) );
    pName.setText( Const.nullToEmpty( input.getProjectId() ) );
    dsName.setText( Const.nullToEmpty( input.getDatasetName() ) );
    wCreateDataset.setSelection( input.getCreateDataset() );
    tName.setText( Const.nullToEmpty( input.getTableName() ) );
    wCreateTable.setSelection( input.getCreateTable() );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  
  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( null == wStepname.getText() || "".equals(wStepname.getText().trim()) ) {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
        mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
        mb.open();
      return;
    }
    stepname = wName.getText();
    //input.setName( wName.getText() );
    input.setUseContainerSecurity(wUseContainerAuth.getSelection() );
    input.setCredentialsPath(cpName.getText() );
    input.setProjectId( pName.getText() );
    input.setDatasetName( dsName.getText() );
    input.setCreateDataset( wCreateDataset.getSelection() );
    input.setTableName( tName.getText() );
    input.setCreateTable( wCreateTable.getSelection() );
    
    dispose();
  }


}