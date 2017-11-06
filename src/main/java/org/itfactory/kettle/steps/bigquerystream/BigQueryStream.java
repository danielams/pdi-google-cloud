
package org.itfactory.kettle.steps.bigquerystream;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMeta;

import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Iterator;
import java.util.Set;

/**
 * BigQuery Stream Output Step
 *
 * @author afowler
 * @since 06-nov-2017
 */
public class BigQueryStream extends BaseStep implements StepInterface {
  private static Class<?> PKG = BigQueryStreamMeta.class; // for i18n purposes, needed by Translator2!!

  private BigQueryStreamMeta meta;
  private BigQueryStreamData data;

  public BigQueryStream( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  // TODO finish the actual step implementation

}