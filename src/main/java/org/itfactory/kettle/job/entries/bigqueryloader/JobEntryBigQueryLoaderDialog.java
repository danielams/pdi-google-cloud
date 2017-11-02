package org.itfactory.kettle.job.entries.bigqueryloader;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.job.entry.JobEntryDialog;

public class JobEntryBigQueryLoaderDialog extends JobEntryDialog implements JobEntryDialogInterface {

    public JobEntryBigQueryLoaderDialog(Shell parent, JobEntryInterface jobEntry, Repository rep, JobMeta jobMeta) {
        super(parent, jobEntry, rep, jobMeta);
    }

    public JobEntryInterface open() {
        return null;
    }
}
