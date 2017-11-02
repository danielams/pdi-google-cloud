package org.itfactory.kettle.google.vfs;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.pentaho.di.core.annotations.KettleLifecyclePlugin;
import org.pentaho.di.core.lifecycle.KettleLifecycleListener;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;

import java.util.Arrays;

/**
 * Created by puls3 on 04/07/2017.
 */
@KettleLifecyclePlugin( id = "GCSFileSystemBootstrap", name = "GCS FileSystem Bootstrap" )
public class GCSFileSystemBootstrap implements KettleLifecycleListener {
    private static Class<?> PKG = GCSFileSystemBootstrap.class;
    private LogChannelInterface log = new LogChannel( GCSFileSystemBootstrap.class.getName() );

    public void onEnvironmentInit() throws LifecycleException {
        try {
            // Register S3 as a file system type with VFS
            FileSystemManager fsm = KettleVFS.getInstance().getFileSystemManager();
            if ( fsm instanceof DefaultFileSystemManager) {
                if ( !Arrays.asList( fsm.getSchemes() ).contains( GCSFileProvider.SCHEME ) ) {
                    ( (DefaultFileSystemManager) fsm ).addProvider( GCSFileProvider.SCHEME, new GCSFileProvider() );
                }
            }
        } catch ( FileSystemException e ) {
            log.logError( BaseMessages.getString( PKG, "GCSSpoonPlugin.StartupError.FailedToLoadGCSDriver" ) );
        }
    }

    public void onEnvironmentShutdown() {

    }
}
