package org.itfactory.kettle.google.vfs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

import java.util.Collection;

/**
 * Created by puls3 on 04/07/2017.
 */
public class GCSFileSystem extends AbstractFileSystem implements FileSystem {

    private Storage storage;

    protected GCSFileSystem(FileName rootName, FileSystemOptions fileSystemOptions) {
        super(rootName, null, fileSystemOptions);
    }

    protected FileObject createFile(AbstractFileName abstractFileName) throws Exception {
        return new GCSFileObject(abstractFileName, this);
    }

    protected void addCapabilities(Collection<Capability> collection) {
        collection.addAll(GCSFileProvider.capabilities);
    }

    public Storage getStorage() {
        if(storage == null) {
            storage = StorageOptions.getDefaultInstance().getService();
        }

        return storage;
    }


}