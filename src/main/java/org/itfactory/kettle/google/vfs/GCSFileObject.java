package org.itfactory.kettle.google.vfs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.CopyRequest;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.commons.vfs2.provider.URLFileName;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by puls3 on 04/07/2017.
 */
public class GCSFileObject extends AbstractFileObject {

    public static final String DELIMITER = "/";
    private GCSFileSystem fileSystem = null;
    private Bucket bucket = null;

    public GCSFileObject(AbstractFileName name, AbstractFileSystem fs) throws Exception {
        super(name, fs);
        this.fileSystem = (GCSFileSystem) fs;

        getGCSBucket();
    }

    protected Bucket getGCSBucket() throws Exception {
        if (bucket == null) {
            String bucketName = ((URLFileName) this.getName()).getHostName();

            Storage storage = fileSystem.getStorage();
            if (storage != null) {
                bucket = storage.get(bucketName);
            } else {
                return null;
            }
        }

        return bucket;
    }

    protected Blob getBlob() throws Exception {
        String path = getBucketRelativeGCSPath(this);
        return bucket.get(path);
    }

    protected Blob getOrCreateBlob(FileObject file) throws Exception {
        Blob blob = getBlob();

        if (blob == null) {
            BlobId blobId = BlobId.of(bucket.getName(), getBucketRelativeGCSPath(file));
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(getContent().getContentInfo().getContentType()).build();
            blob = fileSystem.getStorage().create(blobInfo);
        }

        return blob;
    }

    private String getBucketRelativeGCSPath(FileObject file) {
        if (file.getName().getPath().indexOf(DELIMITER, 0) >= 0) {
            String currentPath = file.getName().getPath();
            String relativePath = currentPath.substring(currentPath.indexOf(DELIMITER, 0) + 1);
            return relativePath;
        } else {
            return "";
        }
    }

    protected long doGetContentSize() throws Exception {
        Blob b = getBlob();

        if (b != null)
            return getBlob().getSize();
        else
            throw new FileSystemException("Unable to get content size.");
    }

    protected InputStream doGetInputStream() throws Exception {
        Blob b = getBlob();

        if (b != null)
            return Channels.newInputStream(getBlob().reader());
        else
            throw new FileSystemException("Unable to get blob to read.");
    }

    @Override
    public boolean delete() throws FileSystemException {
        Blob b;
        try {
            b = getBlob();
        } catch (Exception e) {
            throw new FileSystemException(e);
        }

        if (b != null)
            b.delete();
        else
            return false;

        return true;
    }

    @Override
    protected OutputStream doGetOutputStream(boolean bAppend) throws Exception {
        return Channels.newOutputStream(getOrCreateBlob(this).writer());
    }

    protected FileType doGetType() throws Exception {

        if (getName().getPath().equals("") || getName().getPath().equals(DELIMITER) || getName().getPath()
                .endsWith(DELIMITER)) {
            return FileType.FOLDER;
        }

        Blob b = getBlob();
        if(b == null) {
            String testFolderStr = getBucketRelativeGCSPath(this).concat(DELIMITER);
            Blob testFolder = bucket.get(testFolderStr);

            if(testFolder == null)
                return FileType.IMAGINARY;
            else
                return FileType.FOLDER;
        }
        else {
            return FileType.FILE;
        }
    }

    protected String[] doListChildren() throws Exception {

        if(getType() != FileType.FOLDER)
            return new String[]{};

        List<String> childrenList = new ArrayList<String>();

        String folderPrefix = getBucketRelativeGCSPath(this);
        if(!folderPrefix.endsWith(DELIMITER)) {
            folderPrefix = folderPrefix + DELIMITER;
        }

        Page<Blob> blobs = bucket.list(BlobListOption.currentDirectory(),
                BlobListOption.prefix(folderPrefix));

        for (Blob blob : blobs.iterateAll()) {
            if(!blob.getName().equals(folderPrefix))
                childrenList.add(blob.getName().substring(folderPrefix.length()));
        }

        String[] childrenArr = new String[childrenList.size()];
        childrenArr = childrenList.toArray(childrenArr);

        return childrenArr;
    }

    @Override
    protected long doGetLastModifiedTime() throws Exception {
        return getBlob().getUpdateTime();
    }

    @Override
    protected void doRename(FileObject newFile) throws Exception {
        CopyRequest request = CopyRequest.newBuilder()
                .setSource(getBlob().getBlobId())
                .setTarget(BlobId.of(bucket.getName(), getBucketRelativeGCSPath(newFile)))
                .build();

        Blob blob = fileSystem.getStorage().copy(request).getResult();

        if (blob == null)
            throw new FileSystemException("Unable to rename blob.");

        delete();
    }

    @Override
    protected void doCreateFolder() throws Exception {
        Blob blob = getBlob();

        if (blob == null) {
            BlobId blobId = BlobId.of(bucket.getName(), getBucketRelativeGCSPath(this)+DELIMITER);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(getContent().getContentInfo().getContentType()).build();
            fileSystem.getStorage().create(blobInfo);
        }
    }
}
