package org.itfactory.kettle.google.vfs;

import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by puls3 on 04/07/2017.
 */
public class GCSFileProvider extends AbstractOriginatingFileProvider {

    public static final String SCHEME = "gcs";

    public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES =
            new UserAuthenticationData.Type[] {UserAuthenticationData.USERNAME,
                    UserAuthenticationData.PASSWORD };

    protected static final Collection<Capability> capabilities =
            Collections.unmodifiableCollection(
                    Arrays.asList(
                            new Capability[] {
                                    Capability.CREATE,
                                    Capability.DELETE,
                                    Capability.RENAME,
                                    Capability.GET_TYPE,
                                    Capability.LIST_CHILDREN,
                                    Capability.READ_CONTENT,
                                    Capability.URI,
                                    Capability.WRITE_CONTENT,
                                    Capability.GET_LAST_MODIFIED,
                                    Capability.RANDOM_ACCESS_READ
                            }));


    public GCSFileProvider() {
        super();
        setFileNameParser(GCSFileNameParser.getInstance());
    }

    protected FileSystem doCreateFileSystem(FileName fileName, FileSystemOptions fileSystemOptions) throws FileSystemException {
        return new GCSFileSystem(fileName, fileSystemOptions);
    }

    public Collection<Capability> getCapabilities() {
        return capabilities;
    }
}
