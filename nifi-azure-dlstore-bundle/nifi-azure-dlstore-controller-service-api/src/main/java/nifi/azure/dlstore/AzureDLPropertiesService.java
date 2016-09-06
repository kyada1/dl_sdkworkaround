package nifi.azure.dlstore;

import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;

import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.datalake.store.DataLakeStoreAccountManagementClient;
import com.microsoft.azure.management.datalake.store.DataLakeStoreFileSystemManagementClient;
import com.microsoft.azure.management.datalake.store.implementation.DataLakeStoreAccountManagementClientImpl;
import com.microsoft.azure.management.datalake.store.implementation.DataLakeStoreFileSystemManagementClientImpl;
import com.microsoft.rest.credentials.ServiceClientCredentials;

public interface AzureDLPropertiesService extends ControllerService{
    String getProperty(String key);
	DataLakeStoreAccountManagementClient getAdlsClient();
	DataLakeStoreFileSystemManagementClient getAdlsFileSystemClient() ;
}
