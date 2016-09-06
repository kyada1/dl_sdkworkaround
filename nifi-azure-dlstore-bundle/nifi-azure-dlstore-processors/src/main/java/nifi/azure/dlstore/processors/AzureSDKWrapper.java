package nifi.azure.dlstore.processors;

import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.datalake.store.DataLakeStoreAccountManagementClient;
import com.microsoft.azure.management.datalake.store.DataLakeStoreFileSystemManagementClient;
import com.microsoft.azure.management.datalake.store.implementation.DataLakeStoreAccountManagementClientImpl;
import com.microsoft.azure.management.datalake.store.implementation.DataLakeStoreFileSystemManagementClientImpl;
import com.microsoft.azure.management.datalake.store.models.AdlsErrorException;
import com.microsoft.rest.ServiceResponse;

import java.io.IOException;

// Wrap call to the non-thread-safe MS SDK routine
public final class AzureSDKWrapper {

	private static Object lockGetCreds = new Object();
	private static Object lockGetDLClient = new Object();
	private static Object lockGetDLFileSysClient = new Object();
	private static Object lockDLCreateFile = new Object();

	// Token based credentials for use with a REST Service Client
	public static ApplicationTokenCredentials GetCreds(String clientId, String tenantId, String clientSecret) {
		synchronized (lockGetCreds) {
			ApplicationTokenCredentials creds = new ApplicationTokenCredentials(clientId, tenantId, clientSecret, null);
			return creds;
		}
	}

	// Creates Data Lake Store account management client
	public static DataLakeStoreAccountManagementClient GetDLClient(ApplicationTokenCredentials creds) {
		synchronized (lockGetDLClient) {
			DataLakeStoreAccountManagementClient adlsClient = new DataLakeStoreAccountManagementClientImpl(creds);
			return adlsClient;
		}
	}

	// Creates Data Lake Store filesystem management client
	public static DataLakeStoreFileSystemManagementClient GetDLFileSysClient(ApplicationTokenCredentials creds,
																			 DataLakeStoreAccountManagementClient adlsClient,
																			 String subId) {
		synchronized (lockGetDLFileSysClient) {
			DataLakeStoreFileSystemManagementClient adlsFileSystemClient = new DataLakeStoreFileSystemManagementClientImpl(creds);
			adlsClient.withSubscriptionId(subId);
			return adlsFileSystemClient;
		}
	}

	// Create a file in Azure DL Store
	public static ServiceResponse<Void> DLCreateFile(DataLakeStoreAccountManagementClient adlsClient,
													 DataLakeStoreFileSystemManagementClient adlsFileSystemClient,
													 String adlsAccountName,
													 String path,
													 String contents,
													 boolean force)
													throws IOException, AdlsErrorException {
		synchronized (lockDLCreateFile) {
			byte[] bytesContents = contents.getBytes();

			// Create file with contents
			return adlsFileSystemClient.fileSystems().create(adlsAccountName, path, bytesContents, force);
		}
	}
}
