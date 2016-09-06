package nifi.azure.dlstore;

import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.azure.management.datalake.store.DataLakeStoreAccountManagementClient;
import com.microsoft.azure.management.datalake.store.DataLakeStoreFileSystemManagementClient;
import com.microsoft.azure.management.datalake.store.implementation.DataLakeStoreAccountManagementClientImpl;
import com.microsoft.azure.management.datalake.store.implementation.DataLakeStoreFileSystemManagementClientImpl;
import com.microsoft.rest.credentials.ServiceClientCredentials;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Tags({"credentials", "data lake", "azure"})
@CapabilityDescription("Provides a controller service to Azure DL Store credentials properties.")
public class AzureDLConnectionService extends AbstractControllerService implements AzureDLPropertiesService{
    private static final Logger log = LoggerFactory.getLogger(AzureDLConnectionService.class);
    private static DataLakeStoreAccountManagementClient _adlsClient;
    private static DataLakeStoreFileSystemManagementClient _adlsFileSystemClient;

    private static ScheduledExecutorService _service;

    private static String _adlsAccountName;
    private static String _resourceGroupName;
    private static String _location;
    private static String _tenantId;
    private static String _subId;
    private static String _clientId;
    private static String _clientSecret;

    public static final PropertyDescriptor DATALAKE_ACCOUNT_NAME = new PropertyDescriptor.Builder()
            .name("Azure Data Lake Account Name")
            .description("Azure Data Lake Account Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESOURCE_GROUP_NAME = new PropertyDescriptor.Builder()
            .name("Resource Group Name")
            .description("Azure Resource Group Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor LOCATION = new PropertyDescriptor.Builder()
            .name("Location")
            .description("Azure Data Lake Store Location")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TENANT_ID = new PropertyDescriptor.Builder()
            .name("Tenant Id")
            .description("Azure Tenant Id")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SUB_ID = new PropertyDescriptor.Builder()
            .name("Subscription Id")
            .description("Azure Subscription Id")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Client Id")
            .description("Azure Client Id")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLEINT_SECRET_KEY = new PropertyDescriptor.Builder()
            .name("Client Secret Key")
            .description("Azure Client Secret Key")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> serviceProperties;

    static{
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DATALAKE_ACCOUNT_NAME);
        descriptors.add(RESOURCE_GROUP_NAME);
        descriptors.add(LOCATION);
        descriptors.add(TENANT_ID);
        descriptors.add(SUB_ID);
        descriptors.add(CLIENT_ID);
        descriptors.add(CLEINT_SECRET_KEY);
        serviceProperties = Collections.unmodifiableList(descriptors);
    }

    private Properties properties = new Properties();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return serviceProperties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException{
        log.info("Starting AzureDLConnectionService.");

        properties.clear();

        properties.put("DATALAKE_ACCOUNT_NAME", context.getProperty(DATALAKE_ACCOUNT_NAME).getValue());
        properties.put("RESOURCE_GROUP_NAME", context.getProperty(RESOURCE_GROUP_NAME).getValue());
        properties.put("LOCATION", context.getProperty(LOCATION).getValue());
        properties.put("TENANT_ID", context.getProperty(TENANT_ID).getValue());
        properties.put("SUB_ID", context.getProperty(SUB_ID).getValue());
        properties.put("CLIENT_ID", context.getProperty(CLIENT_ID).getValue());
        properties.put("CLEINT_SECRET_KEY", context.getProperty(CLEINT_SECRET_KEY).getValue());

        _adlsAccountName = context.getProperty(DATALAKE_ACCOUNT_NAME).getValue();
        _resourceGroupName = context.getProperty(RESOURCE_GROUP_NAME).getValue();
        _location = context.getProperty(LOCATION).getValue();
        _tenantId = context.getProperty(TENANT_ID).getValue();
        _subId = context.getProperty(SUB_ID).getValue();
        _clientId = context.getProperty(CLIENT_ID).getValue();
        _clientSecret = context.getProperty(CLEINT_SECRET_KEY).getValue();

        _service = Executors.newScheduledThreadPool(3, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(Runnable r) {
                final Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setName("Refresh Azure DL Store token");
                return t;
            }
        });

        Runnable runnable = new Runnable() {
            public void run() {
                _adlsClient = null;
                _adlsFileSystemClient = null;
                ApplicationTokenCredentials creds = new ApplicationTokenCredentials(_clientId, _tenantId, _clientSecret, null);
                SetupClients(creds);
            }
        };

        try
        {
            _service.scheduleAtFixedRate(runnable, 0, 24, TimeUnit.HOURS);
        }
        catch (Exception e)
        {
            log.error("Could not start AzureDLConnectionService. " + e.toString());
        }

        //for(String name : properties.stringPropertyNames()){
        //    log.info("Set " + name + " => " + properties.get(name));
        //}
    }

    /**
     * Shutdown pool, close all open connections.
     */
    @OnDisabled
    public void shutdown() {
        try {
            _service.shutdown();
        } catch (Exception e) {
            log.error("Could not shutdown AzureDLConnectionService. " + e.toString());
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public DataLakeStoreAccountManagementClient getAdlsClient() {
        return _adlsClient;
    }

    public DataLakeStoreFileSystemManagementClient getAdlsFileSystemClient() {
        return _adlsFileSystemClient;
    }

    //Set up clients
    public static void SetupClients(ServiceClientCredentials creds)
    {
        _adlsClient = new DataLakeStoreAccountManagementClientImpl(creds);
        _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClientImpl(creds);

        _adlsClient.withSubscriptionId(_subId);
    }
}
