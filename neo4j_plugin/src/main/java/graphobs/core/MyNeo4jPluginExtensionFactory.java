package graphobs.core;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import graphobs.datasources.opensearch.*;
import graphobs.datasources.prometheus.*;
import graphobs.datasources.jaeger.JaegerImportProcedure;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;


@ServiceProvider
public class MyNeo4jPluginExtensionFactory extends ExtensionFactory<MyNeo4jPluginExtensionFactory.Dependencies> {

    public MyNeo4jPluginExtensionFactory() {
        super(ExtensionType.DATABASE, "MyExtensionFactory");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        // Pass dependencies to the adapter instance - each adapter gets its own references
        return new MyAdapter(
            dependencies.db(),
            dependencies.databaseManagementService(),
            dependencies.log()
        );
    }

    public class MyAdapter extends LifecycleAdapter {

        // Static lock to prevent parallel execution of auto-registration across all instances
        private static final Object REGISTRATION_LOCK = new Object();
        private static volatile boolean registrationInProgress = false;

        private final GraphDatabaseService db;
        private final DatabaseManagementService managementService;
        private final LogService log;
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        public MyAdapter(GraphDatabaseService db, DatabaseManagementService managementService, LogService log) {
            this.db = db;
            this.managementService = managementService;
            this.log = log;
        }

        @Override
        public void start() throws Exception {
            if (!db.databaseName().equals(SYSTEM_DATABASE_NAME)) {
                log.getUserLog(MyNeo4jPluginExtensionFactory.class).info("Registering transaction event listener for database " + db.databaseName());
                managementService.registerTransactionEventListener(
                        db.databaseName(),
                        new MyTransactionEventListener(db, log)
                );
            } else {
                log.getUserLog(MyNeo4jPluginExtensionFactory.class).info("System database. Not registering transaction event listener");
            }

            // Only run auto-registration for the default 'neo4j' database (not system or other databases)
            if ("neo4j".equals(db.databaseName())) {
                scheduler.schedule(() -> {
                    Log userLog = log.getUserLog(MyNeo4jPluginExtensionFactory.class);
                    try {
                        // Index-Erstellung
                        try (Transaction tx = db.beginTx()) {
                            createIndexIfNotExists(tx, userLog, "time_series", new String[]{"start", "end"});
                            createIndexIfNotExists(tx, userLog, "time_series", new String[]{"start"});
                            createIndexIfNotExists(tx, userLog, "time_series", new String[]{"end"});

                            createIndexIfNotExists(tx, userLog, "time_period", new String[]{"end", "start"});
                            createIndexIfNotExists(tx, userLog, "time_period", new String[]{"start"});
                            createIndexIfNotExists(tx, userLog, "time_period", new String[]{"end"});

                            // UNIQUE constraints to prevent duplicates from race conditions
                            createUniqueConstraintIfNotExists(tx, userLog, "Service", "id");
                            createUniqueConstraintIfNotExists(tx, userLog, "Operation", "id");

                            tx.commit();
                        }
                        userLog.info("Indexes and constraints created successfully.");

                        // Auto-Registrierung via Umgebungsvariablen (synchronized to prevent race conditions)
                        synchronized (REGISTRATION_LOCK) {
                            if (registrationInProgress) {
                                userLog.info("Auto-registration already in progress, skipping.");
                                return;
                            }
                            registrationInProgress = true;
                        }
                        try {
                            autoRegisterDataSources(userLog);
                        } finally {
                            registrationInProgress = false;
                        }

                    } catch (Exception e) {
                        userLog.error("Startup initialization failed: " + e.getMessage(), e);
                    }
                }, 30, TimeUnit.SECONDS);
            }
        }

        /**
         * Auto-registers Prometheus and Jaeger data sources based on environment variables.
         * Set PROMETHEUS_URL and JAEGER_URL environment variables to enable auto-registration.
         */
        private void autoRegisterDataSources(Log userLog) {
            String prometheusUrl = System.getenv("PROMETHEUS_URL");
            String jaegerUrl = System.getenv("JAEGER_URL");
            String opensearchUrl = System.getenv("OPENSEARCH_URL");

            // Register Prometheus if URL is provided
            if (prometheusUrl != null && !prometheusUrl.isBlank()) {
                userLog.info("Auto-registering Prometheus data source: " + prometheusUrl);
                try {
                    PrometheusClient client = new PrometheusHttpClient(prometheusUrl, userLog);
                    PodRepository repo = new Neo4jPodRepository(db);
                    PrometheusRegistrationService svc = new PrometheusRegistrationService(client, repo, userLog);
                    long podCount = svc.register(prometheusUrl);
                    userLog.info("Prometheus registration complete. Registered " + podCount + " pods.");
                    // Note: ServicePodLinker is called inside JaegerImportProcedure.importTraces()
                } catch (Exception e) {
                    userLog.error("Failed to register Prometheus: " + e.getMessage(), e);
                }
            } else {
                userLog.info("PROMETHEUS_URL not set, skipping Prometheus auto-registration.");
            }

            // Import Jaeger graph if URL is provided
            if (jaegerUrl != null && !jaegerUrl.isBlank()) {
                userLog.info("Auto-importing Jaeger graph: " + jaegerUrl);
                try {
                    JaegerImportProcedure jaegerImport = new JaegerImportProcedure();
                    jaegerImport.db = db;
                    jaegerImport.log = userLog;
                    jaegerImport.importTraces(jaegerUrl).forEach(result -> {
                        userLog.info("Jaeger import result: " + result.message +
                                " (services=" + result.services +
                                ", operations=" + result.operations +
                                ", dependencies=" + result.dependencies + ")");
                    });
                } catch (Exception e) {
                    userLog.error("Failed to import Jaeger graph: " + e.getMessage(), e);
                }
            } else {
                userLog.info("JAEGER_URL not set, skipping Jaeger auto-import.");
            }

            // Register OpenSearch if URL is provided
            if (opensearchUrl != null && !opensearchUrl.isBlank()) {
                userLog.info("Auto-registering OpenSearch data source: " + opensearchUrl);
                try {
                    String osUser = System.getenv("OPENSEARCH_USER");
                    String osPassword = System.getenv("OPENSEARCH_PASSWORD");
                    String osIndex = System.getenv("OPENSEARCH_INDEX");
                    if (osIndex == null || osIndex.isBlank()) osIndex = "*";

                    OpenSearchClient osClient = new OpenSearchHttpClient(opensearchUrl, osUser, osPassword, userLog);
                    LogRepository osRepo = new Neo4jLogRepository(db);
                    OpenSearchRegistrationService osSvc = new OpenSearchRegistrationService(osClient, osRepo, userLog);
                    long indexCount = osSvc.register(opensearchUrl, osIndex);
                    userLog.info("OpenSearch registration complete. Registered " + indexCount + " indices.");

                    // Store credentials on OpenSearch node
                    if (osUser != null && !osUser.isBlank()) {
                        try (Transaction tx = db.beginTx()) {
                            java.util.Map<String, Object> credParams = new java.util.HashMap<>();
                            credParams.put("url", opensearchUrl);
                            credParams.put("user", osUser);
                            credParams.put("password", osPassword);
                            tx.execute("MATCH (os:OpenSearch {url:$url}) SET os.user=$user, os.password=$password", credParams);
                            tx.commit();
                        }
                    }
                } catch (Exception e) {
                    userLog.error("Failed to register OpenSearch: " + e.getMessage(), e);
                }
            } else {
                userLog.info("OPENSEARCH_URL not set, skipping OpenSearch auto-registration.");
            }
        }

        private void createUniqueConstraintIfNotExists(Transaction tx, Log userLog, String label, String property) {
            String constraintName = label.toLowerCase() + "_" + property + "_unique";
            String query = "CREATE CONSTRAINT " + constraintName + " IF NOT EXISTS FOR (n:" + label + ") REQUIRE n." + property + " IS UNIQUE";
            try {
                tx.execute(query);
                userLog.info("Constraint created: " + constraintName);
            } catch (Exception e) {
                userLog.warn("Could not create constraint " + constraintName + ": " + e.getMessage());
            }
        }

        private void createIndexIfNotExists(Transaction tx, Log userLog, String label, String[] properties) {
            String indexName = label + "_" + String.join("_", properties) + "_index";

            // Build CREATE INDEX query with IF NOT EXISTS
            StringBuilder createQuery = new StringBuilder();
            createQuery.append("CREATE INDEX ").append(indexName)
                    .append(" IF NOT EXISTS FOR (n:").append(label).append(") ON (");
            for (int i = 0; i < properties.length; i++) {
                createQuery.append("n.").append(properties[i]);
                if (i < properties.length - 1) {
                    createQuery.append(", ");
                }
            }
            createQuery.append(")");
            userLog.info("Index erstellt: " + createQuery.toString());
            tx.execute(createQuery.toString());
            userLog.info("Index erstellt: " + indexName);
        }
    }

    interface Dependencies {
        GraphDatabaseService db();
        DatabaseManagementService databaseManagementService();
        LogService log();
    }
}
