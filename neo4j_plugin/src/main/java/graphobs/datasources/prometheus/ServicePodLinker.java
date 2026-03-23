package graphobs.datasources.prometheus;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import graphobs.matching.ServicePodMatcher;


public class ServicePodLinker {

    private final GraphDatabaseService db;
    private final Log log;

    public ServicePodLinker(GraphDatabaseService db, Log log) {
        this.db = db;
        this.log = log;
    }

    public void linkServicesAndPods() {
        ServicePodMatcher.linkServicesAndPods(db, log);
        linkServicesAndOperationsToPrometheus();
    }

    public void linkServicesAndOperationsToPrometheus() {
        ServicePodMatcher.linkServicesAndOperationsToPrometheus(db);
    }
}