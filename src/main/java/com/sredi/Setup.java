package com.sredi;

import com.sredi.common.RandomStringUtil;
import com.sredi.rdb.RdbBuilder;
import com.sredi.rdb.RdbUtil;
import com.sredi.replication.ReplicationConstants;
import com.sredi.replication.ReplicationRole;
import com.sredi.replication.WorkerConnectionProvider;
import com.sredi.utils.Constant;
import com.sredi.utils.Repository;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;

@Slf4j
public class Setup {

    public static int init(String[] args) {
        parseArgs(args);

        var dir = Repository.configGet("dir");
        var dbFileName = Repository.configGet("dbfilename");
        var port = Repository.configGet("port");

        if (dir != null && dbFileName != null) {
            try {
                var data = RdbUtil.openRdbFile(dir, dbFileName);
                var builder = new RdbBuilder().bytes(data);
                var rdb = builder.build();

                log.info("rdb: {}", rdb);
                if (rdb != null) {
                    rdb.init();
                }
            } catch (Exception e) {
                log.info("RDB Read Failed. init without RDB file.", e);
            }
        }

        if (port != null) {
            try {
                var portNumber = Integer.parseInt(port);

                return portNumber;
            } catch (Exception e) {
                log.info("Setting Custom Port Number Failed. use default port ({})", Constant.DEFAULT_PORT);
            }
        } else {
            Repository.configSet("port", String.valueOf(Constant.DEFAULT_PORT));
        }

        return Constant.DEFAULT_PORT;
    }

    public static void initReplica() {
        var replicaOf = Repository.getReplicationConfig(ReplicationConstants.REPLICATION_REPLICA_OF);

        if (replicaOf == null) {
            initReplicaForMaster();
        } else {
            initReplicaFoWorker();
        }
    }

    private static void initReplicaForMaster() {
        var replid = RandomStringUtil.createRandomString(40);

        Repository.setReplicationSetting("role", ReplicationRole.MASTER.name().toLowerCase());
        Repository.setReplicationSetting("master_replid", replid);
        Repository.setReplicationSetting("master_repl_offset", "0");
    }

    private static void initReplicaFoWorker() {
        Repository.setReplicationSetting("role", ReplicationRole.WORKER.name().toLowerCase());

        var replicaOf = Repository.getReplicationConfig(ReplicationConstants.REPLICATION_REPLICA_OF);
        var workerConnectionProvider = new WorkerConnectionProvider();

        workerConnectionProvider.init(replicaOf.getFirst(), Integer.parseInt(replicaOf.getLast()));
    }

    public static void parseArgs(String[] args) {
        var idx = 0;

        while (idx < args.length) {
            var keyword = args[idx++];

            if (ReplicationConstants.REPLICATION_CONFIG_LIST.contains(keyword)) {
                var values = new ArrayList<String>();
                while (idx < args.length && !args[idx].startsWith("--")) {
                    values.add(args[idx++]);
                }

                Repository.setReplicationConfig(keyword.substring(2), values);
            } else if (keyword.startsWith("--")) {
                var value = args[idx++];
                Repository.configSet(keyword.substring(2), value);
            }
        }
    }
}
