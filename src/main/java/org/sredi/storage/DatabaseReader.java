package org.sredi.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sredi.rdb.OpCode;
import org.sredi.rdb.RdbFileParser;

public class DatabaseReader {
    private static final Logger log = LoggerFactory.getLogger(DatabaseReader.class);

    private final File dbFile;
    private final Map<String, StoredData> dataStoreMap;
    private Clock clock;

    public DatabaseReader(File dbFile, Map<String, StoredData> dataStoreMap, Clock clock) {
        this.dbFile = dbFile;
        this.dataStoreMap = dataStoreMap;
        this.clock = clock;
    }

    public void readDatabase() throws IOException {
        // open file and read as input stream
        log.info("Reading database file: {}", dbFile);
        log.info("File size: {}", dbFile.length());
        InputStream dbFileInput = new FileInputStream(dbFile);
        try {
            RdbFileParser rdbFileParser = new RdbFileParser(new BufferedInputStream(dbFileInput), clock);
            OpCode dbCode = rdbFileParser.initDB();
            // skip AUX section
            if (dbCode == OpCode.AUX) {
                dbCode = rdbFileParser.skipAux();
            }
            // db code should be SELECTDB now.
            String format = String.format("Database unexpected OpCode: 0x%X not equal to 0x%X",
                    dbCode.getCode(), OpCode.SELECTDB.getCode());
            if (dbCode != OpCode.SELECTDB) {
                throw new IOException(
                        format);
            }
            // select DB (0)
            rdbFileParser.selectDB(dataStoreMap);
        } finally {
            // close file
            dbFileInput.close();
        }
    }

}
