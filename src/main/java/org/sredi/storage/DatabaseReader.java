package org.sredi.storage;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Map;

import org.sredi.rdb.OpCode;
import org.sredi.rdb.RdbFileParser;

public class DatabaseReader {

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
        System.out.println("Reading database file: " + dbFile);
        System.out.println("File size: " + dbFile.length());
        InputStream dbFileInput = new FileInputStream(dbFile);
        try {
            RdbFileParser rdbFileParser = new RdbFileParser(new BufferedInputStream(dbFileInput), clock);
            OpCode dbCode = rdbFileParser.initDB();
            // skip AUX section
            if (dbCode == OpCode.AUX) {
                dbCode = rdbFileParser.skipAux();
            }
            // db code should be SELECTDB now.
            if (dbCode != OpCode.SELECTDB) {
                throw new IOException(
                        String.format("Database unexpected OpCode: 0x%X not equal to 0x%X",
                                dbCode.getCode(), OpCode.SELECTDB.getCode()));
            }
            // select DB (0)
            if (dbCode == OpCode.SELECTDB) {
                rdbFileParser.selectDB(dataStoreMap);
            } else {
                throw new IOException(
                        String.format("Database unexpected OpCode: 0x%X not equal to 0x%X",
                                dbCode.getCode(), OpCode.SELECTDB.getCode()));
            }
        } finally {
            // close file
            dbFileInput.close();
        }
    }

}
