package ml.littlebulb.presto.kudu;

import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import ml.littlebulb.presto.kudu.properties.RangePartition;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

import java.util.List;
import java.util.Map;

public interface KuduClientSession {
    List<String> listSchemaNames();

    List<SchemaTableName> listTables(String schemaNameOrNull);

    Schema getTableSchema(KuduTableHandle tableName);

    boolean tableExists(SchemaTableName schemaTableName);

    Map<String, Object> getTableProperties(KuduTableHandle tableName);

    List<KuduSplit> buildKuduSplits(KuduTableLayoutHandle layoutHandle);

    KuduScanner createScanner(KuduSplit kuduSplit);

    KuduTable openTable(SchemaTableName schemaTableName);

    KuduSession newSession();

    void createSchema(String schemaName);

    boolean schemaExists(String schemaName);

    void dropSchema(String schemaName);

    void dropTable(SchemaTableName schemaTableName);

    void renameTable(SchemaTableName schemaTableName, SchemaTableName newSchemaTableName);

    KuduTable createTable(ConnectorTableMetadata tableMetadata, boolean ignoreExisting);

    void addColumn(SchemaTableName schemaTableName, ColumnMetadata column);

    void dropColumn(SchemaTableName schemaTableName, String name);

    void renameColumn(SchemaTableName schemaTableName, String oldName, String newName);

    void addRangePartition(SchemaTableName schemaTableName, RangePartition rangePartition);

    void dropRangePartition(SchemaTableName schemaTableName, RangePartition rangePartition);
}
