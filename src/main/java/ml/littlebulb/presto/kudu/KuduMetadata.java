/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ml.littlebulb.presto.kudu;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import ml.littlebulb.presto.kudu.properties.KuduTableProperties;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static ml.littlebulb.presto.kudu.Types.checkType;

public class KuduMetadata implements ConnectorMetadata {
    private final String connectorId;
    private KuduClientSession clientSession;

    @Inject
    public KuduMetadata(KuduConnectorId connectorId, KuduClientSession clientSession) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return clientSession.listSchemaNames();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return clientSession.listTables(schemaName.orElse(null));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix) {
        requireNonNull(prefix, "SchemaTablePrefix is null");

        List<SchemaTableName> tables;
        if (prefix.isEmpty()) {
            tables = listTables(session, Optional.empty());
        } else if (!prefix.getTable().isPresent()) {
            tables = listTables(session, prefix.getSchema());
        } else {
            tables = ImmutableList.of(new SchemaTableName(prefix.getSchema().orElse(null), prefix.getTable().orElse(null)));
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : tables) {
            KuduTableHandle tableHandle = getTableHandle(session, tableName);
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);
            columns.put(tableName, tableMetadata.getColumns());
        }
        return columns.build();
    }


    private ConnectorTableMetadata getTableMetadata(KuduTableHandle tableHandle) {
        KuduTable table = tableHandle.getTable(clientSession);
        Schema schema = table.getSchema();

        List<ColumnMetadata> columnsMetaList = schema.getColumns().stream()
                .filter(col -> !col.isKey() || !col.getName().equals(KuduColumnHandle.ROW_ID))
                .map(col -> {
                    StringBuilder extra = new StringBuilder();
                    if (col.isKey()) {
                        extra.append("key, ");
                    } else if (col.isNullable()) {
                        extra.append("nullable, ");
                    }
                    if (col.getEncoding() != null) {
                        extra.append("encoding=").append(col.getEncoding().name()).append(", ");
                    }
                    if (col.getCompressionAlgorithm() != null) {
                        extra.append("compression=").append(col.getCompressionAlgorithm().name()).append(", ");
                    }
                    if (extra.length() > 2) {
                        extra.setLength(extra.length() - 2);
                    }
                    Type prestoType = TypeHelper.fromKuduColumn(col);
                    return new ColumnMetadata(col.getName(), prestoType, null, extra.toString(), false);
                }).collect(toImmutableList());

        Map<String, Object> properties = clientSession.getTableProperties(tableHandle);
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columnsMetaList, properties);
    }

    private KuduTableHandle fromConnectorTableHandle(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return checkType(tableHandle, KuduTableHandle.class, "tableHandle");
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                      ConnectorTableHandle connectorTableHandle) {
        KuduTableHandle tableHandle = fromConnectorTableHandle(session, connectorTableHandle);
        Schema schema = clientSession.getTableSchema(tableHandle);

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            ColumnSchema col = schema.getColumnByIndex(i);
            String name = col.getName();
            Type type = TypeHelper.fromKuduColumn(col);
            KuduColumnHandle columnHandle = new KuduColumnHandle(name, i, type);
            columnHandles.put(name, columnHandle);
        }

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        fromConnectorTableHandle(session, tableHandle);
        KuduColumnHandle kuduColumnHandle = checkType(columnHandle, KuduColumnHandle.class, "columnHandle");
        if (kuduColumnHandle.isVirtualRowId()) {
            return new ColumnMetadata(KuduColumnHandle.ROW_ID, VarbinaryType.VARBINARY, null, true);
        } else {
            return kuduColumnHandle.getColumnMetadata();
        }
    }

    @Override
    public KuduTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
        try {
            KuduTable table = clientSession.openTable(schemaTableName);
            return new KuduTableHandle(connectorId, schemaTableName, table);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
                                                            ConnectorTableHandle tableHandle,
                                                            Constraint constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns) {
        KuduTableHandle handle = fromConnectorTableHandle(session, tableHandle);
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new KuduTableLayoutHandle(handle, constraint.getSummary(), desiredColumns));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getTableMetadataInternal(session, tableHandle);
    }

    private ConnectorTableMetadata getTableMetadataInternal(ConnectorSession session,
                                                            ConnectorTableHandle tableHandle) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        return getTableMetadata(kuduTableHandle);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties) {
        clientSession.createSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName) {
        clientSession.dropSchema(schemaName);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {
        clientSession.createTable(tableMetadata, ignoreExisting);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        clientSession.dropTable(kuduTableHandle.getSchemaTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        clientSession.renameTable(kuduTableHandle.getSchemaTableName(), newTableName);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        clientSession.addColumn(kuduTableHandle.getSchemaTableName(), column);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) column;
        clientSession.dropColumn(kuduTableHandle.getSchemaTableName(), kuduColumnHandle.getName());
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source,
                             String target) {
        KuduTableHandle kuduTableHandle = fromConnectorTableHandle(session, tableHandle);
        KuduColumnHandle kuduColumnHandle = (KuduColumnHandle) source;
        clientSession.renameColumn(kuduTableHandle.getSchemaTableName(), kuduColumnHandle.getName(), target);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle connectorTableHandle) {
        KuduTableHandle tableHandle = fromConnectorTableHandle(session, connectorTableHandle);

        KuduTable table = tableHandle.getTable(clientSession);
        Schema schema = table.getSchema();

        List<ColumnSchema> columns = schema.getColumns();
        List<String> columnNames = columns.stream().map(ColumnSchema::getName).collect(toImmutableList());
        List<Type> columnTypes = columns.stream()
                .map(TypeHelper::fromKuduColumn).collect(toImmutableList());

        return new KuduInsertTableHandle(
                connectorId,
                tableHandle.getSchemaTableName(),
                columnNames,
                columnTypes,
                table);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics) {
        return Optional.empty();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
                                                       Optional<ConnectorNewTableLayout> layout) {
        boolean generateUUID = !tableMetadata.getProperties().containsKey(KuduTableProperties.PARTITION_DESIGN);
        ConnectorTableMetadata finalTableMetadata = tableMetadata;
        if (generateUUID) {
            String rowId = KuduColumnHandle.ROW_ID;
            List<ColumnMetadata> copy = new ArrayList<>(tableMetadata.getColumns());
            copy.add(0, new ColumnMetadata(rowId, VarcharType.VARCHAR, "key=true", null, true));
            List<ColumnMetadata> finalColumns = ImmutableList.copyOf(copy);
            Map<String, Object> propsCopy = new HashMap<>(tableMetadata.getProperties());
            propsCopy.put(KuduTableProperties.COLUMN_DESIGN, "{\"" + rowId + "\": {\"key\": true}}");
            propsCopy.put(KuduTableProperties.PARTITION_DESIGN, "{\"hash\": [{\"columns\": [\"" + rowId + "\"], " +
                    "\"buckets\": 2}]}");
            propsCopy.put(KuduTableProperties.NUM_REPLICAS, 1);
            Map<String, Object> finalProperties = ImmutableMap.copyOf(propsCopy);
            finalTableMetadata = new ConnectorTableMetadata(tableMetadata.getTable(),
                    finalColumns, finalProperties, tableMetadata.getComment());
        }
        KuduTable table = clientSession.createTable(finalTableMetadata, false);

        Schema schema = table.getSchema();

        List<ColumnSchema> columns = schema.getColumns();
        List<String> columnNames = columns.stream().map(ColumnSchema::getName).collect(toImmutableList());
        List<Type> columnTypes = columns.stream()
                .map(TypeHelper::fromKuduColumn).collect(toImmutableList());
        List<Type> columnOriginalTypes = finalTableMetadata.getColumns().stream()
                .map(ColumnMetadata::getType).collect(toImmutableList());

        return new KuduOutputTableHandle(
                connectorId,
                finalTableMetadata.getTable(),
                columnOriginalTypes,
                columnNames,
                columnTypes,
                generateUUID,
                table);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics) {
        return Optional.empty();
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return KuduColumnHandle.ROW_ID_HANDLE;
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return fromConnectorTableHandle(session, tableHandle);
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments) {
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle,
                                          ConnectorTableLayoutHandle tableLayoutHandle) {
        return false;
    }


}