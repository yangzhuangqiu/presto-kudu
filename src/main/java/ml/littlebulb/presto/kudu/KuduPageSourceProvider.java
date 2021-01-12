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

import io.prestosql.spi.connector.*;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KuduPageSourceProvider implements ConnectorPageSourceProvider {

    private KuduRecordSetProvider recordSetProvider;

    @Inject
    public KuduPageSourceProvider(KuduRecordSetProvider recordSetProvider) {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
                                                ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns) {
        KuduRecordSet recordSet = (KuduRecordSet) recordSetProvider.getRecordSet(transactionHandle, session, split, columns);
        if (columns.contains(KuduColumnHandle.ROW_ID_HANDLE)) {
            return new KuduUpdatablePageSource(recordSet);
        } else {
            return new RecordPageSource(recordSet);
        }
    }
}
