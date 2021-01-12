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

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import java.util.List;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class KuduSplitManager implements ConnectorSplitManager {

    private final String connectorId;
    private final KuduClientSession clientSession;

    @Inject
    public KuduSplitManager(KuduConnectorId connectorId, KuduClientSession clientSession) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                          ConnectorSession session, ConnectorTableLayoutHandle layout,
                                          SplitSchedulingStrategy splitSchedulingStrategy) {
        KuduTableLayoutHandle layoutHandle = (KuduTableLayoutHandle) layout;

        List<KuduSplit> splits = clientSession.buildKuduSplits(layoutHandle);

        return new FixedSplitSource(splits);
    }
}
