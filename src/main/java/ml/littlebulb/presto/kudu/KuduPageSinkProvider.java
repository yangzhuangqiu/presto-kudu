package ml.littlebulb.presto.kudu;

import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KuduPageSinkProvider implements ConnectorPageSinkProvider {
    private final KuduClientSession clientSession;

    @Inject
    public KuduPageSinkProvider(KuduClientSession clientSession) {
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle) {
        requireNonNull(outputTableHandle, "outputTableHandle is null");
        checkArgument(outputTableHandle instanceof KuduOutputTableHandle, "outputTableHandle is not an instance of KuduOutputTableHandle");
        KuduOutputTableHandle handle = (KuduOutputTableHandle) outputTableHandle;

        return new KuduPageSink(session, clientSession, handle, handle.isGenerateUUID());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle) {
        requireNonNull(insertTableHandle, "insertTableHandle is null");
        checkArgument(insertTableHandle instanceof KuduInsertTableHandle, "insertTableHandle is not an instance of KuduInsertTableHandle");
        KuduInsertTableHandle handle = (KuduInsertTableHandle) insertTableHandle;

        return new KuduPageSink(session, clientSession, handle, false);
    }
}
