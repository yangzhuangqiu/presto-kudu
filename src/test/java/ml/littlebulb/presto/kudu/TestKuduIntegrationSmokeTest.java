package ml.littlebulb.presto.kudu;

import io.prestosql.spi.type.VarcharType;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.airlift.tpch.TpchTable.ORDERS;

/**
 * Kudu master server is expected to be running on localhost. At least one
 * Kudu tablet server must be running, too.
 * With Docker, use e.g.
 *   "docker run --rm -d --name apache-kudu --net=host usuresearch/kudu-docker-slim:release-v1.6.0-2"
 */
public class TestKuduIntegrationSmokeTest extends AbstractTestIntegrationSmokeTest {
    public static final String SCHEMA = "tpch";

    private QueryRunner queryRunner;

    public TestKuduIntegrationSmokeTest() {
        super(() -> KuduQueryRunnerFactory.createKuduQueryRunnerTpch(ORDERS));
    }

    @BeforeClass
    public void setUp() {
        queryRunner = getQueryRunner();
    }

    /**
     * Overrides original implementation because of usage of 'extra' column.
     */
    @Test
    @Override
    public void testDescribeTable() {
        MaterializedResult actualColumns = this.computeActual("DESC ORDERS").toTestTypes();
        MaterializedResult.Builder builder = MaterializedResult.resultBuilder(this.getQueryRunner().getDefaultSession(), VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR);
        for (MaterializedRow row: actualColumns.getMaterializedRows()) {
            builder.row(row.getField(0), row.getField(1), "", "");
        }
        MaterializedResult filteredActual = builder.build();
        builder = MaterializedResult.resultBuilder(this.getQueryRunner().getDefaultSession(), VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR);
        MaterializedResult expectedColumns = builder
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "varchar", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "").build();
        Assert.assertEquals(filteredActual, expectedColumns, String.format("%s != %s", filteredActual, expectedColumns));
    }


    @AfterClass(alwaysRun = true)
    public final void destroy() {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }
}
