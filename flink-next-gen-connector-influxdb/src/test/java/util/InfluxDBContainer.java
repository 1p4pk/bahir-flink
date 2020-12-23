package util;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InfluxDBContainer<SELF extends InfluxDBContainer<SELF>> extends GenericContainer<SELF> {
    public static final Integer INFLUXDB_PORT = 8086;

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("quay.io/influxdb/influxdb:v2.0.2");
    private static final String INFLUX_SETUP = "influx-setup.sh";
    private static final String DATA = "bird-migration.txt";

    private String username;
    private String password;
    private String bucket;
    private String organization;

    public InfluxDBContainer() {
        super(DEFAULT_IMAGE_NAME);
        this.username = "any";
        this.password = "12345678";
        this.bucket = "test-bucket";
        this.organization = "HPI";
        this.waitStrategy = (new WaitAllStrategy())
                .withStrategy(Wait.forHttp("/ping").withBasicCredentials(this.username, this.password).forStatusCode(204))
                .withStrategy(Wait.forListeningPort());
        this.withExposedPorts(INFLUXDB_PORT);
    }

    public void startPreIngestedInfluxDB() {
        this.withCopyFileToContainer(MountableFile.forClasspathResource(DATA), String.format("/%s", DATA)).
                withCopyFileToContainer(MountableFile.forClasspathResource(INFLUX_SETUP), String.format("%s", INFLUX_SETUP));

        this.start();

        this.writeDataToInfluxDB();
    }

    public InfluxDB getNewInfluxDB() {
        InfluxDB influxDB = InfluxDBFactory.connect(this.getUrl(), this.username, this.password);
        influxDB.setDatabase(this.bucket);
        return influxDB;
    }

    private void writeDataToInfluxDB() {
        try {
            Container.ExecResult execResult = this.execInContainer("chmod", "-x", "/influx-setup.sh");
            assertEquals(execResult.getExitCode(), 0);
            Container.ExecResult writeResult = this.execInContainer("/bin/bash", "/influx-setup.sh", this.username, this.password, this.bucket, this.organization, DATA);
            assertEquals(writeResult.getExitCode(), 0);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private String getUrl() {
        return "http://" + this.getHost() + ":" + this.getMappedPort(INFLUXDB_PORT);
    }
}
