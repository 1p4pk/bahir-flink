package util;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

public class InfluxDBContainer<SELF extends InfluxDBContainer<SELF>> extends GenericContainer<SELF> {
    public static final Integer INFLUXDB_PORT = 8086;

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("quay.io/influxdb/influxdb:v2.0.2");

    private String bucket;
    private String username;
    private String password;

    public InfluxDBContainer() {
        super(DEFAULT_IMAGE_NAME);
        this.username = "any";
        this.password = "12345678";
        this.waitStrategy = (new WaitAllStrategy())
                .withStrategy(Wait.forHttp("/ping").withBasicCredentials(this.username, this.password).forStatusCode(204))
                .withStrategy(Wait.forListeningPort());
        this.withExposedPorts(INFLUXDB_PORT);
    }

    public SELF withBucket(String bucket) {
        this.bucket = bucket;
        return this.self();
    }

    public SELF withUsername(String username) {
        this.username = username;
        return this.self();
    }

    public SELF withPassword(String password) {
        this.password = password;
        return this.self();
    }

    public String getUrl() {
        return "http://" + this.getHost() + ":" + this.getMappedPort(INFLUXDB_PORT);
    }

    public InfluxDB getNewInfluxDB() {
        InfluxDB influxDB = InfluxDBFactory.connect(this.getUrl(), this.username, this.password);
        influxDB.setDatabase(this.bucket);
        return influxDB;
    }
}
