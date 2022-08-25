package cn.sliew.flinkful.cli.descriptor.example;

import cn.sliew.flinkful.cli.base.CliClient;
import cn.sliew.flinkful.common.enums.DeploymentTarget;
import cn.sliew.flinkful.common.examples.FlinkExamples;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.*;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;

public class StandaloneSessionExample {

    public static void main(String[] args) throws Exception {
        String flink_home = System.getenv("FLINK_HOME");
        System.out.println(1);
        CliClient client = Util.buildCliClient();
        JobID submit = client.submit(DeploymentTarget.STANDALONE_SESSION, buildConfiguration(), Util.buildJarJob());
        System.out.println(1);
    }

    private static Configuration buildConfiguration() throws MalformedURLException {
        Configuration configuration = FlinkExamples.loadConfiguration();
        configuration.setString(JobManagerOptions.ADDRESS, "222.30.195.178");
        configuration.setInteger(JobManagerOptions.PORT, 6123);
        configuration.setInteger(RestOptions.PORT, 8081);
        URL exampleUrl = new File(FlinkExamples.EXAMPLE_JAR).toURL();
        ConfigUtils.encodeCollectionToConfig(configuration, PipelineOptions.JARS, Collections.singletonList(exampleUrl), Object::toString);
        return configuration;
    }
}
