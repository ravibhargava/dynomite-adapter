package com.thomsonreuters.adapter.test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.netflix.config.ConfigurationManager;
import com.palantir.docker.compose.DockerComposition;
import com.thomsonreuters.adapter.impl.AdapterImpl;


@Category(IntegrationTest.class)
public class AdapterTest implements Serializable{
	private static JavaSparkContext sc = new JavaSparkContext("local", "Test", System.getenv("SPARK_HOME"), System.getenv("JARS"));;
	@ClassRule
	public static DockerComposition composition = DockerComposition.of("src/test/resources/docker-compose-dynomite.yml")
    .build();
	@BeforeClass 
	public static void startup() {
		try {
			ConfigurationManager.loadPropertiesFromResources("junit.properties");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Test
	public void testClient() {
		try {
			final String key = UUID.randomUUID().toString();
			JavaRDD<String> rdd = sc.parallelize(Arrays.asList("1", "2", "3", "4"));
			AdapterImpl w = new AdapterImpl(sc);
			w.addlist(key, rdd);
			JavaRDD<String> list = w.fromDynomiteList(key);
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
	@Test
	public void testDataFrame() throws Exception {
		final String key = UUID.randomUUID().toString();
	    SQLContext sqlCtx = new SQLContext(sc);
	    DataFrame dataframe = sqlCtx.jsonFile("src/test/resources/dataframe.json");
	    AdapterImpl w = new AdapterImpl(sc);
	    w.toDynomiteDataFrame(key, dataframe);
	}
}
