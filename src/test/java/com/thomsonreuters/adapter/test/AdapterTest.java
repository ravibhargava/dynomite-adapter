package com.thomsonreuters.adapter.test;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import scala.Tuple2;
import scala.collection.parallel.ParIterableLike.FlatMap;

import com.netflix.config.ConfigurationManager;
import com.palantir.docker.compose.DockerComposition;
import com.thomsonreuters.adapter.impl.AdapterImpl;


@Category(IntegrationTest.class)
public class AdapterTest implements Serializable{
	private static JavaSparkContext sc = new JavaSparkContext("local", "Test", System.getenv("SPARK_HOME"), System.getenv("JARS"));
	private static SQLContext sqlContext;
	@ClassRule
	public static DockerComposition composition = DockerComposition.of("src/test/resources/docker-compose-dynomite.yml")
    .build();
	@BeforeClass 
	public static void startup() {
		try {
			sqlContext = new SQLContext(sc);
			ConfigurationManager.loadPropertiesFromResources("junit.properties");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Test
	public void testList() {
		try {
			final String key = UUID.randomUUID().toString();
			JavaRDD<String> rdd = sc.parallelize(Arrays.asList("1", "2", "3", "4"));
			AdapterImpl w = new AdapterImpl(sc);
			w.addlist(key, rdd);
			JavaRDD<String> list = w.fromDynomiteList(key);
			List<String> strings = list.collect();
			for (String string:strings)
			System.out.println("**************"+string);
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
	@Test
	public void testDataFrame() throws Exception {
		final String key = UUID.randomUUID().toString();
	    DataFrame dataframe = sqlContext.jsonFile("src/test/resources/dataframe.json");
	    AdapterImpl w = new AdapterImpl(sc);
	    w.toDynomiteDataFrame(dataframe, key);
	    DataFrame df = w.fromDynomiteDataFrame(key); 
	}
	public class AllTypes {

		private boolean bool;
		public void setBool(boolean bool) {
			this.bool = bool;
		}
		public void setB(byte b) {
			this.b = b;
		}
		public void setD(Date d) {
			this.d = d;
		}
		public void setS(String s) {
			this.s = s;
		}
		public void setDbl(double dbl) {
			this.dbl = dbl;
		}
		public void setF(float f) {
			this.f = f;
		}
		public void setI(int i) {
			this.i = i;
		}
		public void setLn(long ln) {
			this.ln = ln;
		}
		public void setSh(short sh) {
			this.sh = sh;
		}
		public void setTs(Timestamp ts) {
			this.ts = ts;
		}
		private byte b;
		
		public boolean isBool() {
			return bool;
		}
		public byte getB() {
			return b;
		}
		public Date getD() {
			return d;
		}
		public String getS() {
			return s;
		}
		public double getDbl() {
			return dbl;
		}
		public float getF() {
			return f;
		}
		public int getI() {
			return i;
		}
		public long getLn() {
			return ln;
		}
		public short getSh() {
			return sh;
		}
		public Timestamp getTs() {
			return ts;
		}
		private Date d;
		private String s;
		private double dbl;
		private float f;
		private int i;
		private long ln;
		private short sh;
		private Timestamp ts;

	}
	@Test
	public void testDataFrame3() throws Exception {
		StructType schema = DataTypes.createStructType(new StructField[] {
				DataTypes.createStructField("bool", DataTypes.BooleanType, false),
				DataTypes.createStructField("byte", DataTypes.ByteType, false),
				DataTypes.createStructField("date", DataTypes.DateType, true),
				DataTypes.createStructField("string", DataTypes.StringType, true),
				DataTypes.createStructField("double", DataTypes.DoubleType, true),
				DataTypes.createStructField("float", DataTypes.FloatType, true),
				DataTypes.createStructField("int", DataTypes.IntegerType, true),
				DataTypes.createStructField("long", DataTypes.LongType, true),
				DataTypes.createStructField("short", DataTypes.ShortType, true),
				DataTypes.createStructField("timestamp", DataTypes.TimestampType, true)
				});
		final String key = UUID.randomUUID().toString();
		JavaRDD<String> sample = sc.textFile("src/test/resources/alltypes.csv");
		JavaRDD<Row> peopleRDD = sample.map(new Function<String, Row>() {
				    @Override
				    public Row call(String line) throws Exception {
				      AllTypes types = new AllTypes();
				      types.setBool(true);
				      types.setB((byte)0x11);
				      types.setD(new Date(0L));
				      types.setS("foo");
				      types.setDbl(0.22222);
				      types.setF(12);
				      types.setI(100);
				      types.setLn(200L);
				      types.setSh((short)3);
				      types.setTs(new Timestamp(0L));
				      return RowFactory.create(types.isBool(), types.getB(), types.getD(), types.getS(),
				    		  types.getDbl(),types.getF(), types.getI(), types.getLn(), types.getSh(),
				    		  types.getTs());
				    }
				  });
		DataFrame allTypesDF = sqlContext.createDataFrame(peopleRDD, schema);
	    AdapterImpl w = new AdapterImpl(sc);
	    w.toDynomiteDataFrame(allTypesDF, key);
	    DataFrame df = w.fromDynomiteDataFrame(key); 
	}

	@Test
	public void testDataFrame2() throws Exception {
		StructType schema = DataTypes.createStructType(new StructField[] {
				//DataTypes.createStructField("bool", DataTypes.BooleanType, false),
				DataTypes.createStructField("bool", DataTypes.StringType, false),
				DataTypes.createStructField("byte", DataTypes.ByteType, false),
				DataTypes.createStructField("date", DataTypes.DateType, true),
				DataTypes.createStructField("string", DataTypes.StringType, true),
				DataTypes.createStructField("double", DataTypes.DoubleType, true),
				DataTypes.createStructField("float", DataTypes.FloatType, true),
				DataTypes.createStructField("int", DataTypes.IntegerType, true),
				DataTypes.createStructField("long", DataTypes.LongType, true),
				DataTypes.createStructField("short", DataTypes.ShortType, true),
				DataTypes.createStructField("timestamp", DataTypes.TimestampType, true)
				});
		final String key = UUID.randomUUID().toString();
		JavaRDD<String> sample = sc.textFile("src/test/resources/alltypes.csv");
		JavaRDD<Row> rowRDD = sample.map(
				  new Function<String, Row>() {
				    public Row call(String record) throws Exception {
				      String[] fields = record.split(",");
				      for (String field:fields)
				      System.out.println("field="+field);
				      return RowFactory.create(fields[0],fields[1],fields[2],fields[3],fields[4],fields[5],fields[6],fields[7],fields[8],fields[9]);
				    }
				  });
		DataFrame allTypesDF = sqlContext.createDataFrame(rowRDD, schema);
	    AdapterImpl w = new AdapterImpl(sc);
	    w.toDynomiteDataFrame(allTypesDF, key);
	    DataFrame df = w.fromDynomiteDataFrame(key); 
	}

	@Test
	public void testDynomiteKV() {
		String key = UUID.randomUUID().toString();
	    AdapterImpl w = new AdapterImpl(sc);
	    List<Tuple2<String,String>> listR = new ArrayList<Tuple2<String,String>>();
	    listR.add(new Tuple2<String,String>("a1", "a2"));
	    JavaPairRDD<String,String> rdd = sc.parallelizePairs(listR);
	    w.toDynomiteKV(rdd);
	    JavaPairRDD<String,String> result = w.fromDynomiteKV("a1");
	}
	
	
	public void testDynomiteHash(String hashname)  {
		String key = UUID.randomUUID().toString();
	    AdapterImpl w = new AdapterImpl(sc);
	    List<Tuple2<String,String>> listR = new ArrayList<Tuple2<String,String>>();
	    listR.add(new Tuple2<String,String>("a1", "a2"));
	    JavaPairRDD<String,String> rdd = sc.parallelizePairs(listR);
	    w.toDynomiteHASH(rdd, hashname);
	    JavaPairRDD<String, Map<String,String>> result = w.fromDynomiteHash(hashname);
	}
	public JavaRDD<String> testfromDynomiteList(final String key) {return null;}
	public JavaRDD<String> testfromDynomiteSet(String key) {return null;}	
	public void testtoDynomiteLIST(JavaRDD<String> listRDD, final String listName) {}
	public void testtoDynomiteSET(JavaRDD<String> setRDD, final String setName) {}
	

}
