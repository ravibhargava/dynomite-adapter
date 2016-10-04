package com.thomsonreuters.adapter.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import scala.Function1;
import scala.Tuple2;
import scala.collection.parallel.ParIterableLike.FlatMap;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
//	@Test
//	public void testList() {
//		try {
//			final String key = UUID.randomUUID().toString();
//			JavaRDD<String> rdd = sc.parallelize(Arrays.asList("1", "2", "3", "4"));
//			AdapterImpl w = new AdapterImpl(sc);
//			w.addlist(key, rdd);
//			JavaRDD<String> list = w.fromDynomiteList(key);
//			List<String> strings = list.collect();
//			for (String string:strings)
//			System.out.println("**************"+string);
//		}
//		catch (Exception e){
//			e.printStackTrace();
//		}
//	}
	@Test
	public void testDataFrame() throws Exception {
		final String key = UUID.randomUUID().toString();
	    DataFrame dataframe = sqlContext.jsonFile("src/test/resources/dataframe.json");
	    AdapterImpl w = new AdapterImpl(sc);
	    w.toDynomiteDataFrame(dataframe, key);
	    DataFrame df = w.fromDynomiteDataFrame(key); 
	    scala.collection.Iterator<Row> iter = df.rdd().toLocalIterator();
	    while (iter.hasNext()) {
	    	Row r = iter.next();
	    	for (int i=0; i< r.size(); i++) {
	    		System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%"+r.get(i));
	    	}
	    }
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
				DataTypes.createStructField("bool1", DataTypes.BooleanType, false),
				DataTypes.createStructField("byte1", DataTypes.ByteType, false),
				DataTypes.createStructField("date1", DataTypes.DateType, true),
				DataTypes.createStructField("string1", DataTypes.StringType, true),
				DataTypes.createStructField("double1", DataTypes.DoubleType, true),
				DataTypes.createStructField("float1", DataTypes.FloatType, true),
				DataTypes.createStructField("int1", DataTypes.IntegerType, true),
				DataTypes.createStructField("long1", DataTypes.LongType, true),
				DataTypes.createStructField("short1", DataTypes.ShortType, true),
				DataTypes.createStructField("timestamp1", DataTypes.TimestampType, true)
				});
		final String key = UUID.randomUUID().toString();
		JavaRDD<String> sample = sc.parallelize(Arrays.asList(""));
		JavaRDD<Row> alltypes = sample.map(new Function<String, Row>() {
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
		DataFrame allTypesDF = sqlContext.createDataFrame(alltypes, schema);
	    AdapterImpl w = new AdapterImpl(sc);
	    w.toDynomiteDataFrame(allTypesDF, key);
	    DataFrame df = w.fromDynomiteDataFrame(key); 
	}



	@Test
	public void testDynomiteKV() {
		final String key = "Heart";
		AdapterImpl w = new AdapterImpl(sc);
		List<Tuple2<String, String>> listR = Arrays.asList(
                new Tuple2<String, String>("Heart", "1"),
                new Tuple2<String, String>("Diamonds", "2"));
		JavaPairRDD<String,String> rdd = sc.parallelizePairs(listR);
		w.toDynomiteKV(rdd);
		JavaPairRDD<String,String> result = w.fromDynomiteKV(key);
		result.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>(){
			public void call(Iterator<Tuple2<String, String>> iterator) throws Exception {
				while (iterator.hasNext()){
					Tuple2<String, String> s = iterator.next();
					Assert.assertTrue(s._1.equals(key));
					Assert.assertTrue(s._2.equals("1"));
				}
			}});	    	
	}

	@Test
	public void testDynomiteHash()  {
		final String hashname = "hashname";
		String key = UUID.randomUUID().toString();
	    AdapterImpl w = new AdapterImpl(sc);
	    List<Tuple2<String,String>> listR = new ArrayList<Tuple2<String,String>>();
	    listR.add(new Tuple2<String,String>("a1", "a2"));
	    JavaPairRDD<String,String> rdd = sc.parallelizePairs(listR);
	    w.toDynomiteHASH(rdd, hashname);
	    JavaPairRDD<String, Map<String,String>> result = w.fromDynomiteHash(hashname);
		result.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Map<String,String>>>>(){
			public void call(Iterator<Tuple2<String, Map<String,String>>> iterator) throws Exception {
				while (iterator.hasNext()){
					Tuple2<String, Map<String,String>> s = iterator.next();
					Assert.assertTrue(s._1.equals(hashname));
					Map<String,String> map = s._2;
					Assert.assertTrue(map.get("a1").equals("a2"));
				}
			}});	
	}
	
	String[] lists = {"The dark side of the moon", "Yellow brick road", "The wall"};
	
	@Test
	public void testDynomiteList() {
		String key = UUID.randomUUID().toString();
	    AdapterImpl w = new AdapterImpl(sc);
	    JavaRDD<String> listRDD = sc.textFile("src/test/resources/list.txt");
	    w.toDynomiteLIST(listRDD, key);
	    JavaRDD<String> result = w.fromDynomiteList(key);
	    JavaRDD<String> list = w.fromDynomiteList(key);
		List<String> strings = list.collect();
		for (String string:strings) {
			if (!(string.trim().equals(lists[0].trim()) || string.trim().equals(lists[1].trim()) || string.trim().equals(lists[2].trim()))){
				fail();
			}
		}	
	}
	//@Test iterator not impemented in Dynomite 
	public void testDynomiteSet() {
		String key = UUID.randomUUID().toString();
	    AdapterImpl w = new AdapterImpl(sc);
	    JavaRDD<String> listRDD = sc.textFile("src/test/resources/set.txt");
	    w.toDynomiteSET(listRDD, key);
	    JavaRDD<String> result = w.fromDynomiteSet(key);
		List<String> strings = result.collect();
		for (String string:strings) {
			System.out.println(string);
		}	
	}
	

	@Test
	public void testUserDefinedType() throws Exception {
		//First create the address
		List<StructField> addressFields = new ArrayList<StructField>();
		addressFields.add(DataTypes.createStructField("street", DataTypes.StringType, true));
		addressFields.add(DataTypes.createStructField("zip", DataTypes.StringType, true));
		StructType addressStruct = DataTypes.createStructType(addressFields);

		//Then create the person, using the address struct
		List<StructField> personFields = new ArrayList<StructField>();
		personFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		personFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		personFields.add(DataTypes.createStructField("address", addressStruct, true));

		StructType schema = DataTypes.createStructType(personFields);
		JavaRDD<String> sample = sc.parallelize(Arrays.asList(""));
		final String key = UUID.randomUUID().toString();
		JavaRDD<Row> person = sample.map(new Function<String, Row>() {
				    @Override
				    public Row call(String line) throws Exception {
				      Person p = new Person();
				      p.setName("foo");
				      p.setAge(10);
				      Address address = new Address();
				      address.setAddress("123 Main St");
				      address.setZip("95129");
				      p.setAddress(address);
				      return RowFactory.create(p.getName(), p.getAge(), p.getAddress());
				    }
				  });
		DataFrame personDF = sqlContext.createDataFrame(person, schema);
	    AdapterImpl w = new AdapterImpl(sc);
	    w.toDynomiteDataFrame(personDF, key);
	    DataFrame df = w.fromDynomiteDataFrame(key); 

	}



	@Test
	public void testDataFrame2() throws Exception {
		StructType schema = DataTypes.createStructType(new StructField[] {
				DataTypes.createStructField("bool", DataTypes.BooleanType, false),
				DataTypes.createStructField("byte", DataTypes.ByteType, false),
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("string", DataTypes.StringType, true),
				DataTypes.createStructField("double", DataTypes.DoubleType, true),
				DataTypes.createStructField("float", DataTypes.FloatType, true),
				DataTypes.createStructField("int", DataTypes.IntegerType, true),
				DataTypes.createStructField("long", DataTypes.LongType, true),
				DataTypes.createStructField("short", DataTypes.ShortType, true),
				DataTypes.createStructField("timestamp", DataTypes.StringType, true)
				});
		final String key = UUID.randomUUID().toString();
		JavaRDD<String> sample = sc.textFile("src/test/resources/alltypes.csv");
		JavaRDD<Row> rowRDD = sample.map(
				  new Function<String, Row>() {
				    public Row call(String record) throws Exception {
				      String[] fields = record.split(",");
				      for (String field:fields)
				      System.out.println("field="+field);
				      return RowFactory.create(new Boolean(fields[0]),new Byte(fields[1]),fields[2],fields[3],new Double(fields[4]),new Float(fields[5]),
				    		  new Integer(fields[6]), new Long(fields[7]), new Short(fields[8]), fields[9]);
				    }
				  });
		DataFrame allTypesDF = sqlContext.createDataFrame(rowRDD, schema);
	    AdapterImpl w = new AdapterImpl(sc);
	    w.toDynomiteDataFrame(allTypesDF, key);
	    DataFrame df = w.fromDynomiteDataFrame(key); 
	}

}