package com.mobiliya.final_demo;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Hello world!
 *
 */

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.netlib.util.doubleW;

import scala.Tuple2;

public class App {
	
	
	public static boolean isFirstTime=true;
	private static long TotalCount=0;

	public static void main(String[] args) throws InterruptedException 
	{
		System.setProperty("hadoop.home.dir", "C:/Users/Rameshwar/Documents/WinUtils");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Final_Demo");
		
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
		
		Collection<String> topics = Arrays.asList("test");
		
		Map<String, Object> params = new HashMap<>();
		params.put("bootstrap.servers", "192.168.1.7:9092");
		params.put("key.deserializer", StringDeserializer.class);
		params.put("value.deserializer", StringDeserializer.class);
		params.put("group.id", "spark-group");
		params.put("auto.offset.reset", "latest");
		params.put("enable.auto.commit", true);
		
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(), 
				                      ConsumerStrategies.Subscribe(topics, params));

		
		JavaDStream<String> msgDataStream = stream.map(item->item.value());
		
		 //Create JavaRDD<Row>
		
		
		
		
        msgDataStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
              public void call(JavaRDD<String> rdd) throws IOException { 
                  JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
                      @Override
                      public Row call(String msg) {
                        Row row = RowFactory.create(msg.split(","));
                        return row;
                      }
                    });
                  
                  
        //Create Schema    
                  
                  List<StructField> fields = new ArrayList<>();
                  fields.add(DataTypes.createStructField("amount", DataTypes.StringType, true));
                  fields.add(DataTypes.createStructField("oldbalanceOrg", DataTypes.StringType, true));
                  fields.add(DataTypes.createStructField("newbalanceOrig", DataTypes.StringType, true));
                  fields.add(DataTypes.createStructField("oldbalanceDest", DataTypes.StringType, true));
                  fields.add(DataTypes.createStructField("newbalanceDest", DataTypes.StringType, true));
                  fields.add(DataTypes.createStructField("isFraud", DataTypes.StringType, true));

                  StructType schema = DataTypes.createStructType(fields);
                          
        
        
        
        
        //Get Spark 2.0 session       
        
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				.appName("Final_Demo")
				.getOrCreate();
        
		
	    
        
        Dataset<Row> msgDataFrame = spark.createDataFrame(rowRDD, schema);
        List<Row> rows = msgDataFrame.collectAsList();
        Dataset<Row> df = spark.createDataFrame(rows, schema).toDF();        
       // df.show();
       // df.printSchema();
        
        
        Dataset<Row> df2 = df.withColumn("amount", df.col("amount").cast( DataTypes.DoubleType)).withColumnRenamed("amount", "amount")
        		.withColumn("oldbalanceOrg", df.col("oldbalanceOrg").cast( DataTypes.DoubleType)).withColumnRenamed("oldbalanceOrg", "oldbalanceOrg")
        		.withColumn("newbalanceOrig", df.col("newbalanceOrig").cast( DataTypes.DoubleType)).withColumnRenamed("newbalanceOrig", "newbalanceOrig")
        		.withColumn("oldbalanceDest", df.col("oldbalanceDest").cast( DataTypes.DoubleType)).withColumnRenamed("oldbalanceDest", "oldbalanceDest")
        		.withColumn("newbalanceDest", df.col("newbalanceDest").cast( DataTypes.DoubleType)).withColumnRenamed("newbalanceDest", "newbalanceDest")
        		.withColumn("isFraud", df.col("isFraud").cast( DataTypes.IntegerType)).withColumnRenamed("isFraud", "isFraud");


        
        

        
        
        df2.printSchema();
        df2.show();
        
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[] {"amount","oldbalanceOrg","newbalanceOrig","oldbalanceDest","newbalanceDest","isFraud"});
		vectorAssembler.setOutputCol("features");
		Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(df2);
		
		Dataset<Row> modelInputData = csvDataWithFeatures.select("isFraud","features").withColumnRenamed("isFraud", "label");
		modelInputData.show();
		modelInputData.printSchema();
		
		
		if(modelInputData.count()!=0) {
			
		
			TotalCount+=modelInputData.count();
			System.out.println("No of Data Records in batch :    \t"+modelInputData.count());
			System.out.println("Total Records Processed  :    \t"+TotalCount);

		
		if(isFirstTime) {
			
			KMeans kmeans = new KMeans().setK(2).setSeed(1L);
			KMeansModel model = kmeans.fit(modelInputData);
			model.write().overwrite().save("my_model");
			Dataset<Row> predictions = model.transform(modelInputData);
	
			// Evaluate clustering by computing Silhouette score
			ClusteringEvaluator evaluator = new ClusteringEvaluator();
	
			double silhouette = evaluator.evaluate(predictions);
			System.out.println("Silhouette with squared euclidean distance = " + silhouette);
	
			// Shows the result.
			Vector[] centers = model.clusterCenters();
			System.out.println("Cluster Centers: ");
			for (Vector center: centers) {
			  System.out.println(center);
			}
			
			model.transform(modelInputData).show();
			isFirstTime=false;
		}
		else {
			
			
			KMeans kmeans = new KMeans().setK(2).setSeed(1L);
			KMeansModel model = KMeansModel.load("my_model");
			model=kmeans.fit(modelInputData);
			
			Dataset<Row> predictions = model.transform(modelInputData);
	
			// Evaluate clustering by computing Silhouette score
			ClusteringEvaluator evaluator = new ClusteringEvaluator();
	
			double silhouette = evaluator.evaluate(predictions);
			System.out.println("Silhouette with squared euclidean distance = " + silhouette);
	
			// Shows the result.
			Vector[] centers = model.clusterCenters();
			System.out.println("Cluster Centers: ");
			for (Vector center: centers) {
			  System.out.println(center);
			}
			model.transform(modelInputData).show();
			
		}
		

		}
		else {
			System.out.println("No Data , Collect from Sadhu.......");
		}
		
       
              }
        });
		
		sc.start();
		sc.awaitTermination();
	}

}
