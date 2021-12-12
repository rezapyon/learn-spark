package com.github.rezapyon.learn_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitors {
    public static void main(String[] args)
    {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Gym Competitors").master("local[*]").getOrCreate();

        Dataset<Row> csvDataset = spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/GymCompetition.csv");

//        csvDataset.show();

        StringIndexer genderIndexer = new StringIndexer();
        genderIndexer.setInputCol("Gender");
        genderIndexer.setOutputCol("GenderIndex");
        csvDataset = genderIndexer.fit(csvDataset).transform(csvDataset);

        OneHotEncoder genderEncoder = new OneHotEncoder();
        genderEncoder.setInputCols(new String[] {"GenderIndex"});
        genderEncoder.setOutputCols(new String[] {"GenderVector"});
        csvDataset = genderEncoder.fit(csvDataset).transform(csvDataset);
//        csvDataset.show();

        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[] {"Age", "Height", "Weight", "GenderVector"});
        vectorAssembler.setOutputCol("features");
        Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(csvDataset);

        Dataset<Row>  modelInputDate = csvDataWithFeatures.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
//        modelInputDate.show();

        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model =  linearRegression.fit(modelInputDate);
        System.out.println("The model has intercept " + model.intercept() + " and coefficients " + model.coefficients());

        // don't do this on production!!
        model.transform(modelInputDate).show();
    }
}
