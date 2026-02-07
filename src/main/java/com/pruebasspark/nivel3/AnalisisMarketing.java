package com.pruebasspark.nivel3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AnalisisMarketing {
    public static void main(String[] args) {

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        SparkSession spark = SparkSession.builder()
                .appName("AnalisisMarketing")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dfOriginal = spark.read()
                .option("header",true)
                .option("inferSchema",true)
                .csv("insumos/keywords.csv");

        dfOriginal.show();

        //Creacion de la vista temporal de bd
        dfOriginal.createOrReplaceTempView("datos_seo");

        //Consulta con mas vistas
        System.out.println("--- Consulta del top trafico ---");
        Dataset<Row> topVistas = spark.sql("SELECT * FROM datos_seo WHERE clicks > 400");
        topVistas.show();

        //Ranking por pais
        System.out.println("--- Ranking por pais ---");
        Dataset<Row> topPais = spark.sql("SELECT pais, SUM(clicks) as click_por_pais FROM datos_seo GROUP BY pais ORDER BY click_por_pais DESC");
        topPais.show();

        //Keyword por plataforma
        System.out.println("--- Keywords por plataforma ---");
        Dataset<Row> keyWords = spark.sql("SELECT plataforma, COUNT(keyword) as conteo FROM datos_Seo GROUP BY plataforma");
        keyWords.show();
    }
}
