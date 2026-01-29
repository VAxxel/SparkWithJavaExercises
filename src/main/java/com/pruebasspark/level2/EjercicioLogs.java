package com.pruebasspark.level2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class EjercicioLogs {
    public static void main(String[] args) {
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel","error");

        SparkSession spark = SparkSession.builder()
                .appName("EjercicioLogs")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dfOriginal = spark.read()
                .json("insumos/servidor_logs.json");

        dfOriginal.show();

        //Creacion del dfLimpio (con columnas del timestamp con nulos eliminada)
        Dataset<Row> dfLimpio = dfOriginal.na().drop(new String[]{"timestamp"});
        System.out.println("---Impresion del Dataset sin valores nulos ---");
        dfLimpio.show();

        //Cambio de nombre de la columna de ip a direccion_ip
        dfLimpio = dfLimpio.withColumnRenamed("ip","direccion_ip");
        System.out.println("--- Impresion del dataset con cambio de nombre ---");
        dfLimpio.show();

        //Dataset con accion que no contenga error
        dfLimpio = dfLimpio.filter(col("accion").notEqual("ERROR"));
        System.out.println("--- Impresion de dataset sin error ---");
        dfLimpio.show();

        //Generacion de salidas
        System.out.println("--- Generacion de salidas en Parquet y Json ---");

        dfLimpio.write()
                .mode("overwrite")
                .parquet("salidas/salidasUsuariosParquet");

        dfLimpio.write()
                .mode("overwrite")
                .json("salidas/salidasUsuariosJson");
    }
}