package com.pruebasspark.level2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;


public class ExampleNivel2 {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        SparkSession spark = SparkSession.builder()
                .appName("EjemploNivel2")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dfOriginal = spark.read()
                .option("header", true)//Le decimos que use la primera fila como nombres de columnas
                .option("inferSchema", true)//Infiere si es double, String, int, etc.
                .csv("insumos/ventas.csv");//La ruta donde se encuentra el archivo

        dfOriginal.show();

        //Eliminar columnas donde una columna especifica sea nula
        Dataset<Row> dfLimpio = dfOriginal.na().drop(new String[]{"fecha"});

        //Rellenar nulos con un valor(ej. 0) en columnas especificas
        dfLimpio = dfLimpio.na().fill(0,new String[]{"cantidad"});
        dfLimpio.show();

        //Creamos la columna "Total" multiplicando cantidad por precio
        dfLimpio = dfLimpio.withColumn("Total",col("cantidad").multiply(col("precio_unitario")));

        System.out.println("--- Tabla con fechas sin null y cantidad 0 ---");
        dfLimpio.show();

        //Salida de CSV com caracteristicas pedidas
        System.out.println("--- Salida del CSV ---");
        dfLimpio.write()
                .mode("overwrite")//El mode Overwrite sobrescribe la carpeta si ya existe, es util para las pruebas
                .option("header",true)//Esta opcion nos da los encabezados de la salida del csv
                .csv("salidas/SalidaVentasCsv");

        System.out.println("--- Salida de Json ---");
        dfLimpio.write()
                .mode("overwrite")//El mode Overwrite sobrescribe la carpeta si ya existe, es util para las pruebas
                .json("salidas/SalidaVentasJson");

        System.out.println("--- Salida de Parquet ---");
        dfLimpio.write()
                .mode("overwrite")//El mode Overwrite sobrescribe la carpeta si ya existe, es util para las pruebas
                .parquet("salidas/SalidaVentasParquet");
    }
}