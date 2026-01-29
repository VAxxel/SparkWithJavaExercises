package com.pruebasspark.level2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;

public class CredencialesEmpleados {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        SparkSession spark = SparkSession.builder()
                .appName("CredencialesEmpleados")
                .master("local[*]")
                .getOrCreate();


       Dataset<Row> dforiginal = spark.read()
               .option("header", true)
               .option("inferSchema", true)
               .csv("insumos/empleados.csv");

        System.out.println("--- Dataset original ---");
        dforiginal.show();

        System.out.println("--- Dataset con apellidos ---");
        Dataset<Row> dfFinal = dforiginal.na().fill("Desconocido", new String[]{"apellido"});
        dfFinal.show();

        System.out.println("---Creacion de columna nombre completo---");
        dfFinal = dfFinal.withColumn("nombre_completo",concat_ws(" ", col("nombre"),col("apellido")));
        dfFinal.show();

        System.out.println("---Tabla que solo tiene 3 columnas ---");
        dfFinal = dfFinal.select(col("id"),col("nombre_completo"),col("salario"));
        dfFinal.show();

        System.out.println("--- Salida del csv ---");
        dfFinal.write()
                .mode("overwrite")
                .option("header",true)//Necesitamos esta opcion para que tenga encabezados la salida
                .csv("salidas/credencialesEmpleadosCsv");
    }
}
