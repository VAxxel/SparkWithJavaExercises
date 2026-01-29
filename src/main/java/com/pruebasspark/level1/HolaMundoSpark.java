package com.pruebasspark.level1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class HolaMundoSpark {
    public static void main(String[] args) {

        //Comenzamos iniciando la sesion(Driver)
        //.master("local[*]") significa que usa todos los nucleos de la cpu local
        System.out.println("--- Iniciando Spark Session ---");

        try (var spark = SparkSession.builder()
                .appName("HolaMundoSpark")
                .master("local[*]")
                .getOrCreate()){

            spark.sparkContext().setLogLevel("WARN");

            //Creamos datos de prueba simulando una tabla
            var listOfData = List.of(
                    RowFactory.create("Java",2000),
                    RowFactory.create("Python",2500),
                    RowFactory.create("Scala",5000)
            );

            //definimos el esquema de las columnas
            var esquemaColumns = new StructType(new StructField[]{
                    new StructField("Lenguaje", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("Users",DataTypes.IntegerType, false, Metadata.empty())
            });

            //Creamos el dataset(DataFrame)
            Dataset<Row> df = spark.createDataFrame(listOfData, esquemaColumns);

            //Transformamos: filtramos donde los usuarios sean mayor a 2000
            System.out.println("-- Filtro mayor a 2000 ---");
            var dfPopular = df.filter("Users > 2000");

            //Accion: mostrar resultados en la consola
            dfPopular.show();

            //Accion de imprimir el esquema para verificar los tipos
            dfPopular.printSchema();
        }
    }
}
