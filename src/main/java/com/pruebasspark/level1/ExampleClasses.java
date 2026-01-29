package com.pruebasspark.level1;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static org.apache.spark.sql.functions.col;

public class ExampleClasses {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        //El objeto que coordina todo
        SparkSession spark = SparkSession.builder()
                .appName("PracticaEstudiantes")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        //List<Row> Una lista standar que contiene objetos Row de spark
        //Row es una fila generica (como array de objetos[String, String, Int])
        List<Row> datosEstudiantes = List.of(
                RowFactory.create("Vince", "Matematicas", 95),
                RowFactory.create("Vince", "Historia", 80),
                RowFactory.create("Ana", "Matematicas", 40),
                RowFactory.create("Ana","Historia", 50),
                RowFactory.create("Adrian", "Matematicas", 85),
                RowFactory.create("Norma","Historia",78)
        );

        //StrucType: El tipo de dato que define la estructura de la tabla (El esquema)
        StructType esquema = new StructType(new StructField[]{
           new StructField("Estudiante", DataTypes.StringType, false, Metadata.empty()),
           new StructField("Materia", DataTypes.StringType, false, Metadata.empty()),
           new StructField("Calificacion", DataTypes.IntegerType, false, Metadata.empty())
        });

        //Dataset<Row>: El corazon de Spark
        //Es una coleccion distribuida de filas. En python se llama DataFrame
        //En Java se llama Dataset<Row> (un Dataset de filas genericas)

        Dataset<Row> dfNotas = spark.createDataFrame(datosEstudiantes, esquema);

        System.out.println("--- Tabla Original ---");
        dfNotas.show();

        //TRANSFORMACIONS:

        //Filter devuelve otro nuevo Dataset<Row>
        System.out.println("Alumnos Aprobados: ");
        Dataset<Row> dfAprobados = dfNotas.filter(col("Calificacion").geq(60));
        dfAprobados.show();

        //Filter con condiciones complejas tambien devuelve Dataset<Row>
        //geq -> greater or equal
        System.out.println("--- Aprobados en Matematicas ---");
        Dataset<Row> dfAproMat = dfNotas.filter(col("Materia")
                .equalTo("Matematicas").and(col("Calificacion").geq(60)));
        dfAproMat.show();

        //OrderBy tambien devuelve un Dataset<Row>
        System.out.println("--- Cuadro de Honor(Ordenado) ---");
        Dataset<Row> dfOrdenado = dfNotas.orderBy(col("Calificacion").desc());
        dfOrdenado.show();
    }
}