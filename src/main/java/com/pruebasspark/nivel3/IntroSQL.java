package com.pruebasspark.nivel3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IntroSQL {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        SparkSession spark = SparkSession.builder()
                .appName("IntroSQL")
                .master("local[*]")
                .getOrCreate();

            Dataset<Row> dfOriginal = spark.read()
                    .option("header",true)
                    .option("inferSchema", true)
                    .csv("insumos/estudiantes.csv");

            dfOriginal.createOrReplaceTempView("notas");

        //Consultamos todos los estudiantes
        System.out.println("--- Todos los estudiantes ---");
        Dataset<Row> resultados = spark.sql("SELECT * FROM notas");
        resultados.show();

        //Ejercicio 2 filtramos donde estudiantes tengan >80 calificacion
        System.out.println("--- Estudiantes con mas de 80 ---");
        Dataset<Row> calificacionesMas80 = spark.sql("SELECT * FROM notas WHERE calificacion > 80");
        calificacionesMas80.show();

        System.out.println("--- Top 3 Mejores Calificaciones ---");
        // ORDER BY calificacion DESC = Ordena de mayor a menor
        // LIMIT 3 = Solo dame los primeros 3
        Dataset<Row> top3 = spark.sql("SELECT * FROM notas ORDER BY calificacion DESC LIMIT 3");
        top3.show();


        //Ejercicio 3: Agrupamos promedio por materia
        //Poder real de sql(resumir datos)
        System.out.println("--- Promedio por materia ---");
        Dataset<Row> promedioMateria = spark.sql("SELECT materia, AVG(calificacion) as promedio FROM notas GROUP BY materia");
        promedioMateria.show();

        System.out.println("--- Promedio Redondeado (2 decimales) ---");
        Dataset<Row> promedioLimpio = spark.sql("SELECT materia, ROUND(AVG(calificacion), 2) as promedio FROM notas GROUP BY materia");
        promedioLimpio.show();


    }
}
