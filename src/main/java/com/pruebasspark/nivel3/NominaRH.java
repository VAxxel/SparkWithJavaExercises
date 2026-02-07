package com.pruebasspark.nivel3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class NominaRH {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        SparkSession spark = SparkSession.builder()
                .appName("NominaRH")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dfOriginal = spark.read()
                .option("header",true)
                .option("inferSchema",true)
                .csv("insumos/nomina.csv");

        System.out.println("--- Dataset original ---");
        dfOriginal.show();
        dfOriginal.createOrReplaceTempView("empleados");

        //Empleados con >=5 años de antiguedad
        System.out.println("--- Empleados con mas de 5 años ---");
        Dataset<Row> emplAntiguos = spark.sql("SELECT * FROM empleados WHERE antiguedad >=5");
        emplAntiguos.show();

        System.out.println("--- bono ---");
        Dataset<Row> bono = spark.sql("SELECT nombre, salario, (salario * .20) as bono_navidad FROM empleados");
        bono.show();

        System.out.println("--- Sueldo por departamento ---");
        Dataset<Row> sueldoDepto = spark.sql("SELECT departamento, SUM(salario) as salario_departamento FROM empleados GROUP BY departamento ORDER BY salario_departamento DESC");
        sueldoDepto.show();

        sueldoDepto.write()
                .mode("overwrite")
                .csv("salidas/Sueldos_departamento");
    }
}
