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

import static org.apache.spark.sql.functions.col;

public class RetoInventario {
    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        SparkSession spark = SparkSession.builder()
                .appName("RetoInventario")
                .master("local[*]")
                .getOrCreate();

        //Lista del inventario que vamos a manejar
        List<Row> inventario = List.of(
                RowFactory.create("Laptop Gamer", "Computacion", 1500.0, 5),
                RowFactory.create("Mouse RGB", "Perifericos", 25D, 100),
                RowFactory.create("Monitor 4k", "Perifericos", 300D, 8),
                RowFactory.create("Teclado Mecanico", "Perifericos", 80D, 15),
                RowFactory.create("Tarjeta Grafica", "Computacion", 800.0, 2),
                RowFactory.create("Cable HDMI", "Cables", 10.0, 50)
        );

        //Estructura que sigue la tabla de arriba
        StructType esquema = new StructType(new StructField[]{
                new StructField("Articulo", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Departamento", DataTypes.StringType, false, Metadata.empty()),
                new StructField("Precio",DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("Stock", DataTypes.IntegerType, false, Metadata.empty())
        });

        //Dataset original con inventario y estruuctura
        Dataset<Row> dfOriginal = spark.createDataFrame(inventario, esquema);
        System.out.println("---Listado del Dataset original---");
        dfOriginal.show();

        //Se suele usar df y no dset porque en python es dataframe
        //Dataset con articulos con precio de mas de 500
        Dataset<Row> dfPremium = dfOriginal.filter(col("Precio").geq(500));
        System.out.println("---Listado de articulos premium---");
        dfPremium.show();

        //Dataset de stock
        //Dataset de stock del departamento de computacion < 10
        Dataset<Row> dfStock = dfOriginal.filter(col("Departamento")
                .equalTo("Computacion").and(col("Stock").lt(10)));

        System.out.println("---Estock de computacion <10 ---");
        dfStock.show();

        //Dataset ordenado por precio
        System.out.println("---Dataset ordenado por precio---");
        Dataset<Row> dfPrice = dfOriginal.orderBy(col("Precio").desc());
        dfPrice.show();
    }
}