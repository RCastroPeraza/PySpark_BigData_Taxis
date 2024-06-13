# Bienvenido a la Actividad de Proyecto
Como ya se mencionó anteriormente en esta actividad estarás usando Apache Spark y su interface para Python PySpark para resolver ciertos ejercicios sobre los datos de Taxis de NYC.

Este archivo en el que vas a estar trabajando es conocido como un Notebook y está compuesto por celdas de código, en este caso en lenguaje Python, para ejecutar una celda puedes dar click sobre ella y luego presionar el botón ![Botón de play](https://i.ibb.co/9hvVpCK/Captura-de-pantalla-de-2021-07-14-14-18-01.png) o presionar Ctrl+Enter.

## 1. Crear una sesión en Spark
Esta sesión nos permite conectarnos al cluster Spark que desplegamos con anterioridad.


```python
from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()

spark.sparkContext.setLogLevel("OFF")
```

    24/06/12 13:27:20 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).


## 2. Leer los datos
En un paso anterior subiste 2 archivos a la carpeta __datos__, ahora vamos a leerlos y explorarlos antes de realizar los ejercicios.


```python
# Para leer los datos necesitas la ruta del archivo e indicar si tienen una cabecera o no, que en este caso si la tienen.
datos_2019 = spark.read.csv("../data/yellow_tripdata_2019-01.csv", header=True)
# Para visualizar los datos que leiste debes usar la función show e indicar el número de filas, en este caso 5.
datos_2019.show(n=5)
```

                                                                                    

    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
    |VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
    |       1| 2019-01-01 00:46:40|  2019-01-01 00:53:20|              1|         1.50|         1|                 N|         151|         239|           1|          7|  0.5|    0.5|      1.65|           0|                  0.3|        9.95|                null|
    |       1| 2019-01-01 00:59:47|  2019-01-01 01:18:59|              1|         2.60|         1|                 N|         239|         246|           1|         14|  0.5|    0.5|         1|           0|                  0.3|        16.3|                null|
    |       2| 2018-12-21 13:48:30|  2018-12-21 13:52:40|              3|          .00|         1|                 N|         236|         236|           1|        4.5|  0.5|    0.5|         0|           0|                  0.3|         5.8|                null|
    |       2| 2018-11-28 15:52:25|  2018-11-28 15:55:45|              5|          .00|         1|                 N|         193|         193|           2|        3.5|  0.5|    0.5|         0|           0|                  0.3|        7.55|                null|
    |       2| 2018-11-28 15:56:57|  2018-11-28 15:58:33|              5|          .00|         2|                 N|         193|         193|           2|         52|    0|    0.5|         0|           0|                  0.3|       55.55|                null|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
    only showing top 5 rows
    



```python
datos_2013 = spark.read.csv("../data/yellow_tripdata_2013-01.csv", header=True)
datos_2013.show(n=5)
```

    +---------+-------------------+-------------------+---------------+-------------------+-------------------+------------------+---------+------------------+-------------------+------------------+------------+-----------+---------+-------+----------+------------+------------+
    |vendor_id|    pickup_datetime|   dropoff_datetime|passenger_count|      trip_distance|   pickup_longitude|   pickup_latitude|rate_code|store_and_fwd_flag|  dropoff_longitude|  dropoff_latitude|payment_type|fare_amount|surcharge|mta_tax|tip_amount|tolls_amount|total_amount|
    +---------+-------------------+-------------------+---------------+-------------------+-------------------+------------------+---------+------------------+-------------------+------------------+------------+-----------+---------+-------+----------+------------+------------+
    |      CMT|2013-01-01 15:11:48|2013-01-01 15:18:10|              4|                  1|-73.978165000000004|40.757976999999997|        1|                 N|-73.989840000000001|40.751173000000001|         CSH|        6.5|        0|    0.5|         0|           0|           7|
    |      CMT|2013-01-06 00:18:35|2013-01-06 00:22:54|              1|                1.5|-74.006680000000003|40.731780999999998|        1|                 N|-73.994499000000005|40.750658999999999|         CSH|          6|      0.5|    0.5|         0|           0|           7|
    |      CMT|2013-01-05 18:49:41|2013-01-05 18:54:23|              1| 1.1000000000000001|         -74.004711|40.737769999999998|        1|                 N|-74.009831000000005|40.725999999999999|         CSH|        5.5|        1|    0.5|         0|           0|           7|
    |      CMT|2013-01-07 23:54:15|2013-01-07 23:58:20|              2|0.69999999999999996|-73.974599999999995|40.759945000000002|        1|                 N|-73.984736999999996|40.759388000000001|         CSH|          5|      0.5|    0.5|         0|           0|           6|
    |      CMT|2013-01-07 23:25:03|2013-01-07 23:34:24|              1| 2.1000000000000001|-73.976252000000002|         40.748528|        1|                 N|-74.002583000000001|40.747866999999999|         CSH|        9.5|      0.5|    0.5|         0|           0|        10.5|
    +---------+-------------------+-------------------+---------------+-------------------+-------------------+------------------+---------+------------------+-------------------+------------------+------------+-----------+---------+-------+----------+------------+------------+
    only showing top 5 rows
    


## Ejercicios
A continuación, vas a encontrar 2 ejercicios resueltos y explicados y uno para que completes de acuerdo a las instrucciones. **Todos los ejercicios deben resolverse siguiendo el patrón Map Reduce**.

### Ejercicio 1. Contar la cantidad total de viajes 
Explicación


```python
from operator import add # Requerido por la función reduceByKey

ruta_entrada = "../data/*"

# 1. Carga todos los archivos de datos en un RDD
archivos_entrada = spark.sparkContext.textFile(ruta_entrada)
```


```python
# 2. Lee los archivos línea por línea y aplica las siguientes operaciones:
#    1. Divide la línea por comas
#    2. Filtra las líneas de acuerdo a la cantidad de columnas, sólo conserva las que tengan más de 1. Además, evita que las líneas con los
#       los nombres de las columnas sean tenidas en cuenta
#    3. Por cada línea genera un pareja (llave, valor) donde la llave corresponde al tipo de servicio (yellow, fhv) y el valor a a ser 1
#       En este caso la llave la determina la cantidad de columnas de la línea, pues como observamos durante la exploración los registros
#       de FHV sólo tienen 6 columnas.
#    4. Ordena las parejas (llave, valor) por llave
#    5. Realiza la operación reduce por llave, aplicando la operación de suma (add), es decir, va a tomar todas las parejas (llave, valor)
#       con la misma llave y las va a sumar para así generar la cantidad total de viajes para cada tipo de servicio
resultado_map_reduce = archivos_entrada.map(lambda linea: linea.split(",")) \
                                .filter(lambda linea: len(linea)>1 and "PULocationID" not in linea) \
                                .map(lambda linea: ("yellow",1) if len(linea) > 6 else ("fhv", 1)) \
                                .sortByKey()\
                                .reduceByKey(add)
```

                                                                                    


```python
# 4. Imprime los resultados en consola.
# En este caso usamos take y pasamos un valor de 2 dado que solo se van a generar 2 parejas (llave, valor), una por cada tipo de servicio
print(resultado_map_reduce.take(2))
```

    [Stage 17:===========================================>            (39 + 2) / 50]

    [('yellow', 22446376), ('fhv', 612593)]


                                                                                    

### Ejercicio 2. Contar las cantidad total de viajes por zona de recogida
Explicación


```python
muestra = archivos_entrada.take(5)
for linea in muestra:
    print(linea)

```

    PAR1�d�L�	        
    ��=�$7�'֐��5e�$ ���tY{�C,^L凉jj+��$AH$�ۏ?���߄.(�,��]��� 3�P�mj��� w-�v�)�|M�v�=�j��F�S3bit�� ͘�d�d�y4�Bs���Ɠ�4w;ߦ�������<�\-wq�u]��.3.n��A�K(ߛ�Є�&�Sd8H�������X8��m��6�	�F�K����BtK*���t�����e9}:;X>�3�'�þ��=����C�ʐ`�PxT^0j|��mP�j���7
    ��f�l�U���S	n�ݦ��� Y:���Ƶ��('_�[JM[���m)5m�l
    �Է�w���n���-���_�4�� ɓg�g�M�`�.Vݡ}�F�������I"� �P�;+�4�t\��BWH���g[7���G�J#�V&O�ķU�?g��U"���&/'�*c
    �7ݟ�b���Qe�O4}�d ��s�?݆S"|


                                                                                    

Con esta información confirmamos que por alguna razón el texto se corrompe 
intentaré solucionarlo  mediante operaciones separadas


```python
datos_2019.show(n=5) 
```

    [Stage 20:>                                                         (0 + 1) / 1]

    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
    |VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
    |       1| 2019-01-01 00:46:40|  2019-01-01 00:53:20|              1|         1.50|         1|                 N|         151|         239|           1|          7|  0.5|    0.5|      1.65|           0|                  0.3|        9.95|                null|
    |       1| 2019-01-01 00:59:47|  2019-01-01 01:18:59|              1|         2.60|         1|                 N|         239|         246|           1|         14|  0.5|    0.5|         1|           0|                  0.3|        16.3|                null|
    |       2| 2018-12-21 13:48:30|  2018-12-21 13:52:40|              3|          .00|         1|                 N|         236|         236|           1|        4.5|  0.5|    0.5|         0|           0|                  0.3|         5.8|                null|
    |       2| 2018-11-28 15:52:25|  2018-11-28 15:55:45|              5|          .00|         1|                 N|         193|         193|           2|        3.5|  0.5|    0.5|         0|           0|                  0.3|        7.55|                null|
    |       2| 2018-11-28 15:56:57|  2018-11-28 15:58:33|              5|          .00|         2|                 N|         193|         193|           2|         52|    0|    0.5|         0|           0|                  0.3|       55.55|                null|
    +--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+
    only showing top 5 rows
    


                                                                                    


```python
datos_2013.show(n=5)
```

                                                                                    

    +---------+-------------------+-------------------+---------------+-------------------+-------------------+------------------+---------+------------------+-------------------+------------------+------------+-----------+---------+-------+----------+------------+------------+
    |vendor_id|    pickup_datetime|   dropoff_datetime|passenger_count|      trip_distance|   pickup_longitude|   pickup_latitude|rate_code|store_and_fwd_flag|  dropoff_longitude|  dropoff_latitude|payment_type|fare_amount|surcharge|mta_tax|tip_amount|tolls_amount|total_amount|
    +---------+-------------------+-------------------+---------------+-------------------+-------------------+------------------+---------+------------------+-------------------+------------------+------------+-----------+---------+-------+----------+------------+------------+
    |      CMT|2013-01-01 15:11:48|2013-01-01 15:18:10|              4|                  1|-73.978165000000004|40.757976999999997|        1|                 N|-73.989840000000001|40.751173000000001|         CSH|        6.5|        0|    0.5|         0|           0|           7|
    |      CMT|2013-01-06 00:18:35|2013-01-06 00:22:54|              1|                1.5|-74.006680000000003|40.731780999999998|        1|                 N|-73.994499000000005|40.750658999999999|         CSH|          6|      0.5|    0.5|         0|           0|           7|
    |      CMT|2013-01-05 18:49:41|2013-01-05 18:54:23|              1| 1.1000000000000001|         -74.004711|40.737769999999998|        1|                 N|-74.009831000000005|40.725999999999999|         CSH|        5.5|        1|    0.5|         0|           0|           7|
    |      CMT|2013-01-07 23:54:15|2013-01-07 23:58:20|              2|0.69999999999999996|-73.974599999999995|40.759945000000002|        1|                 N|-73.984736999999996|40.759388000000001|         CSH|          5|      0.5|    0.5|         0|           0|           6|
    |      CMT|2013-01-07 23:25:03|2013-01-07 23:34:24|              1| 2.1000000000000001|-73.976252000000002|         40.748528|        1|                 N|-74.002583000000001|40.747866999999999|         CSH|        9.5|      0.5|    0.5|         0|           0|        10.5|
    +---------+-------------------+-------------------+---------------+-------------------+-------------------+------------------+---------+------------------+-------------------+------------------+------------+-----------+---------+-------+----------+------------+------------+
    only showing top 5 rows
    


Parece que el problema surge de la unión de los dataframes.


```python
muestra_entrada = archivos_entrada.take(10)
for linea in muestra_entrada:
    print(linea)
```

    PAR1�d�L�	        
    ��=�$7�'֐��5e�$ ���tY{�C,^L凉jj+��$AH$�ۏ?���߄.(�,��]��� 3�P�mj��� w-�v�)�|M�v�=�j��F�S3bit�� ͘�d�d�y4�Bs���Ɠ�4w;ߦ�������<�\-wq�u]��.3.n��A�K(ߛ�Є�&�Sd8H�������X8��m��6�	�F�K����BtK*���t�����e9}:;X>�3�'�þ��=����C�ʐ`�PxT^0j|��mP�j���7
    ��f�l�U���S	n�ݦ��� Y:���Ƶ��('_�[JM[���m)5m�l
    �Է�w���n���-���_�4�� ɓg�g�M�`�.Vݡ}�F�������I"� �P�;+�4�t\��BWH���g[7���G�J#�V&O�ķU�?g��U"���&/'�*c
    �7ݟ�b���Qe�O4}�d ��s�?݆S"|
    �L�bT%�) ��2�^H߶�BVm����\,���qD�~�ob�B���9�k�6A������F:��*/{\����tw3�-P�r�V��\�]��W��.���{Y�TH�<��$�r�ME������1*��#2�[|��o37��\I�._a�� �P7��\|�+]0���r)�:F�TQYE�CL��w��k��
    t�҄}��$��@�4C~�C�pk��ʃ��[�Lj���C�8�M�2`w�.c�5sA��o���~)h*����1r qi���Px�����Ld�B��
    5��h7t���Bsa��`S���]Fd
    =u�*;�#X.��bˠ ��nn�@�}14�ɗ��nk�X�z�9?}�������|�;e]�(sbա �*�4z((�~���e��e�[ˏn����}��#��}~*m�֑�a�UZ���G9��I��E�9dND&�))�y�E����(֕D�N��ª(
    �,�(E�v^E��]Mܣe����9�)ԕ�S�뮸�T}�;_���Z��a����k���Z�U��Mrg��81�Q�R��4 �_%g�����J>�&ZA.*|Z#"�zL������n�˄ǡ�=�p�W��^�i��č����C��b���U*=M�I�;y~���j�Z.FW}e&�����QL�D4�Řm�c�>�U��(x�)G��>{yg�Ǎ!o/�H�T���ã�	-�4�f��,��r4`�zI�xGϦS��ڄ��~���f�Po�@�fw�tև4��


En efecto, está roto
Voy a intentar armar el RDD unicamente con los datos de 2 de ellos en vez de los 4 archivos presentes, unicamente tomando los csv


```python
from operator import add # Requerido por la función reduceByKey

# Rutas específicas de los archivos deseados
rutas_entrada = [
    "../data/yellow_tripdata_2019-01.csv",
    "../data/yellow_tripdata_2013-01.csv"
]

# Cargar los archivos en un RDD
archivos_entrada = spark.sparkContext.textFile(",".join(rutas_entrada))
```


```python
muestra_entrada = archivos_entrada.take(10)
for linea in muestra_entrada:
    print(linea)
```

    VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge
    1,2019-01-01 00:46:40,2019-01-01 00:53:20,1,1.50,1,N,151,239,1,7,0.5,0.5,1.65,0,0.3,9.95,
    1,2019-01-01 00:59:47,2019-01-01 01:18:59,1,2.60,1,N,239,246,1,14,0.5,0.5,1,0,0.3,16.3,
    2,2018-12-21 13:48:30,2018-12-21 13:52:40,3,.00,1,N,236,236,1,4.5,0.5,0.5,0,0,0.3,5.8,
    2,2018-11-28 15:52:25,2018-11-28 15:55:45,5,.00,1,N,193,193,2,3.5,0.5,0.5,0,0,0.3,7.55,
    2,2018-11-28 15:56:57,2018-11-28 15:58:33,5,.00,2,N,193,193,2,52,0,0.5,0,0,0.3,55.55,
    2,2018-11-28 16:25:49,2018-11-28 16:28:26,5,.00,1,N,193,193,2,3.5,0.5,0.5,0,5.76,0.3,13.31,
    2,2018-11-28 16:29:37,2018-11-28 16:33:43,5,.00,2,N,193,193,2,52,0,0.5,0,0,0.3,55.55,
    1,2019-01-01 00:21:28,2019-01-01 00:28:37,1,1.30,1,N,163,229,1,6.5,0.5,0.5,1.25,0,0.3,9.05,
    1,2019-01-01 00:32:01,2019-01-01 00:45:39,1,3.70,1,N,229,7,1,13.5,0.5,0.5,3.7,0,0.3,18.5,


Este nuevo ya no está roto!


```python
# 1. Carga todos los archivos de datos. 
# Para este caso ya no es necesario cargar los datos pues lo hicimos en el ejercicio anterior.

# 2. Lee los archivos línea por línea y aplica las siguientes operaciones:
#    1. Divide la línea por comas
#    2. Filtra las líneas de acuerdo a la cantidad de columnas, sólo conserva las que tengan más de 1. Además, evita que las líneas con los
#       los nombres de las columnas sean tenidas en cuenta
#    3. Por cada línea genera un pareja (llave, valor) donde la llave corresponde a la zona de recogida (PULocationID) y el valor a a ser 1
#       En este caso la llave la determina la cantidad de columnas de la línea, pues como observamos durante la exploración los registros
#       de FHV tienen el PULocationID es una columna diferente a los Yellow. 
#       Recordemos que en los lenguajes de programación comienzan a contar desde 0 por esto para los taxis FHV se hace linea[3] y para los Yellow linea[7]
#    4. Ordena las parejas (llave, valor) por llave
#    5. Realiza la operación reduce por llave, aplicando la operación de suma (add), es decir, va a tomar todas las parejas (llave, valor)
#       con la misma llave y las va a sumar para así generar la cantidad total de viajes para cada zona de recogida
#    6. Ordena los resultados del reduce por su valor, que en este caso es la cantidad de viajes de cada zona de recogida.
resultado_map_reduce_2 = archivos_entrada.map(lambda linea: linea.split(",")) \
                                .filter(lambda linea: len(linea)>1 and "PULocationID" not in linea) \
                                .map(lambda linea: (linea[7],1) if len(linea) > 6 else (linea[3], 1)) \
                                .sortByKey() \
                                .reduceByKey(add) \
                                .sortBy(lambda par: par[1], ascending=False)
```

                                                                                    

Ahora revisar si la información ya salió de manera correcta


```python
# 4. Imprime los resultados en consola.
# En este caso sólo vamos a tomar las 10 zonas de recogida con mayor cantidad de viajes
print(resultado_map_reduce_2.take(10))
```

                                                                                    

    [('1', 14456513), ('237', 332473), ('236', 323008), ('161', 312392), ('162', 277166), ('230', 263646), ('186', 260712), ('48', 240903), ('2', 239170), ('170', 238978)]


### Ejercicio 3. Top 5 lugares de llegada
Explicación

Por lo que entiendo, esto debe de ejecutarse para poder verificar las zonas de llegada, y pedir unicamente el top 5 de ellos


```python
# 1. Carga todos los archivos de datos. 
# Para este caso ya no es necesario cargar los datos pues lo hicimos en el ejercicio anterior.

# 2. Lee los archivos línea por línea y aplica las siguientes operaciones:
#    1. Divide la línea por comas
#    2. Filtra las líneas de acuerdo a la cantidad de columnas, sólo conserva las que tengan más de 1. Además, evita que las líneas con los
#       los nombres de las columnas sean tenidas en cuenta
#    3. Por cada línea genera un pareja (llave, valor) donde la llave corresponde a la zona de recogida (PULocationID) y el valor a a ser 1
#       En este caso la llave la determina la cantidad de columnas de la línea, pues como observamos durante la exploración los registros
#       de FHV tienen el PULocationID es una columna diferente a los Yellow. 
#       Recordemos que en los lenguajes de programación comienzan a contar desde 0 por esto para los taxis FHV se hace linea[3] y para los Yellow linea[7]
#    4. Ordena las parejas (llave, valor) por llave
#    5. Realiza la operación reduce por llave, aplicando la operación de suma (add), es decir, va a tomar todas las parejas (llave, valor)
#       con la misma llave y las va a sumar para así generar la cantidad total de viajes para cada zona de recogida
#    6. Ordena los resultados del reduce por su valor, que en este caso es la cantidad de viajes de cada zona de recogida.
resultado_map_reduce_3 = archivos_entrada.map(lambda linea: linea.split(",")) \
                                .filter(lambda linea: len(linea)>1 and "DOLocationID" not in linea) \
                                .map(lambda linea: (linea[8],1) if len(linea) > 6 else (linea[3], 1)) \
                                .sortByKey() \
                                .reduceByKey(add) \
                                .sortBy(lambda par: par[1], ascending=False)
```

                                                                                    


```python
# 5. Imprime los resultados en consola.
# En este caso sólo vamos a tomar las 5 zonas de llegada con mayor cantidad de viajes
print(resultado_map_reduce_3.take(5))
```

                                                                                    

    [('', 7326207), ('N', 7285231), ('236', 334323), ('237', 296185), ('161', 293782)]


¡Tecnicamente, con esto terminaría este ejercicio!
