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

    24/06/12 14:48:31 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).


## 2. Explorar los datos
En un paso anterior subiste 2 archivos a la carpeta __datos__, ahora vamos a leerlos y explorarlos antes de realizar los ejercicios. Para explorarlos vamos a cargarlos de DataFrames, las cuales son una de las estructuras de datos que ofrece Spark.


```python
# Para leer los datos necesitas la ruta del archivo e indicar si tienen una cabecera o no, que en este caso si la tienen.
datos_yellow = spark.read.csv("../data/yellow_tripdata_2019-01.csv", header=True)
# Para visualizar los datos que leiste debes usar la función show e indicar el número de filas, en este caso 5.
datos_yellow.show(n=5)
```

    [Stage 1:>                                                          (0 + 1) / 1]

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
# Leer el archivo Parquet
datos_fhv = spark.read.parquet("../data/fhv_tripdata_2019-01.parquet", header=True)

# Mostrar las primeras 5 filas
datos_fhv.show(n=5)
```

    [Stage 8:>                                                          (0 + 1) / 1]

    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+
    |dispatching_base_num|    pickup_datetime|   dropOff_datetime|PUlocationID|DOlocationID|SR_Flag|Affiliated_base_number|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+
    |              B00001|2019-01-01 00:30:00|2019-01-01 02:51:55|        null|        null|   null|                B00001|
    |              B00001|2019-01-01 00:45:00|2019-01-01 00:54:49|        null|        null|   null|                B00001|
    |              B00001|2019-01-01 00:15:00|2019-01-01 00:54:52|        null|        null|   null|                B00001|
    |              B00008|2019-01-01 00:19:00|2019-01-01 00:39:00|        null|        null|   null|                B00008|
    |              B00008|2019-01-01 00:27:00|2019-01-01 00:37:00|        null|        null|   null|                B00008|
    +--------------------+-------------------+-------------------+------------+------------+-------+----------------------+
    only showing top 5 rows
    


                                                                                    

## Ejercicios
A continuación, vas a encontrar 2 ejercicios resueltos y explicados y uno para que completes de acuerdo a las instrucciones. 

Para cada uno de los ejercicios se van a presentar 2 opciones de solución, la primera utiliza los RDD y la segunda utiliza DataFrames. Esta última es la opción más reciente y la recomendada.

### Ejercicio 1. Contar la cantidad total de viajes 
Para este ejercicio vas a contar la cantidad total de viajes para cada uno de los tipos de servicios: **Yellow** y **FHV**

#### Opción 1. Usando RDD


```python
from operator import add # Requerido por la función reduceByKey

# ruta_entrada = "../data/*"
# Esto corrompe el archivo por lo que voy a intentar cambiar las instrucciones obteniendo un resultado 
# lógico

# 1. Carga todos los archivos de datos en un RDD
#archivos_entrada = spark.sparkContext.textFile(ruta_entrada)

# Encontrar columnas comunes
columnas_comunes = list(set(datos_fhv.columns).intersection(set(datos_yellow.columns)))
#Esto permite igualar el numero de columnas 

# Seleccionar columnas comunes
datos_fhv_comunes = datos_fhv.select(columnas_comunes)
datos_yellow_comunes = datos_yellow.select(columnas_comunes)

from pyspark.sql.functions import count, lit #esto trae cierts funcionalidades necesarias para el proceso

# Añadir columna "tipo" con el valor correspondiente a cada DataFrame
datos_fhv_comunes = datos_fhv_comunes.withColumn("tipo", lit("fhv"))
datos_yellow_comunes = datos_yellow_comunes.withColumn("tipo", lit("yellow"))

# Unir los DataFrames
archivos_entrada= datos_fhv_comunes.union(datos_yellow_comunes)

```

Al parecer no se detectan columnas comunes salvo la de tipo


```python
# Mostrar las primeras 5 líneas del DataFrame
archivos_entrada.show(10)
```

    +----+
    |tipo|
    +----+
    | fhv|
    | fhv|
    | fhv|
    | fhv|
    | fhv|
    | fhv|
    | fhv|
    | fhv|
    | fhv|
    | fhv|
    +----+
    only showing top 10 rows
    



```python
# Obtener el número de columnas de datos_fhv
num_columnas_fhv = len(datos_fhv.columns)
print("Número de columnas en datos_fhv:", num_columnas_fhv)

# Obtener el número de columnas de datos_yellow
num_columnas_yellow = len(datos_yellow.columns)
print("Número de columnas en datos_yellow:", num_columnas_yellow)

```

    Número de columnas en datos_fhv: 7
    Número de columnas en datos_yellow: 18


A pesar de eso, si se detectan columnas (las esperadas cuando se hace el conteo manual)


```python
# Encontrar columnas comunes
columnas_comunes = list(set(datos_fhv.columns).intersection(set(datos_yellow.columns)))

# Mostrar las primeras cinco líneas
print(columnas_comunes[:5])
```

    []


Se confirma la hipótesis de que no hay columnas comunes


```python
# Seleccionar columnas "PULocationID" y "DOLocationID" de datos_fhv
datos_fhv_seleccionados = datos_fhv.select("PULocationID", "DOLocationID")

# Mostrar las primeras cinco filas
datos_fhv_seleccionados.show(5)

```

    [Stage 10:>                                                         (0 + 1) / 1]

    +------------+------------+
    |PULocationID|DOLocationID|
    +------------+------------+
    |        null|        null|
    |        null|        null|
    |        null|        null|
    |        null|        null|
    |        null|        null|
    +------------+------------+
    only showing top 5 rows
    


                                                                                    

Se escogen las columnas en específico 


```python
datos_fhv_seleccionados.tail(5)

```

                                                                                    




    [Row(PULocationID=None, DOLocationID=265.0),
     Row(PULocationID=None, DOLocationID=265.0),
     Row(PULocationID=None, DOLocationID=265.0),
     Row(PULocationID=None, DOLocationID=265.0),
     Row(PULocationID=None, DOLocationID=265.0)]




```python
from pyspark.sql.functions import col

# Contar la cantidad de nulos en PULocationID
nulos_PULocationID = datos_fhv_seleccionados.where(col("PULocationID").isNull()).count()

# Contar la cantidad de nulos en DOLocationID
nulos_DOLocationID = datos_fhv_seleccionados.where(col("DOLocationID").isNull()).count()

print("Cantidad de nulos en PULocationID:", nulos_PULocationID)
print("Cantidad de nulos en DOLocationID:", nulos_DOLocationID)
```

                                                                                    

    Cantidad de nulos en PULocationID: 1819286
    Cantidad de nulos en DOLocationID: 664725


Se cuenta la cantidad de nulos para verificar que si hay presentes 


```python
# Contar la cantidad de NO nulos en PULocationID
no_nulos_PULocationID = datos_fhv_seleccionados.where(col("PULocationID").isNotNull()).count()

# Contar la cantidad de NO nulos en DOLocationID
no_nulos_DOLocationID = datos_fhv_seleccionados.where(col("DOLocationID").isNotNull()).count()

print("Cantidad de NO nulos en PULocationID:", no_nulos_PULocationID)
print("Cantidad de NO nulos en DOLocationID:", no_nulos_DOLocationID)

```

    [Stage 19:=============================>                            (1 + 1) / 2]

    Cantidad de NO nulos en PULocationID: 21339778
    Cantidad de NO nulos en DOLocationID: 22494339


                                                                                    

Se cuenta la cantidad de no nulos para verificar que si hay presentes, se realiza lo mismo pero ahora para datos_yellow


```python
# Seleccionar columnas "PULocationID" y "DOLocationID" de datos_yellow
datos_yellow_seleccionados = datos_yellow.select("PULocationID", "DOLocationID")

# Mostrar las primeras cinco filas
datos_yellow_seleccionados.show(5)

# Contar la cantidad de nulos y no nulos en PULocationID de datos_yellow
nulos_PULocationID_yellow = datos_yellow_seleccionados.where(col("PULocationID").isNull()).count()
no_nulos_PULocationID_yellow = datos_yellow_seleccionados.where(col("PULocationID").isNotNull()).count()

# Contar la cantidad de nulos y no nulos en DOLocationID de datos_yellow
nulos_DOLocationID_yellow = datos_yellow_seleccionados.where(col("DOLocationID").isNull()).count()
no_nulos_DOLocationID_yellow = datos_yellow_seleccionados.where(col("DOLocationID").isNotNull()).count()

print("Cantidad de nulos en PULocationID (datos_yellow):", nulos_PULocationID_yellow)
print("Cantidad de NO nulos en PULocationID (datos_yellow):", no_nulos_PULocationID_yellow)
print("Cantidad de nulos en DOLocationID (datos_yellow):", nulos_DOLocationID_yellow)
print("Cantidad de NO nulos en DOLocationID (datos_yellow):", no_nulos_DOLocationID_yellow)

```

    +------------+------------+
    |PULocationID|DOLocationID|
    +------------+------------+
    |         151|         239|
    |         239|         246|
    |         236|         236|
    |         193|         193|
    |         193|         193|
    +------------+------------+
    only showing top 5 rows
    


    [Stage 28:================================================>         (5 + 1) / 6]

    Cantidad de nulos en PULocationID (datos_yellow): 0
    Cantidad de NO nulos en PULocationID (datos_yellow): 7667792
    Cantidad de nulos en DOLocationID (datos_yellow): 0
    Cantidad de NO nulos en DOLocationID (datos_yellow): 7667792


                                                                                    


```python
datos_fhv_seleccionados = datos_fhv_seleccionados.withColumn("tipo", lit("fhv"))
datos_yellow_seleccionados = datos_yellow_seleccionados.withColumn("tipo", lit("yellow"))

```


```python
datos_fhv_seleccionados.tail(5)
```

                                                                                    




    [Row(PULocationID=None, DOLocationID=265.0, tipo='fhv'),
     Row(PULocationID=None, DOLocationID=265.0, tipo='fhv'),
     Row(PULocationID=None, DOLocationID=265.0, tipo='fhv'),
     Row(PULocationID=None, DOLocationID=265.0, tipo='fhv'),
     Row(PULocationID=None, DOLocationID=265.0, tipo='fhv')]




```python
datos_yellow_seleccionados.tail(5)
```




    [Row(PULocationID='263', DOLocationID='4', tipo='yellow'),
     Row(PULocationID='193', DOLocationID='193', tipo='yellow'),
     Row(PULocationID='264', DOLocationID='264', tipo='yellow'),
     Row(PULocationID='264', DOLocationID='7', tipo='yellow'),
     Row(PULocationID='193', DOLocationID='193', tipo='yellow')]



Aquí ya uní los dataframes


```python
# Unir los DataFrames
archivos_entrada_df= datos_fhv_seleccionados.union(datos_yellow_seleccionados)
```


```python
archivos_entrada_df.show(5)
```

    +------------+------------+----+
    |PULocationID|DOLocationID|tipo|
    +------------+------------+----+
    |        null|        null| fhv|
    |        null|        null| fhv|
    |        null|        null| fhv|
    |        null|        null| fhv|
    |        null|        null| fhv|
    +------------+------------+----+
    only showing top 5 rows
    



```python
archivos_entrada=archivos_entrada_df.rdd

# Mostrar las primeras cinco líneas
print("Primeras cinco líneas:")
for linea in archivos_entrada.take(5):
    print(linea)



```

    Primeras cinco líneas:


    [Stage 42:>                                                         (0 + 1) / 1]

    Row(PULocationID=None, DOLocationID=None, tipo='fhv')
    Row(PULocationID=None, DOLocationID=None, tipo='fhv')
    Row(PULocationID=None, DOLocationID=None, tipo='fhv')
    Row(PULocationID=None, DOLocationID=None, tipo='fhv')
    Row(PULocationID=None, DOLocationID=None, tipo='fhv')


                                                                                    

Aqui no funciona porque la memoria para la operación "no da"


```python
# Mostrar las últimas cinco líneas
print("\nÚltimas cinco líneas:")
for linea in archivos_entrada.take(archivos_entrada.count())[-5:]:
    print(linea)
```

    
    Últimas cinco líneas:


    [Stage 44:>                                                         (0 + 1) / 1]


    ---------------------------------------------------------------------------

    Py4JJavaError                             Traceback (most recent call last)

    /tmp/ipykernel_14/3506228363.py in <module>
          1 # Mostrar las últimas cinco líneas
          2 print("\nÚltimas cinco líneas:")
    ----> 3 for linea in archivos_entrada.take(archivos_entrada.count())[-5:]:
          4     print(linea)


    /usr/local/lib/python3.7/dist-packages/pyspark/rdd.py in take(self, num)
       1564 
       1565             p = range(partsScanned, min(partsScanned + numPartsToTry, totalParts))
    -> 1566             res = self.context.runJob(self, takeUpToNumLeft, p)
       1567 
       1568             items += res


    /usr/local/lib/python3.7/dist-packages/pyspark/context.py in runJob(self, rdd, partitionFunc, partitions, allowLocal)
       1231         # SparkContext#runJob.
       1232         mappedRDD = rdd.mapPartitions(partitionFunc)
    -> 1233         sock_info = self._jvm.PythonRDD.runJob(self._jsc.sc(), mappedRDD._jrdd, partitions)
       1234         return list(_load_from_socket(sock_info, mappedRDD._jrdd_deserializer))
       1235 


    /usr/local/lib/python3.7/dist-packages/py4j/java_gateway.py in __call__(self, *args)
       1303         answer = self.gateway_client.send_command(command)
       1304         return_value = get_return_value(
    -> 1305             answer, self.gateway_client, self.target_id, self.name)
       1306 
       1307         for temp_arg in temp_args:


    /usr/local/lib/python3.7/dist-packages/pyspark/sql/utils.py in deco(*a, **kw)
        109     def deco(*a, **kw):
        110         try:
    --> 111             return f(*a, **kw)
        112         except py4j.protocol.Py4JJavaError as e:
        113             converted = convert_exception(e.java_exception)


    /usr/local/lib/python3.7/dist-packages/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
        326                 raise Py4JJavaError(
        327                     "An error occurred while calling {0}{1}{2}.\n".
    --> 328                     format(target_id, ".", name), value)
        329             else:
        330                 raise Py4JError(


    Py4JJavaError: An error occurred while calling z:org.apache.spark.api.python.PythonRDD.runJob.
    : org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 44.0 failed 4 times, most recent failure: Lost task 0.3 in stage 44.0 (TID 99) (172.18.0.4 executor 7): java.lang.OutOfMemoryError: Java heap space
    	at java.util.Arrays.copyOf(Arrays.java:3236)
    	at java.io.ByteArrayOutputStream.grow(ByteArrayOutputStream.java:118)
    	at java.io.ByteArrayOutputStream.ensureCapacity(ByteArrayOutputStream.java:93)
    	at java.io.ByteArrayOutputStream.write(ByteArrayOutputStream.java:153)
    	at org.apache.spark.util.ByteBufferOutputStream.write(ByteBufferOutputStream.scala:41)
    	at java.io.ObjectOutputStream$BlockDataOutputStream.drain(ObjectOutputStream.java:1877)
    	at java.io.ObjectOutputStream$BlockDataOutputStream.setBlockDataMode(ObjectOutputStream.java:1786)
    	at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1189)
    	at java.io.ObjectOutputStream.writeObject(ObjectOutputStream.java:348)
    	at org.apache.spark.serializer.JavaSerializationStream.writeObject(JavaSerializer.scala:44)
    	at org.apache.spark.serializer.JavaSerializerInstance.serialize(JavaSerializer.scala:101)
    	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:542)
    	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    	at java.lang.Thread.run(Thread.java:748)
    
    Driver stacktrace:
    	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2253)
    	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2202)
    	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2201)
    	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
    	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
    	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
    	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2201)
    	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1078)
    	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1078)
    	at scala.Option.foreach(Option.scala:407)
    	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1078)
    	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2440)
    	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2382)
    	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2371)
    	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
    	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:868)
    	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2202)
    	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2223)
    	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2242)
    	at org.apache.spark.api.python.PythonRDD$.runJob(PythonRDD.scala:166)
    	at org.apache.spark.api.python.PythonRDD.runJob(PythonRDD.scala)
    	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    	at java.lang.reflect.Method.invoke(Method.java:498)
    	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
    	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
    	at py4j.Gateway.invoke(Gateway.java:282)
    	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
    	at py4j.commands.CallCommand.execute(CallCommand.java:79)
    	at py4j.GatewayConnection.run(GatewayConnection.java:238)
    	at java.lang.Thread.run(Thread.java:748)
    Caused by: java.lang.OutOfMemoryError: Java heap space
    	at java.util.Arrays.copyOf(Arrays.java:3236)
    	at java.io.ByteArrayOutputStream.grow(ByteArrayOutputStream.java:118)
    	at java.io.ByteArrayOutputStream.ensureCapacity(ByteArrayOutputStream.java:93)
    	at java.io.ByteArrayOutputStream.write(ByteArrayOutputStream.java:153)
    	at org.apache.spark.util.ByteBufferOutputStream.write(ByteBufferOutputStream.scala:41)
    	at java.io.ObjectOutputStream$BlockDataOutputStream.drain(ObjectOutputStream.java:1877)
    	at java.io.ObjectOutputStream$BlockDataOutputStream.setBlockDataMode(ObjectOutputStream.java:1786)
    	at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1189)
    	at java.io.ObjectOutputStream.writeObject(ObjectOutputStream.java:348)
    	at org.apache.spark.serializer.JavaSerializationStream.writeObject(JavaSerializer.scala:44)
    	at org.apache.spark.serializer.JavaSerializerInstance.serialize(JavaSerializer.scala:101)
    	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:542)
    	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    	... 1 more




```python
from operator import add

# Convertir archivos_entrada a RDD de filas y luego a RDD de cadenas de texto
rdd_archivos_entrada = archivos_entrada_df.rdd.map(lambda row: ",".join([str(x) for x in row]))

# Aplicar las transformaciones sobre el RDD
resultado_map_reduce = rdd_archivos_entrada.map(lambda linea: linea.split(",")) \
                                .filter(lambda linea: len(linea) > 1 and "PULocationID" not in linea) \
                                .map(lambda linea: ("yellow", 1) if linea[-1].strip() == "yellow" else ("fhv", 1)) \
                                .reduceByKey(add) \
                                .sortByKey()

```

                                                                                    


```python
# Mostrar los resultados usando take para obtener una muestra
print("Resultados:")
for result in resultado_map_reduce.take(3):  # Muestra los primeros 10 resultados
    print(result)
```

    Resultados:
    ('fhv', 23159064)
    ('yellow', 7667792)



```python
# Contar el total de registros en datos_fhv
total_registros_fhv = datos_fhv_seleccionados.count()

print("Total de registros en datos_fhv:", total_registros_fhv)
```

    [Stage 84:>                                                         (0 + 1) / 1]

    Total de registros en datos_fhv: 23159064


                                                                                    


```python
# Contar el total de registros en datos_fhv
total_registros_yellow = datos_yellow_seleccionados.count()

print("Total de registros en datos_yellow:", total_registros_yellow)
```

    [Stage 85:================================================>         (5 + 1) / 6]

    Total de registros en datos_yellow: 7667792


                                                                                    

Con esto me aseguro que la cantidad llevada a cabo con Mapreduce fue correcto en correspondencia a lo descrito en dataframes

#### Opción 2. Usando DataFrames
En este caso dado que ambos archivos tienen columnas diferentes, al cargalos en un solo DataFrame sólo se conservarían las columnas del primer archivo leído en este caso el archivo con los datos de **FHV** y no habría una forma de diferenciar las filas de **FHV** de las **Yellow**, por lo tanto, vamos a usar los DataFrames creados en el paso 2 para contar la cantida de datos de cada uno de los tipos de servicios.


```python
# Vamos a usar la función count para contar la cantidad total de viajes de cada uno de los DataFrames.
cantidad_yellow = datos_yellow.count()
cantidad_fhv = datos_fhv.count()

# Luego, usamos print para visualizar los resultados
print("Cantidad de viajes de tipo yellow: ", cantidad_yellow)
print("Cantidad de viajes de tipo fhv: ", cantidad_fhv)
```

    [Stage 68:>                                                         (0 + 1) / 1]

    Cantidad de viajes de tipo yellow:  7667792
    Cantidad de viajes de tipo fhv:  23130810


                                                                                    

### Ejercicio 2. Contar las cantidad total de viajes por zona de recogida
Para este ejercicio vas a contar la cantidad total de viajes para cada uno de las zonas de recogida.

#### Opción 1. Usando RDD
El código de la zona de recogida corresponde a la variable **PULocationID**, que el caso de los taxis Yellow se encuentra en la columna 8 y para los FHV en la 4.


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
# Aplicar las transformaciones sobre el RDD

resultado_map_reduce_2 = rdd_archivos_entrada.map(lambda linea: linea.split(",")) \
                                .filter(lambda linea: len(linea) > 1 and "PULocationID" not in linea) \
                                .map(lambda linea: (linea[0], 1)) \
                                .reduceByKey(add) \
                                .sortBy(lambda par: par[1], ascending=False)
```

                                                                                    


```python
# 4. Imprime los resultados en consola.
# En este caso sólo vamos a tomar las 10 zonas de recogida con mayor cantidad de viajes
print(resultado_map_reduce_2.take(10))
```

    [Stage 104:==========================================>              (6 + 1) / 8]

    [('None', 1819286), ('264.0', 779103), ('79.0', 352219), ('237', 332473), ('132.0', 329720), ('236', 323008), ('161.0', 317469), ('161', 312392), ('234.0', 310241), ('61.0', 294099)]


                                                                                    

#### Opción 2. Usando DataFrames
En este caso vamos a unir ambos dataframes usando la función *unionByName* la cual permite unir 2 DataFrames que tengan columnas diferentes. 

Dado que en ambos DataFrames la columna con la zona de recogida es llamada PULocationID no es necesario renombrarla en ninguno de los 2, si el nombre de esta columna fuese diferente sería necesario renombrarla igual en ambos DataFrames para poder realizar el cálculo.


```python
# 1. Se usa la función unionByName para unir ambos DataFrames
dataframe_union = datos_yellow.unionByName(datos_fhv, allowMissingColumns=True)

# 2. Se utiliza la función groupBy para agrupar las filas por la zona de recogida (PULocationID).
#    Luego, se utiliza la función count para contar la cantidad de viajes/filas para cada PULocationID único.
#    Por último, se ordena de manera descendente por la cantidad de viajes (variable count) usando la función orderBy.
dataframe_respuesta = dataframe_union.groupBy("PULocationID").count().orderBy("count", ascending=False)

# 3. En este caso sólo vamos a tomar las 10 zonas de recogida con mayor cantidad de viajes
dataframe_respuesta.show(n=10)
```

    [Stage 107:====================================================>(197 + 3) / 200]

    +------------+-------+
    |PULocationID|  count|
    +------------+-------+
    |        null|1819286|
    |       264.0| 779103|
    |        79.0| 352219|
    |         237| 332473|
    |       132.0| 329720|
    |         236| 323008|
    |       161.0| 317469|
    |         161| 312392|
    |       234.0| 310241|
    |        61.0| 294099|
    +------------+-------+
    only showing top 10 rows
    


                                                                                    


```python
Con esto confirmamos la operación realizada es correcta
```

### Ejercicio 3. Matriz de origen destino
Para este ejercicio vas a contar la cantidad de viajes que tengan la misma zona de salida y llegada, es decir, vas a hacer un conteo por dos llaves.

#### Opción 1. Usando RDD
Para este caso vamos a usar como llave una tupla con la zona de recogida y llegada, es por eso que en el map la llave luce de la siguiente manera para los FHV:
``(linea[7], linea[8])``


```python
# 1. Carga todos los archivos de datos. 
# Para este caso ya no es necesario cargar los datos pues lo hicimos en el ejercicio anterior.

# 2. Lee los archivos línea por línea y aplica las siguientes operaciones:
#    1. Divide la línea por comas
#    2. Filtra las líneas de acuerdo a la cantidad de columnas, sólo conserva las que tengan más de 1. Además, evita que las líneas con los
#       los nombres de las columnas sean tenidas en cuenta
#    3. Por cada línea genera un pareja (llave, valor) donde la llave corresponde a la zona de recogida (PULocationID) y la zona de llegada (DOLocationID)
#       y el valor va a ser 1.
#       En este caso la llave la determina la cantidad de columnas de la línea, pues como observamos durante la exploración los registros
#       de FHV tienen el PULocationID y DOLocationID es una columna diferente a los Yellow. 
#       Recordemos que en los lenguajes de programación comienzan a contar desde 0 por esto para los taxis FHV se hace linea[3] y para los Yellow linea[7]
#    4. Ordena las parejas (llave, valor) por llave
#    5. Realiza la operación reduce por llave, aplicando la operación de suma (add), es decir, va a tomar todas las parejas (llave, valor)
#       con la misma llave y las va a sumar para así generar la cantidad total de viajes para cada pareja de zona de recogida y llegada.
#    6. Ordena los resultados del reduce por su valor, que en este caso es la cantidad de viajes por cada pareja (zona de recogida, zona llegada).
# Aplicar las transformaciones sobre el RDD
resultado_map_reduce_3 = rdd_archivos_entrada.map(lambda linea: linea.split(",")) \
                                .filter(lambda linea: len(linea) > 1 and "PULocationID" not in linea) \
                                .map(lambda linea: ((linea[0], linea[1]), 1)) \
                                .reduceByKey(add) \
                                .sortBy(lambda par: par[1], ascending=False) 
```

                                                                                    


```python
# 4. Imprime los resultados en consola.
# En este caso sólo vamos a tomar las 20 combinaciones de zona de recogida y llegada con mayor cantidad de viajes
print(resultado_map_reduce_3.take(20))
```

    [Stage 121:=================================================>       (7 + 1) / 8]

    [(('None', '265.0'), 1125595), (('264.0', '264.0'), 775115), (('None', 'None'), 658972), (('264', '264'), 138614), (('132.0', '265.0'), 63668), (('237', '236'), 51080), (('236', '236'), 47714), (('236', '237'), 43308), (('237', '237'), 39907), (('76.0', '76.0'), 38431), (('138.0', '265.0'), 36823), (('26.0', '26.0'), 33860), (('61.0', '61.0'), 32659), (('39.0', '39.0'), 29427), (('None', '0.0'), 28062), (('7.0', '7.0'), 26705), (('265.0', '265.0'), 26625), (('181.0', '181.0'), 26335), (('239', '238'), 25663), (('129.0', '129.0'), 25396)]


                                                                                    

#### Opción 1. Usando DataFrames


```python
# 1. Se usa la función unionByName para unir ambos DataFrames
dataframe_union = datos_yellow.unionByName(datos_fhv, allowMissingColumns=True)

# 2. Se utiliza la función groupBy para agrupar las filas por la zona de recogida (PULocationID) y la zona de llegada (DOLocationID).
#    Luego, se utiliza la función count para contar la cantidad de viajes/filas para cada PULocationID único.
#    Por último, se ordena de manera descendente por la cantidad de viajes (variable count) usando la función orderBy.
dataframe_respuesta = dataframe_union.groupBy("PULocationID","DOLocationID").count().orderBy("count", ascending=False)

# 3. En este caso sólo vamos a tomar las 10 zonas de recogida con mayor cantidad de viajes
dataframe_respuesta.show(n=20)
```

                                                                                    

    +------------+------------+-------+
    |PULocationID|DOLocationID|  count|
    +------------+------------+-------+
    |        null|       265.0|1125595|
    |       264.0|       264.0| 775115|
    |        null|        null| 658972|
    |         264|         264| 138614|
    |       132.0|       265.0|  63668|
    |         237|         236|  51080|
    |         236|         236|  47714|
    |         236|         237|  43308|
    |         237|         237|  39907|
    |        76.0|        76.0|  38431|
    |       138.0|       265.0|  36823|
    |        26.0|        26.0|  33860|
    |        61.0|        61.0|  32659|
    |        39.0|        39.0|  29427|
    |        null|         0.0|  28062|
    |         7.0|         7.0|  26705|
    |       265.0|       265.0|  26625|
    |       181.0|       181.0|  26335|
    |         239|         238|  25663|
    |       129.0|       129.0|  25396|
    +------------+------------+-------+
    only showing top 20 rows
    


Con esta información, podremos comprobar que lo previsot en el método MapReduce usando RDD se cumple 

### Ejercicio 4. Top 5 lugares de llegada
Para este ejercicio vas a contar la cantidad total de viajes para cada una de las zonas de llegada y posteriormente vas a buscar las 5 zonas con mayor cantidad de viajes. 

Recuerda que la zona de llegada es la columna **DOLocationID** para ambos tipos de servicios.

**Para resolver este ejercicio utiliza el método de DataFrames, es importante que dejes tu respuesta en la variable ``solucion``**

Recuerda que para obtener una Lista con las n primeras filas de un DataFrame puedes usar la función *head* de la siguiente manera:

``dataframe.head(n)``

Si quisieras usar la función para obtener los 2 primeros elementos el código sería el siguiente:

``dataframe.head(2)``


```python
# INICIO DE TU SOLUCIÓN

resultado_map_reduce_4 = rdd_archivos_entrada.map(lambda linea: linea.split(",")) \
                                .filter(lambda linea: len(linea) > 1 and "PULocationID" not in linea) \
                                .map(lambda linea: (linea[1], 1)) \
                                .reduceByKey(add) \
                                .sortBy(lambda par: par[1], ascending=False)

solucion = resultado_map_reduce_4.take(5)
# FIN DE TU SOLUCIÓN
```

                                                                                    

Para asegurarte de que dejaste tu respuesta en la variable ``solucion`` ejecuta la siguiente celda y mira que el resultado sea una lista de Filas/Row así: 

``Row(DOLocationID=x, count=y)``


```python
solucion
```




    [('265.0', 1809708),
     ('264.0', 775155),
     ('None', 664725),
     ('132.0', 357853),
     ('236', 334323)]



Ahora voy aplicar el método mismo pero utilizando DataFrames para asegurarme de mi respuesta


```python
# 2. Se utiliza la función groupBy para agrupar las filas por la zona de recogida (PULocationID).
#    Luego, se utiliza la función count para contar la cantidad de viajes/filas para cada PULocationID único.
#    Por último, se ordena de manera descendente por la cantidad de viajes (variable count) usando la función orderBy.
dataframe_respuesta = dataframe_union.groupBy("DOLocationID").count().orderBy("count", ascending=False)

# 3. En este caso sólo vamos a tomar las 10 zonas de recogida con mayor cantidad de viajes
dataframe_respuesta.show(n=5)
```

                                                                                    

    +------------+-------+
    |DOLocationID|  count|
    +------------+-------+
    |       265.0|1809708|
    |       264.0| 775155|
    |        null| 664725|
    |       132.0| 357853|
    |         236| 334323|
    +------------+-------+
    only showing top 5 rows
    



```python
dataframe_respuesta.head(5)
```

                                                                                    




    [Row(DOLocationID='265.0', count=1809708),
     Row(DOLocationID='264.0', count=775155),
     Row(DOLocationID=None, count=664725),
     Row(DOLocationID='132.0', count=357853),
     Row(DOLocationID='236', count=334323)]



Ahora, debes generar el archivo de respuesta que debe cargar a la actividad de programación es coursera, para esto debes ejecutar la siguiente celda **una sola vez** la cual va a generar un archivo llamado **answer.zip** en la carpeta raíz (la carpeta donde están las carpetas _notebooks_ y _data_).


```python
from generar_archivo.generar_archivo_respuesta import to_dict, preparar_entregable 
import os

preparar_entregable(to_dict(solucion), f"{os.getcwd()}/generar_archivo/response_ppdnt.json", "../")
```

Una vez generado el archivo debes descargarlo, para esto debes dar **click derecho** sobre el y elegir la opción **Download**, esto te permitrá guardar el archivo en la carpeta de tu computador que selecciones. ![Ejemplo de descarga](https://i.ibb.co/d0QBhch/Captura-de-pantalla-de-2021-07-29-20-45-23.png)

# ¡Felicitaciones! Has terminado tu actividad de proyecto
