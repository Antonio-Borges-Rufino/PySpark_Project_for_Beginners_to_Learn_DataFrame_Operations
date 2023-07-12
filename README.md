# Referencia
1. Esse projeto é derivado do site [projectpro.io](https://www.projectpro.io/https://www.projectpro.io/)
2. Os dados são de responsabilidade do site e não serão publicados aqui
3. Ouve pequenas alterações pessoais no código

# Implementações e casos de uso:
1. Após importar as bases de dados, lendo o cabeçalho e o schema, podemos ver o schema e a lista de colunas.
```
df.printSchema()
```
#### Resultado
```
|-- _c0: integer (nullable = true)
 |-- Year: integer (nullable = true)
 |-- Quarter: integer (nullable = true)
 |-- Month: integer (nullable = true)
 |-- DayofMonth: integer (nullable = true)
 |-- DayOfWeek: integer (nullable = true)
 |-- FlightDate: string (nullable = true)
 |-- Reporting_Airline: string (nullable = true)
 |-- DOT_ID_Reporting_Airline: integer (nullable = true)
 |-- IATA_CODE_Reporting_Airline: string (nullable = true)
 |-- Tail_Number: string (nullable = true)
      .
      .
      .
```
2. Nesse caso de uso, quis selecionar apenas algumas colunas importantes.
```
df_menor = df.select("Year","Month","DayofMonth","FlightDate","Tail_Number","Flight_Number_Reporting_Airline")
df_menor.show(5)
```
#### Resultado
```
+----+-----+----------+----------+-----------+-------------------------------+
|Year|Month|DayofMonth|FlightDate|Tail_Number|Flight_Number_Reporting_Airline|
+----+-----+----------+----------+-----------+-------------------------------+
|1998|    1|         2|1998-01-02|     N297US|                            675|
|2009|    5|        28|2009-05-28|     N946AT|                            671|
|2013|    6|        29|2013-06-29|     N665MQ|                           3297|
|2010|    8|        31|2010-08-31|     N6705Y|                           1806|
|2006|    1|        15|2006-01-15|     N504AU|                            465|
+----+-----+----------+----------+-----------+-------------------------------+
only showing top 5 rows
```
3. No próximo caso de uso, surgiu uma necessidade de agrupar e ver a quantidade de vôos por ano/mes
```
df_menor.select("Year","Month").groupBy("Year","Month").count().orderBy("Year","Month").show()
```
#### Resultado
```
+----+-----+-----+
|Year|Month|count|
+----+-----+-----+
|1987|   10|  115|
|1987|   11|  112|
|1987|   12|  112|
|1988|    1|  105|
|1988|    2|   97|
|1988|    3|  112|
|1988|    4|  127|
        .
        .
        .
```
4. Aqui, o caso de uso foi ver a quantidade de cancelamentos de vôos por mes em todas as cidades.
```
df.select("Year","Month","OriginCityName").filter("Cancelled = 1.0").groupBy("Year","Month","OriginCityName").count().orderBy("count",ascending=False).show()
```
#### Resultado
```
+----+-----+--------------------+-----+
|Year|Month|      OriginCityName|count|
+----+-----+--------------------+-----+
|1998|    9|     Minneapolis, MN|    3|
|1996|    1|        New York, NY|    3|
|1999|    9|          Newark, NJ|    3|
|1996|    1|      Washington, DC|    3|
|2020|    3|         Chicago, IL|    3|
|2019|    1|         Chicago, IL|    2|
               .
               .
               .
```
5. Agora, queremos saber a média de atraso dos vôos que atrasaram mais de 15 minutos
```
df.select("Reporting_Airline",'ArrDelay').filter("ArrDel15 = 1.0").groupBy("Reporting_Airline").mean("ArrDelay").show()
```
#### Resultado
```
+-----------------+------------------+
|Reporting_Airline|     avg(ArrDelay)|
+-----------------+------------------+
|               UA| 55.84326923076923|
|               EA| 40.78431372549019|
|               PI|31.704918032786885|
|               NK|  65.0576923076923|
|               PS|              34.0|
                  .
                  .
                  .
```
6. Aqui, pesquisei qual foi a companhia aerea que mais teve horário cumprido.
```
df.select("Year","Reporting_Airline").filter((df.DepDelay<=0)&(df.ArrDelay<=0)).groupBy("Year","Reporting_Airline").count().orderBy("Reporting_Airline","Year").show()
```
#### Resultado
```
+----+-----------------+-----+
|Year|Reporting_Airline|count|
+----+-----------------+-----+
|2007|               9E|   30|
|2008|               9E|   39|
|2009|               9E|   42|
|2010|               9E|   37|
|2013|               9E|   61|
              .
              .
              .
```
