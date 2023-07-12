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
4.
