from functools import reduce
from django.http import JsonResponse
from pyspark.sql.functions import col, avg, sum, when, min as Fmin, max as Fmax
from pyspark.sql import SparkSession
from members.db_config import db 
from pyspark.sql import functions as F
import os
from pyspark.sql.types import FloatType, NumericType

class WeatherDataProcessor:
    def __init__(self):
        # Inizializza la sessione Spark
        self.spark = self.create_spark_session()

    def create_spark_session(self):
        """
        Crea e restituisce una sessione Spark.
        """
        spark = SparkSession.builder \
            .appName("WeatherQuery") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
        return spark


    def station_query(self, state_code, month_start, month_end, info):
        if not state_code:
            return {"error": "State code is required."}
        
        try:
            # Verifica che i mesi siano validi
            if not isinstance(month_start, int) or not isinstance(month_end, int):
                return {"error": "Invalid month values, must be integers."}

            month_start -=1
            month_end -=1

            length = month_end - month_start
            if length < 0:
                return {"error": "Invalid month range. Month start cannot be greater than month end."}

            # Carica il file station
            df = self.spark.read.csv(db[0][5], header=True, inferSchema=True, sep="|")

            arr_month = []
            for i in range(month_start, month_end + 1):
                # Carica i dati per ogni mese
                month_data = self.spark.read.csv(db[i][2], header=True, inferSchema=True)
                arr_month.append(month_data)

            # Verifica che tutti i dataframe abbiano le stesse colonne prima di unirli
            if not all(set(arr_month[0].columns) == set(month.columns) for month in arr_month):
                return {"error": "Mismatch in columns between months."}

            # Unisci i dataframe per i mesi specificati
            for i in range(1, len(arr_month)):
                arr_month[0] = arr_month[0].union(arr_month[i])

            # Unisci con il dataframe delle stazioni meteo
            ris = arr_month[0].join(df, arr_month[0]["WBAN"] == df["WBAN"], "left")
            ris = ris.filter(ris["State"] == state_code)
            
            if info not in ris.columns:
                return {"error": f"Column '{info}' not found in the data."}
            
            # Calcola la media del dato richiesto
            result = ris.agg(avg(info)).collect()

            if result:
                return {"result": float(result[0][0])}
            else:
                return {"error": f"No data found for {state_code} in the specified months."}
            
        except Exception as e:
            return {"error": f"Error loading data: {str(e)}"}

        
    def calculate_monthly_avg_temperature(self, path):
        """
        Calcola la temperatura media mensile per un mese specifico utilizzando i dati forniti.
        """
    
        if not path:
            return {"error": f"No data found for month: {path}"}

        # Carica i dati dal file CSV
        try:
            df = self.spark.read.csv(path, header=True, inferSchema=True)
        except Exception as e:
            return {"error": f"Error loading data from {path}: {str(e)}"}

        # Calcola la temperatura media per stazione
        try:
            overall_avg = df.agg(avg("AvgTemp").alias("overall_avg"))
            
            return (overall_avg.collect()[0][0]-31)/1.8
        except Exception as e:
            return {"error": f"Error processing data: {str(e)}"}


    def find_anomalous_days(self,month_index):
        try:
            df = self.spark.read.csv(db[month_index][0], header=True, inferSchema=True)

            anomalous_days = df.filter(
                (col("Depart") > 5) & 
                ((col("Max5Speed") > 20) | (col("PrecipTotal") > 1))
            )

            return anomalous_days.select("YearMonthDay", "WBAN", "Depart", "Max5Speed", "PrecipTotal").collect()
        except Exception as e:
            return {"error": f"Error processing data: {str(e)}"}

    def ideal_days_for_agriculture(self, path, selected_wban=None):
        try:
            df = self.spark.read.csv(db[path][0], header=True, inferSchema=True)

            df = df.withColumn("Tavg", col("Tavg").cast("float"))
            df = df.withColumn("DewPoint", col("DewPoint").cast("float"))
            df = df.withColumn("PrecipTotal", col("PrecipTotal").cast("float"))
            df = df.withColumn("WetBulb", col("WetBulb").cast("float"))
            
            ideal_days = df.filter(
                (col("Tavg").between(60, 75)) &
                (col("DewPoint").between(40, 60)) &
                (~col("CodeSum").like("%TS%")) &
                (col("PrecipTotal") == 0)
            )

            if selected_wban:
                ideal_days = ideal_days.filter(col("WBAN") == selected_wban)

            result = ideal_days.select(
                "WBAN", 
                "YearMonthDay", 
                "Tavg", 
                "DewPoint", 
                "PrecipTotal", 
                "WetBulb"
            )

            return result.collect()

        except Exception as e:
            return {"error": f"Error processing data: {str(e)}"}

    def get_wban_list(self, state_code):
        # Leggi il file CSV
        df = self.spark.read.csv(db[0][5], header=True, inferSchema=True, sep="|")

        # Filtra i dati per il codice dello stato
        filtered_df = df.filter(df["State"] == state_code)

        # Ottieni la lista dei valori unici di WBAN
        wban_list = [row["WBAN"] for row in filtered_df.select("WBAN").distinct().collect()]

        return wban_list


    def hourly_temp(self, state_code, month, wban, info):
        if not state_code:
            return {"error": "State code is required."}
        
        try:
            month = int(month)-1  # Adatta il mese all'indice dell'array (0-based)
            
            # Carica il file station
            df = self.spark.read.csv(db[0][5], header=True, inferSchema=True, sep="|")
            month_data = self.spark.read.csv(db[month][1], header=True, inferSchema=True)
            
            # Unisci con il dataframe delle stazioni meteo
            ris = month_data.join(df, month_data["WBAN"] == df["WBAN"], "left")
            ris = ris.filter(ris["State"] == state_code)

            # Verifica se la colonna `info` esiste nei dati
            if info not in ris.columns:
                return {"error": f"Column '{info}' not found in the data."}

            # Filtra per WBAN solo se fornito
            if wban:
                ris = ris.filter(ris["WBAN"] == wban)
            
            # Calcola la media oraria per la colonna `info`
            value = []
            for i in range(0, 24):
                temp = ris.filter((ris["Time"] > i * 100) & (ris["Time"] < (i + 1) * 100))
                avg_temp = temp.agg(avg(info)).collect()[0][0]
                value.append(avg_temp if avg_temp is not None else 0)  # Aggiungi 0 se nessun valore è disponibile

            return value

        except Exception as e:
            return {"error": f"Error loading data: {str(e)}"}



    def type_query(self, state_code, month, types, info, unit):
        if not state_code:
            return {"error": "State code is required."}
        
        try:
            # Adatta il mese all'indice dell'array (0-based)
            mon = int(month) - 1
            typ = int(types) - 1
            
            # Carica il file station
            df = self.spark.read.csv(db[0][5], header=True, inferSchema=True, sep="|")
            
            # Carica i dati del mese e tipo
            month_data = self.spark.read.csv(db[mon][typ], header=True, inferSchema=True)

            # Unisci con il dataframe delle stazioni meteo
            ris = month_data.join(df, month_data["WBAN"] == df["WBAN"], "left")
            ris = ris.filter(ris["State"] == state_code)

            # Verifica se la colonna `info` esiste nei dati
            if info not in ris.columns:
                return {"error": f"Column '{info}' not found in the data."}

            # Sostituisci i valori "M" con None e forza la colonna a FloatType
            ris = ris.withColumn(info, when(col(info) == "M", None).otherwise(col(info).cast(FloatType())))

            # Verifica che la colonna `info` sia numerica
            if not isinstance(ris.schema[info].dataType, NumericType):
                return {"error": f"The column '{info}' is not numeric and cannot be aggregated."}

            # Calcola il valore minimo
            min_value = round(ris.agg(Fmin(info)).collect()[0][0],2)
            min_row = ris.filter(ris[info] == min_value).collect()

            # Calcola il valore massimo
            max_value = round(ris.agg(Fmax(info)).collect()[0][0],2)
            max_row = ris.filter(ris[info] == max_value).collect()

            # Costruisci il risultato
            result = {
                "min": {
                    "WBAN": min_row[0]["WBAN"] if min_row else None,
                    "Location": min_row[0]["Location"] if min_row else None,
                    "Latitude": min_row[0]["Latitude"] if min_row else None,
                    "Longitude": min_row[0]["Longitude"] if min_row else None,
                    "Value": min_value
                },
                "max": {
                    "WBAN": max_row[0]["WBAN"] if max_row else None,
                    "Location": max_row[0]["Location"] if max_row else None,
                    "Latitude": max_row[0]["Latitude"] if max_row else None,
                    "Longitude": max_row[0]["Longitude"] if max_row else None,
                    "Value": max_value
                },
                "difference": max_value - min_value if max_value is not None and min_value is not None else None,
                "unit" : unit
            }

            return result

        except Exception as e:
            import traceback
            return {"error": f"Error loading data: {str(e)}\n{traceback.format_exc()}"}

    

    def unreliable_data_query(self, state_code, month_start, month_end):
        if not state_code:
            return {"error": "State code is required."}

        try:
            # Verifica input dei mesi
            if not isinstance(month_start, int) or not isinstance(month_end, int):
                return {"error": "Invalid month values, must be integers."}

            month_start -= 1
            month_end -= 1

            if month_start > month_end:
                return {"error": "Invalid month range. Month start cannot be greater than month end."}

            # Carica il file station per eventuale filtro per stato
            station_df = self.spark.read.csv(db[0][5], header=True, inferSchema=True, sep="|")
            filtered_station_df = station_df.filter(station_df.State == state_code)
            
            # Carica i dati per i mesi specificati
            merged_df = None
            for i in range(month_start, month_end + 1):
                month_data = self.spark.read.csv(db[i][0], header=True, inferSchema=True)

                if merged_df is None:
                    merged_df = month_data
                else:
                    # Unire i DataFrame in base alla colonna WBAN
                    merged_df = merged_df.union(month_data)

            filtered_merged_df = merged_df
            # Filtra per stato, se specificato
            if state_code != "tutti":
                valid_wbans = filtered_station_df.select("WBAN")
                filtered_merged_df = merged_df.join(valid_wbans, on="WBAN", how="inner")

            columns_to_check = [col for col in merged_df.columns if col not in ['WBAN', 'YearMonthDay']]

            # Creiamo una colonna che conta il numero di "s" per ogni riga nelle colonne selezionate
            s_count_col = reduce(lambda acc, col: acc + F.when(F.col(col) == 's', 1).otherwise(0), columns_to_check, F.lit(0))

            # Aggiungere la colonna al DataFrame
            merged_df_with_s_count = filtered_merged_df.withColumn("total_s_count", s_count_col)

            # Calcolare il numero totale di "s" per ogni WBAN
            wbans_with_s_count = merged_df_with_s_count.groupBy("WBAN").agg(F.sum("total_s_count").alias("total_s_count"))

            # Ordinare in ordine decrescente in base al numero totale di "s" e restituire i primi 10 WBAN
            top_10_wbans = wbans_with_s_count.orderBy(F.desc("total_s_count")).limit(10)

            
            top_10_wbans.show()

            result = top_10_wbans.collect()
            
            result_list = [row.asDict() for row in result]
            return {"top_10_wbans": result_list}

        except Exception as e:
            return {"error": f"Error processing data: {str(e)}"}
    
    def stop_spark(self):
        """
        Ferma la sessione Spark. Viene chiamato quando il lavoro è terminato.
        """
        self.spark.stop()



    
