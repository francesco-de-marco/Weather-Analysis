import datetime
from django.shortcuts import render
from django.http import JsonResponse
from spark.WeatherDataProcessor import WeatherDataProcessor 
import logging
from .db_config import db, states, weather_units, wh, whD, typesD

def home(request):
    return render(request, "members/home.html")

logger = logging.getLogger(__name__)

def query_station(request):
    """
    Handle station data query and display a form if no parameters are provided.
    """
    try:
        # Recupero dei parametri della richiesta GET
        state_code = request.GET.get("state_code")
        month_start = request.GET.get("month_start")
        month_end = request.GET.get("month_end")
        info = request.GET.get("info")

        # Log dei parametri ricevuti
        logger.debug(f"Received parameters: state_code={state_code}, month_start={month_start}, month_end={month_end}, info={info}")

        # Se la richiesta è un GET senza parametri, mostra il form
        if not state_code:
            columns_des = [{"key": key, "description": value[0]} for key, value in weather_units.items()]
            months = [f"{i+1:02d}" for i in range(12)]  # Genera una lista di mesi da 01 a 12
            return render(request, "members/station.html", {"states": states, "columns": columns_des, "months": months})

        # Validazione dello stato
        if state_code not in states:
            return JsonResponse({"error": "Invalid state code"}, status=400)

        month_start = int(month_start)
        month_end = int(month_end)
        
        unit = weather_units.get(info, [None, None])[1]
        processor = WeatherDataProcessor()

        # Validazione del range dei mesi
        if not (1 <= month_start <= 12) or not (1 <= month_end <= 12) or month_start > month_end:
            return JsonResponse({"error": "Invalid month range"}, status=400)

        # Verifica della validità della colonna selezionata
        columns = [key for key in weather_units.items()]
        if info not in weather_units:
            return JsonResponse({"error": "Invalid column selected"}, status=400)

        # Esecuzione della query dei dati
        result = processor.station_query(state_code, month_start, month_end, info)
        if "error" in result:
            return JsonResponse(result, status=400)
        
        float_value = result.get("result")

        
        # Ritorno dei risultati
        return JsonResponse({"columns": columns, "result": round(float_value,2), "unit":unit})

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return JsonResponse({"error": f"An internal error occurred: {str(e)}"}, status=500)


    
def calculate_temperature(request):
    try:
        # Passa i mesi disponibili al template
        months = [f"{i+1:02d}" for i in range(len(db))]

        
        # Rendering della pagina HTML con la lista dei mesi
        if request.method == "GET" and not request.GET.get("month"):
            return render(request, "members/tempAvg.html", {"months": months})

        # Aggiungi il log per ispezionare request.GET
        logger.debug(f"request.GET content: {request.GET}")
        
        # Ottieni il mese selezionato
        selected_month = request.GET.get("month")
        if not selected_month:
            return JsonResponse({"error": "No month selected"}, status=400)

        # Controllo per la selezione del mese
        try:
            month_index = int(selected_month) - 1
        except ValueError:
            return JsonResponse({"error": "Invalid month format"}, status=400)

        if month_index < 0 or month_index >= len(db):
            return JsonResponse({"error": "Invalid month selected"}, status=400)

        # Ottieni il percorso del file dal dizionario `db`
        file_path = db[month_index][2]

        # Crea l'istanza della classe WeatherDataProcessor
        processor = WeatherDataProcessor()

        # Passa il parametro db (se è necessario)
        avg_temp_df = processor.calculate_monthly_avg_temperature(file_path)  # Aggiungi db come parametro

        # Verifica se il DataFrame contiene i dati corretti
        if avg_temp_df is None:
            return JsonResponse({"error": "No data available for the selected month"}, status=404)
    
        # Restituisci i risultati come JSON
        return JsonResponse({"months": months, "results": round(avg_temp_df, 2)})

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return JsonResponse({"error": "An internal error occurred"}, status=500)


def days_anomal(request):
    try:
        # Passa i mesi disponibili al template
        months = [f"{i+1:02d}" for i in range(len(db))]

        # Rendering della pagina HTML con la lista dei mesi
        if request.method == "GET" and not request.GET.get("month"):
            return render(request, "members/dayAnom.html", {"months": months})

        # Aggiungi il log per ispezionare request.GET
        logger.debug(f"request.GET content: {request.GET}")
        
        # Ottieni il mese selezionato
        selected_month = request.GET.get("month")
        if not selected_month:
            return JsonResponse({"error": "No month selected"}, status=400)

        # Controllo per la selezione del mese
        try:
            month_index = int(selected_month) - 1
        except ValueError:
            return JsonResponse({"error": "Invalid month format"}, status=400)

        if month_index < 0 or month_index >= len(db):
            return JsonResponse({"error": "Invalid month selected"}, status=400)

        
        processor = WeatherDataProcessor()
        cose = processor.find_anomalous_days(month_index)

        if cose is None or len(cose) == 0:
            return JsonResponse({"error": "No data available for the selected month"}, status=404)

        
        # Converto i dati in un formato JSON-friendly (lista di dizionari)
        results = [
            {
                "YearMonthDay": datetime.datetime.strptime(str(row.YearMonthDay), "%Y%m%d").strftime("%d/%m/%Y"),
                "WBAN": row.WBAN,
                "Depart": row.Depart,
                "Max5Speed": row.Max5Speed,
                "PrecipTotal": row.PrecipTotal
            }
            for row in cose
        ]

        # Restituisci la risposta JSON
        return JsonResponse({"results": results, "message": "Anomalous days found", "count": len(results)})

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return JsonResponse({"error": "An internal error occurred"}, status=500)

def agriculture_days(request):
    try:
        # Passa i mesi disponibili al template
        months = [f"{i+1:02d}" for i in range(len(db))]

        # Rendering della pagina HTML con la lista dei mesi
        if request.method == "GET" and not request.GET.get("month"):
            return render(request, "members/agricultureDays.html", {"months": months})

        # Ottieni il mese selezionato
        selected_month = request.GET.get("month")
        if not selected_month:
            return JsonResponse({"error": "No month selected"}, status=400)

        try:
            month_index = int(selected_month) - 1
        except ValueError:
            return JsonResponse({"error": "Invalid month format"}, status=400)

        if month_index < 0 or month_index >= len(db):
            return JsonResponse({"error": "Invalid month selected"}, status=400)

        # Instanzia il processore dei dati meteo
        processor = WeatherDataProcessor()

        # Ottieni la WBAN se fornita
        selected_wban = request.GET.get("wban")

        # Trova i giorni ideali per l'agricoltura per il mese selezionato
        ideal_days = processor.ideal_days_for_agriculture(month_index, selected_wban)

        if not ideal_days:
            return JsonResponse({"error": "No data available for the selected month or WBAN"}, status=404)

        # Converto i dati in un formato JSON-friendly (lista di dizionari)
        results = [
            {
                "WBAN": row.WBAN,
                "YearMonthDay": datetime.datetime.strptime(str(row.YearMonthDay), "%Y%m%d").strftime("%d/%m/%Y"),
                "Tavg": round(row.Tavg, 2) if row.Tavg is not None else None,
                "DewPoint": round(row.DewPoint, 2) if row.DewPoint is not None else None,
                "PrecipTotal": round(row.PrecipTotal, 2) if row.PrecipTotal is not None else None,
                "WetBulb": round(row.WetBulb, 2) if row.WetBulb is not None else None
            }
            for row in ideal_days
        ]

        # Restituisci la risposta JSON
        return JsonResponse({"results": results, "message": "Ideal days for agriculture found", "count": len(results)})

    except Exception as e:
        # Gestione degli errori generali
        logger.error(f"Error processing data: {str(e)}")
        return JsonResponse({"error": f"Error processing data: {str(e)}"}, status=500)


def get_wban_list_view(request):
    try:
        state_code = request.GET.get("state_code")
        if not state_code:
            return JsonResponse({"error": "State code is required"}, status=400)
        
        processor = WeatherDataProcessor()
        wban_list = processor.get_wban_list(state_code)
        
        return JsonResponse({"wban_list": wban_list})
    except Exception as e:
        return JsonResponse({"error": f"An internal error occurred: {str(e)}"}, status=500)

def temp_hourly(request):
    try:
        # Recupero dei parametri dalla richiesta GET
        state_code = request.GET.get("state_code")
        month = request.GET.get("month")
        info = request.GET.get("info")
        wban = request.GET.get("wban")
        
        if not state_code:
            wban = get_wban_list_view(request)
            columns_des = [{"key": key, "description": value[0]} for key, value in wh.items()]
            months = [f"{i+1:02d}" for i in range(12)]  # Genera una lista di mesi da 01 a 12
            return render(request, "members/tempHour.html", {"states": states, "columns": columns_des, "months": months, "wban": wban})
        
        unit = wh.get(info, [None, None])[1]

        # Verifica se i parametri necessari sono presenti
        if not all([state_code, month, info]):
            return JsonResponse({'error': 'Missing required parameters'}, status=400)
        
        # Verifica la validità dello stato
        processor = WeatherDataProcessor()  # Usa il tuo processore di dati
        result = processor.hourly_temp(state_code, month, None, info)
        
        # Se la funzione ha successo, restituiamo i risultati
        if 'error' in result:
            return JsonResponse(result, status=400)

        return JsonResponse({
            'result': result,  # I dati orari
            'unit': unit,  # Unità (potresti voler modificarlo in base alla colonna info scelta)
        })
    
    except Exception as e:
        return JsonResponse({"error": f"An internal error occurred: {str(e)}"}, status=500)
    
def get_columns_des(request):
    try:
        types = request.GET.get("types")
        if types == "1":
            columns_des = [{"key": key, "description": value[0]} for key, value in whD.items()]
        elif types == "2":
            columns_des = [{"key": key, "description": value[0]} for key, value in wh.items()]
        else:
            columns_des = [{"key": key, "description": value[0]} for key, value in weather_units.items()]

        return JsonResponse({"columns_des": columns_des})
    except Exception as e:
        return JsonResponse({"error": f"An internal error occurred: {str(e)}"}, status=500)

def query_type(request):
    try:
        # Recupero dei parametri dalla richiesta GET
        state_code = request.GET.get("state_code")
        types = request.GET.get("types")
        month = request.GET.get("month")
        info = request.GET.get("info")
        
    
        if not state_code:
            columns_des = get_columns_des(request)
            months = [f"{i+1:02d}" for i in range(12)]  # Genera una lista di mesi da 01 a 12
            
            return render(request, "members/typeQuery.html", {"states": states, "columns": columns_des, "months": months, "typesD": typesD})
        
        
        # Verifica se i parametri necessari sono presenti
        if not all([state_code, month, types, info]):
            return JsonResponse({'error': 'Missing required parameters'}, status=400)
        
        unit = wh.get(info, [None, None])[1]
        # Verifica la validità dello stato
        processor = WeatherDataProcessor()  # Usa il tuo processore di dati
        result = processor.type_query(state_code, month, types, info, unit)
        
        # Se la funzione ha successo, restituiamo i risultati
        if 'error' in result:
            return JsonResponse(result, status=400)
       
        return JsonResponse({
            'result': result,  # I dati orari
            'unit': unit,  # Unità (potresti voler modificarlo in base alla colonna info scelta)
        })
    
    except Exception as e:
        return JsonResponse({"error": f"An internal error occurred: {str(e)}"}, status=500)


def query_unreliable_stations(request):
    """
    Handle the query to retrieve the top 10 unreliable WBANs based on suspicious data ("s").
    """
    try:
        # Recupero dei parametri della richiesta GET
        state_code = request.GET.get("state_code")  # Se 'state_code' è 'tutti', non filtrare per stato
        month_start = request.GET.get("month_start")
        month_end = request.GET.get("month_end")
        info = request.GET.get("info")

        # Log dei parametri ricevuti
        logger.debug(f"Received parameters: state_code={state_code}, month_start={month_start}, month_end={month_end}, info={info}")

        # Se la richiesta è un GET senza parametri, mostra il form
        if not state_code:
            months = [f"{i+1:02d}" for i in range(12)]  # Genera una lista di mesi da 01 a 12
            return render(request, "members/unreliable.html", {"states": states, "months": months})

        # Validazione dello stato
        if state_code != "tutti" and state_code not in states:
            return JsonResponse({"error": "Invalid state code"}, status=400)

        # Validazione dei mesi
        month_start = int(month_start)
        month_end = int(month_end)
        if not (1 <= month_start <= 12) or not (1 <= month_end <= 12) or month_start > month_end:
            return JsonResponse({"error": "Invalid month range"}, status=400)

        # Creazione del processore
        processor = WeatherDataProcessor()

        # Esecuzione della query
        result = processor.unreliable_data_query(state_code, month_start, month_end)
        if "error" in result:
            return JsonResponse(result, status=400)

        # Conversione dei risultati in JSON-friendly
        unreliable_wbans = result.get("top_10_wbans", [])

        # Ora `unreliable_wbans` è la lista di dizionari e puoi iterarci sopra
        response_data = [
            {"WBAN": row["WBAN"], "suspicious_count": row["total_s_count"]} for row in unreliable_wbans
        ]

        # Ritorno dei risultati
        return JsonResponse({"unreliable_wbans": response_data}, status=200)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return JsonResponse({"error": f"An internal error occurred: {str(e)}"}, status=500)


