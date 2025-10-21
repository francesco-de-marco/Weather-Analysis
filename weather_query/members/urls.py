from django.urls import path
from . import views

urlpatterns = [
    path("", views.home, name="home"),  # Home page
    path("calculate-temperature", views.calculate_temperature, name="tempAvg"),
    path("station", views.query_station, name="station"),
    path("anomalus-day", views.days_anomal, name="dayAnom"),
    path("agriculture-day", views.agriculture_days, name="agricultureDays"),
    path("hourly-data", views.temp_hourly, name="tempHour"),
    path("get-wban-list", views.get_wban_list_view, name="get_wban_list"),
    path("type-query", views.query_type, name="typeQuery"),
    path("get-columns-des", views.get_columns_des, name="get-columns-des"),
    path("query_unreliable_stations", views.query_unreliable_stations, name="unreliable")
]
