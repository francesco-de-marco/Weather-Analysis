# 🚀 Weather-Analysis: Piattaforma di Analisi Dati Meteorologici Big Data

## Panoramica del Progetto

Questo progetto implementa una soluzione completa di analisi Big Data per l'elaborazione e la visualizzazione di dati meteorologici storici, sfruttando la potenza di **Apache Spark** per l'analisi intensiva dei dati e **Django** per l'interfaccia web e la gestione delle richieste.

Il sistema è stato progettato per elaborare dataset voluminosi attraverso operazioni parallele, riducendo i tempi di elaborazione rispetto alle soluzioni tradizionali.

### 🛠️ Tecnologie Utilizzate

* **Backend & Elaborazione Dati:** Apache Spark (PySpark)
* **Web Framework:** Django (Python)
* **Database:** Configurazione basata su file (come specificato in `db_config`)

---

## 💾 Dataset

Il progetto analizza dati meteorologici raccolti nel corso del **2013**, organizzati su base **oraria, giornaliera e mensile**.

* **Copertura:** Condizioni atmosferiche negli Stati Uniti, inclusi dati sulle stazioni meteorologiche (posizione, identificazione univoca WBAN).
* **Struttura Dati:** I file sono organizzati in cartelle che richiamano i mesi e sono rinominati per indicare il tipo di informazione contenuta (es. daily, hourly, monthly, station, precip).
* **Unità Standard:** Le temperature sono in Gradi Fahrenheit o Celsius, le precipitazioni in Pollici e le velocità del vento in Miglia all'ora.

---

## ⚙️ Istruzioni per l'Installazione

Queste istruzioni guidano l'utente nella configurazione e nell'avvio del progetto.

### Prerequisiti

* **Python 3.x**
* **Java Development Kit (JDK)** (necessario per Apache Spark)
* **Git**
* **Git LFS** (necessario per scaricare il dataset di grandi dimensioni)

### Passaggi

1.  **Clonazione del Repository:**
    ```bash
    git clone [https://github.com/francesco-de-marco/Weather-Analysis.git](https://github.com/francesco-de-marco/Weather-Analysis.git)
    cd Weather-Analysis
    ```

2.  **Installazione di Git LFS (per scaricare i file grandi):**
    ```bash
    git lfs install
    git lfs fetch
    ```

3.  **Creazione e Attivazione dell'Ambiente Virtuale:**
    ```bash
    python -m venv venv
    .\venv\Scripts\activate  # Comando per Windows (PowerShell/CMD)
    # source venv/bin/activate  # Comando alternativo per Linux/macOS
    ```

4.  **Installazione delle Dipendenze:**
    *(Assicurati di avere il file `requirements.txt` nella radice del progetto)*
    ```bash
    pip install -r requirements.txt
    ```

---

## ▶️ Istruzioni per l'Avvio

Segui questa sequenza di comandi dalla cartella radice del progetto (`Weather-Analysis`):

| Passo | Comando | Descrizione |
| :--- | :--- | :--- |
| **1. Attiva Ambiente** | `.\venv\Scripts\activate` | Attiva l'ambiente virtuale su Windows. |
| **2. Naviga nell'App** | `cd .\weather_query\` | Entra nella directory che contiene il file `manage.py`. |
| **3. Esegui le Migrazioni** | `python manage.py migrate` | Prepara il database Django. |
| **4. Avvia il Server** | `python manage.py runserver` | Avvia il server web di Django. |

**Accesso:**

Una volta che il server è attivo, l'applicazione sarà accessibile nel browser all'indirizzo: **`http://127.0.0.1:8000/`**

---

## 🧩 Funzionalità Principali (Backend Spark)

L'applicazione espone diverse API gestite dalla classe `Weather Data Processor` (PySpark) per l'analisi dei dati:

* **`find_anomalous_days`**: Identifica giorni con deviazioni di temperatura estreme e condizioni di vento/precipitazioni anomale.
* **`ideal_days_for_agriculture`**: Trova i giorni con condizioni ottimali per l'agricoltura in base a Tavg, DewPoint e assenza di temporali.
* **`unreliable_data_query`**: Rileva le 10 stazioni meteorologiche con la più alta frequenza di dati mancanti o errati.
