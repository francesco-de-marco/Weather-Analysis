# 🚀 Weather-Analysis: Big Data Meteorological Data Analysis Platform

## Project Overview

This project implements a complete Big Data analysis solution for processing and visualizing historical meteorological data, leveraging the power of **Apache Spark** for intensive data analysis and **Django** for the web interface and request management.

The system was designed to process large datasets through parallel operations, reducing processing times compared to traditional solutions.

### 🛠️ Technologies Used

* **Backend & Data Processing:** Apache Spark (PySpark)
* **Web Framework:** Django (Python)
* **Database:** File-based configuration (as specified in `db_config`)

***

## 💾 Dataset

The project analyses meteorological data collected throughout **2013**, organized on an **hourly, daily, and monthly** basis.

* **Coverage:** Atmospheric conditions in the United States, including data on weather stations (location, unique WBAN identifier).
* **Data Structure:** Files are organized in folders named after the months and renamed to indicate the type of information they contain (e.g., daily, hourly, monthly, station, precip).
* **Standard Units:** Temperatures are in Degrees Fahrenheit or Celsius, precipitation in Inches, and wind speeds in Miles per hour.

***

## ⚙️ Installation Instructions

These instructions guide the user through configuring and launching the project.

### Prerequisites

* **Python 3.x**
* **Java Development Kit (JDK)** (required for Apache Spark)
* **Git**
* **Git LFS** (required to download the large dataset)

### Steps

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/francesco-de-marco/Weather-Analysis.git
    cd Weather-Analysis
    ```

2.  **Install Git LFS (to download large files):**
    ```bash
    git lfs install
    git lfs fetch
    ```

3.  **Create and Activate the Virtual Environment:**
    ```bash
    python -m venv venv
    .\venv\Scripts\activate  # Command for Windows (PowerShell/CMD)
    # source venv/bin/activate  # Alternative command for Linux/macOS
    ```

4.  **Install Dependencies:**
    *(Make sure you have the `requirements.txt` file in the project root)*
    ```bash
    pip install -r requirements.txt
    ```

***

## ▶️ Launch Instructions

Follow this sequence of commands from the project root folder (`Weather-Analysis`):

| Step | Command | Description |
| :--- | :--- | :--- |
| **1. Activate Environment** | `.\venv\Scripts\activate` | Activates the virtual environment on Windows. |
| **2. Navigate to App** | `cd .\weather_query\` | Enter the directory containing the `manage.py` file. |
| **3. Run Migrations** | `python manage.py migrate` | Prepares the Django database. |
| **4. Start the Server** | `python manage.py runserver` | Starts the Django web server. |

**Access:**

Once the server is running, the application will be accessible in the browser at: **`http://127.0.0.1:8000/`**

***

## 🧩 Core Features (Spark Backend)

The application exposes several APIs managed by the `Weather Data Processor` class (PySpark) for data analysis:

* **`find_anomalous_days`**: Identifies days with extreme temperature deviations and anomalous wind/precipitation conditions.
* **`ideal_days_for_agriculture`**: Finds days with optimal agricultural conditions based on Tavg, DewPoint, and absence of thunderstorms.
* **`unreliable_data_query`**: Detects the 10 weather stations with the highest frequency of missing or erroneous data.
