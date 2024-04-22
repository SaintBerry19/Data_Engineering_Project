import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import date
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import json


# Función para cargar las variables de entorno y construir la cadena de conexión
def load_configuration_and_build_connection_string():
    load_dotenv()
    config = {
        "endpoint": os.getenv("REDSHIFT_ENDPOINT"),
        "db": os.getenv("REDSHIFT_DB"),
        "user": os.getenv("REDSHIFT_USER"),
        "password": os.getenv("REDSHIFT_PASSWORD"),
        "port": os.getenv("REDSHIFT_PORT"),
    }
    conn_string = f"dbname='{config['db']}' port='{config['port']}' user='{config['user']}' password='{config['password']}' host='{config['endpoint']}'"
    return conn_string


# Función para extraer datos de la API
def extract_data():
    api_url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": "false",
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")


# Función para transformar los datos extraídos a DataFrame
def transform_data(data):
    df = pd.DataFrame(data)
    return df


# Función para crear la tabla si no existe
def create_table_if_not_exists():
    conn_string = load_configuration_and_build_connection_string()
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS crypto_markets (
                    id VARCHAR(50) PRIMARY KEY,
                    symbol VARCHAR(10),
                    name VARCHAR(50),
                    image_url VARCHAR(255),
                    current_price DECIMAL(18,2),
                    market_cap BIGINT,
                    market_cap_rank INT,
                    total_volume BIGINT,
                    high_24h DECIMAL(18,2),
                    low_24h DECIMAL(18,2),
                    price_change_24h DECIMAL(18,2),
                    price_change_percentage_24h DECIMAL(18,2),
                    last_updated TIMESTAMP,
                    inserted_at DATE
                );
                """
            )
            conn.commit()


# Funcion para insertar o actualizar datos en la tabla
def load_data(df):
    conn_string = load_configuration_and_build_connection_string()
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            # Crea la tabla temporal
            cursor.execute(
                "CREATE TEMP TEMPORARY TABLE crypto_markets_temp (LIKE crypto_markets);"
            )
            today_str = date.today().strftime("%Y-%m-%d")

            for _, row in df.iterrows():
                new_id = f"{row['symbol']}_{today_str}"
                cursor.execute(
                    """
                    INSERT INTO crypto_markets_temp (
                        id, symbol, name, image_url, current_price, market_cap, market_cap_rank,
                        total_volume, high_24h, low_24h, price_change_24h, 
                        price_change_percentage_24h, last_updated, inserted_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        new_id,
                        row["symbol"],
                        row["name"],
                        row["image"],
                        row["current_price"],
                        row["market_cap"],
                        row["market_cap_rank"],
                        row["total_volume"],
                        row["high_24h"],
                        row["low_24h"],
                        row["price_change_24h"],
                        row["price_change_percentage_24h"],
                        datetime.fromisoformat(
                            row["last_updated"].replace("Z", "+00:00")
                        ),
                        date.today(),
                    ),
                )

            # Actualizar los registros existentes en la tabla principal desde la tabla temporal
            cursor.execute(
                """
                UPDATE crypto_markets
                SET symbol = temp.symbol,
                    name = temp.name,
                    image_url = temp.image_url,
                    current_price = temp.current_price,
                    market_cap = temp.market_cap,
                    market_cap_rank = temp.market_cap_rank,
                    total_volume = temp.total_volume,
                    high_24h = temp.high_24h,
                    low_24h = temp.low_24h,
                    price_change_24h = temp.price_change_24h,
                    price_change_percentage_24h = temp.price_change_percentage_24h,
                    last_updated = temp.last_updated,
                    inserted_at = temp.inserted_at
                FROM crypto_markets_temp temp
                WHERE crypto_markets.id = temp.id;
                """
            )

            # Insertar nuevos registros que no existen en la tabla principal
            cursor.execute(
                """
                INSERT INTO crypto_markets
                SELECT temp.*
                FROM crypto_markets_temp temp
                LEFT JOIN crypto_markets ON crypto_markets.id = temp.id
                WHERE crypto_markets.id IS NULL;
                """
            )

            cursor.execute("DROP TABLE crypto_markets_temp;")
        conn.commit()


# Función para consultar los datos y devolver un DataFrame
def query_data():
    conn_string = load_configuration_and_build_connection_string()
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM crypto_markets")
            results = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            return pd.DataFrame(results, columns=colnames)
         
# Función para cargar los umbrales de alerta desde el archivo JSON
def load_alert_thresholds():
    with open('./alert_values_config.json', 'r') as json_file:
        alert_values = json.load(json_file)
    return alert_values['thresholds']

# Función para enviar correos electrónicos
def send_email(subject, body):
    # Carga las configuraciones de email
    from_email = os.getenv('EMAIL_FROM')
    to_email = os.getenv('EMAIL_TO')
    sendgrid_api_key = os.getenv('SENDGRID_API_KEY')

    # Configura el mensaje de email utilizando SendGrid
    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        html_content=body
    )
    
    # Inicia la instancia de SendGrid y envía el email
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print(f"Email sent! Status code: {response.status_code}")
    except Exception as e:
        print(e)

# Función para verificar los umbrales y enviar alertas
def verify_thresholds_and_alert():
    df = query_data()
    thresholds = load_alert_thresholds()

    for index, row in df.iterrows():
        symbol = row['symbol']
        price_change_percentage_24h = row['price_change_percentage_24h']
        min_t = thresholds[symbol]["min"]
        max_t = thresholds[symbol]["max"]

        if price_change_percentage_24h < min_t or price_change_percentage_24h > max_t:
            if price_change_percentage_24h < min_t:
                subject = f"Alert: {symbol} below threshold"
                condition = "below"
            else:
                subject = f"Alert: {symbol} above threshold"
                condition = "above"

            body = f"""
                <p>The 24h price change percentage for {symbol} is {price_change_percentage_24h}%, 
                which is {condition} the set thresholds of {min_t}% minimum and {max_t}% maximum.</p>
            """
            send_email(subject, body)
            print(f"Alert sent for {symbol}. Condition: {condition}. Price change percentage: {price_change_percentage_24h}%")

    print("Threshold verification and alerting complete.")

