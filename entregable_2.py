import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import date


# Función para cargar las variables de entorno
def load_configuration():
    load_dotenv()
    return {
        "endpoint": os.getenv("REDSHIFT_ENDPOINT"),
        "db": os.getenv("REDSHIFT_DB"),
        "user": os.getenv("REDSHIFT_USER"),
        "password": os.getenv("REDSHIFT_PASSWORD"),
        "port": os.getenv("REDSHIFT_PORT"),
    }


# Función para construir la cadena de conexión
def build_connection_string(config):
    return f"dbname='{config['db']}' port='{config['port']}' user='{config['user']}' password='{config['password']}' host='{config['endpoint']}'"


# Función para crear la tabla si no existe
def create_table_if_not_exists(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS crypto_markets (
                id VARCHAR(50) PRIMARY KEY ,
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
        connection.commit()
    return True


# Funcion para insertar o actualizar datos en la tabla
def upsert_data_into_table(connection, data):
    with connection.cursor() as cursor:
        # Crear una tabla temporal para almacenar los datos entrantes
        cursor.execute("CREATE TEMP TABLE crypto_markets_temp (LIKE crypto_markets);")

        for item in data:
            cursor.execute(
                """
                INSERT INTO crypto_markets_temp (
                    id, symbol, name, image_url, current_price, market_cap, market_cap_rank,
                    total_volume, high_24h, low_24h, price_change_24h, 
                    price_change_percentage_24h, last_updated, inserted_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    item["id"],
                    item["symbol"],
                    item["name"],
                    item["image"],
                    item["current_price"],
                    item["market_cap"],
                    item["market_cap_rank"],
                    item["total_volume"],
                    item["high_24h"],
                    item["low_24h"],
                    item["price_change_24h"],
                    item["price_change_percentage_24h"],
                    datetime.fromisoformat(item["last_updated"].replace("Z", "+00:00")),
                    date.today(),
                ),
            )

        # Actualizar los registros existentes
        cursor.execute(
            """
            UPDATE crypto_markets
            SET symbol = crypto_markets_temp.symbol,
                name = crypto_markets_temp.name,
                image_url = crypto_markets_temp.image_url,
                current_price = crypto_markets_temp.current_price,
                market_cap = crypto_markets_temp.market_cap,
                market_cap_rank = crypto_markets_temp.market_cap_rank,
                total_volume = crypto_markets_temp.total_volume,
                high_24h = crypto_markets_temp.high_24h,
                low_24h = crypto_markets_temp.low_24h,
                price_change_24h = crypto_markets_temp.price_change_24h,
                price_change_percentage_24h = crypto_markets_temp.price_change_percentage_24h,
                last_updated = crypto_markets_temp.last_updated,
                inserted_at = crypto_markets_temp.inserted_at
            FROM crypto_markets_temp
            WHERE crypto_markets.id = crypto_markets_temp.id;
        """
        )

        # Insertar nuevos registros
        cursor.execute(
            """
            INSERT INTO crypto_markets
            SELECT * FROM crypto_markets_temp
            WHERE id NOT IN (SELECT id FROM crypto_markets);
        """
        )

        cursor.execute("DROP TABLE crypto_markets_temp;")
        connection.commit()


# Función para consultar los datos y devolver un DataFrame
def query_data(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM crypto_markets")
        results = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        return pd.DataFrame(results, columns=colnames)


# Función principal para ejecutar los procesos
# Función principal para ejecutar los procesos
def main():
    config = load_configuration()
    conn_string = build_connection_string(config)

    # Uso del manejador de contexto para asegurar el cierre de la conexión
    try:
        with psycopg2.connect(conn_string) as conn:
            # Crear la tabla si no existe y confirmar su creación
            if create_table_if_not_exists(conn):
                print("La tabla se ha creado o ya existe.")

            # Llenar la tabla con los datos de la API
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
                data = response.json()
                if data:
                    upsert_data_into_table(conn, data)
                    print("Datos insertados o actualizados correctamente.")
                else:
                    print("La respuesta de la API no contiene datos.")
            else:
                print(f"Error al recuperar los datos: {response.status_code}")

            # Consultar y mostrar los datos insertados/actualizados
            df = query_data(conn)
            if not df.empty:
                print(df.head())
            else:
                print("No hay datos para mostrar.")

    except psycopg2.Error as e:
        print(f"Error de base de datos: {e}")
    except Exception as e:
        print(f"Error general: {e}")


if __name__ == "__main__":
    main()
