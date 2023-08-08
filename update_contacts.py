import requests
import psycopg2
from celery.schedules import crontab
from celery import Celery
from psycopg2 import sql
from config import DB_NAME, DB_USER, DB_PASSWORD, API_KEY


celery = Celery(__name__, broker='redis://localhost:6379')

celery.conf.beat_schedule = {
    'update-contacts-every-day': {
        'task': 'update_contacts.update_contacts_from_nimble',
        'schedule': crontab(hour="0", minute="0"),  # Оновлювати щодня о 00:00
        'args': (API_KEY,),
    },
}

db_params = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': '',
    'port': '',
}

@celery.task
def update_contacts_from_nimble(api_key):
    try:
        connection = psycopg2.connect(**db_params)  # Підключення до бази даних
        cursor = connection.cursor()

        headers = {
            'Authorization': f'Bearer {api_key}'
        }

        params = {
            'fields': 'first name, last name, email',
            'per_page': '50'
        }

        url = 'https://api.nimble.com/api/v1/contacts'
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            contacts = response.json()["resources"]

            for contact in contacts:
                first_name_check = contact["fields"].get('first name', None)
                first_name = first_name_check[0]['value'] if first_name_check else None

                last_name_check = contact["fields"].get('last name', None)
                last_name = last_name_check[0]['value'] if last_name_check else None

                email_check = contact["fields"].get('email', None)
                email = email_check[0]['value'] if email_check else None

                # Перевірка чи існує контакт в базі даних
                query = sql.SQL("SELECT id FROM contacts WHERE first_name = %s AND last_name = %s AND email = %s;")
                cursor.execute(query, (first_name, last_name, email))
                existing_contact = cursor.fetchone()

                if not existing_contact and (first_name or last_name or email):
                    # Додавання контакту до бази даних
                    query = sql.SQL("INSERT INTO contacts (first_name, last_name, email) VALUES (%s, %s, %s);")
                    cursor.execute(query, (first_name, last_name, email))
                    connection.commit()

    except Exception as e:
        print("Error:", e)

    finally:
        if connection:
            connection.close()