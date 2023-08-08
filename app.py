import psycopg2
from flask import Flask, request, jsonify
from psycopg2 import sql
from config import DB_NAME, DB_USER, DB_PASSWORD


app = Flask(__name__)

db_params = {
    'dbname': DB_NAME,
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': '',
    'port': '',
}


def search_contacts_db(search_query):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Використовуємо fulltext пошук для поля first name та last name
        query = sql.SQL("SELECT * FROM contacts WHERE to_tsvector('english', first_name || ' ' || last_name) @@ to_tsquery('english', %s);")
        cursor.execute(query, (search_query,))

        result = cursor.fetchall()

        return result

    except Exception as e:
        print("Error:", e)
        return []

    finally:
        if connection:
            connection.close()


@app.route('/search_contacts', methods=['GET'])
def search_contacts():
    search_query = request.args.get('query', None)
    if not search_query:
        return jsonify({"error": "Missing search query"}), 400

    contacts = search_contacts_db(search_query)

    return jsonify({"contacts": contacts}), 200


if __name__ == '__main__':
    app.run(debug=True)