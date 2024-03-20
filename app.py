from flask import Flask, Response
import psycopg2
import json

app = Flask(__name__)

# Fungsi untuk mendapatkan data dari tabel at4_union dalam skema a_union
def get_data_from_at4_union():
    try:
        conn = psycopg2.connect(
            dbname="tcm_datalake",
            user="konsultan",
            password="c3nt3rofd4t4",
            host="10.1.3.106",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM a_union.at4_union')
        
        # Gunakan generator untuk mengirimkan data secara bertahap
        def generate():
            row = cursor.fetchone()
            while row:
                yield json.dumps(row) + '\n'  # Mengirimkan setiap baris sebagai JSON
                row = cursor.fetchone()
        
        return generate()
    except Exception as e:
        return str(e)

# Route untuk mendapatkan data dari tabel at4_union
@app.route('/get_at4_union', methods=['GET'])
def get_at4_union():
    at4_union_data = get_data_from_at4_union()
    return Response(at4_union_data, content_type='application/json')

if __name__ == '__main__':
    app.run(debug=True)
