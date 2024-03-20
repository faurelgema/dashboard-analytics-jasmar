from flask import Flask, jsonify
import psycopg2

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
        data = cursor.fetchall()
        conn.close()
        return data
    except Exception as e:
        return str(e)

# Route untuk mendapatkan data dari tabel at4_union
@app.route('/get_at4_union', methods=['GET'])
def get_at4_union():
    at4_union_data = get_data_from_at4_union()
    if isinstance(at4_union_data, list):
        return jsonify({'at4_union_data': at4_union_data})
    else:
        return jsonify({'error': at4_union_data})

if __name__ == '__main__':
    # Menjalankan Flask pada host 0.0.0.0 (semua antarmuka) dan port 5000
    app.run(host='0.0.0.0', port=9011, debug=True)
