from flask import Flask, jsonify
import psycopg2

app = Flask(__name__)

# Fungsi untuk mendapatkan data dari view dashboard_view_lalin_daily dalam skema a_union
def get_data_from_dashboard_view_lalin_daily():
    try:
        conn = psycopg2.connect(
            dbname="tcm_datalake",
            user="konsultan",
            password="c3nt3rofd4t4",
            host="10.1.3.106",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM a_union.dashboard_view_lalin_daily')
        data = cursor.fetchall()
        conn.close()
        return data
    except Exception as e:
        return str(e)

# Route untuk mendapatkan data dari view dashboard_view_lalin_daily
@app.route('/get_dashboard_view_lalin_daily', methods=['GET'])
def get_dashboard_view_lalin_daily():
    dashboard_view_lalin_daily_data = get_data_from_dashboard_view_lalin_daily()
    if isinstance(dashboard_view_lalin_daily_data, list):
        return jsonify({ 'dashboard_view_lalin_daily_data': dashboard_view_lalin_daily_data})
    else:
        return jsonify({'error': dashboard_view_lalin_daily_data})

if __name__ == '__main__':
    # Menjalankan Flask pada host 0.0.0.0 (semua antarmuka) dan port 9011
    app.run(host='0.0.0.0', port=9011, debug=True)
