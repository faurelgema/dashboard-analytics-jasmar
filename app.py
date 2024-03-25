from flask import Flask, jsonify
import psycopg2
import requests
from datetime import datetime
import threading
import time

app = Flask(__name__)
last_value = None 
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

def send_to_powerbi(data_to_send):
    api_url = "https://api.powerbi.com/beta/fbf770ac-3ae0-464d-9457-cb04ad0b06f5/datasets/7218b92d-1e56-4d37-b372-bd94bf5e0dd6/rows?experience=power-bi&key=36OjMGaTeMYx78PXqeGd%2BGObglXl1YHhYhzYLCEqmDQybeVn4036fI4drEJ%2FOoaM9GImo9BK4X3C6NOLAKNFsg%3D%3D"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(api_url, json=data_to_send, headers=headers)

    if response.status_code == 200:
        print('Data berhasil dikirim ke Power BI')
    else:
        print('Gagal mengirim data ke Power BI')

@app.route('/get_dashboard_view_lalin_daily', methods=['GET'])
def send_to_powerbi_endpoint():
    global last_value
    dashboard_view_lalin_daily_data = get_data_from_dashboard_view_lalin_daily()
    print("Data sebelumnya: ", last_value)
    print("Data sekarang: ", dashboard_view_lalin_daily_data)

    if isinstance(dashboard_view_lalin_daily_data, list) and dashboard_view_lalin_daily_data:
        new_value = float(dashboard_view_lalin_daily_data[0][0]) 
        if last_value is not None: 
            difference = abs(new_value - float(last_value))  
            formatted_data = [{"volume_lalin": difference, "date": datetime.now().strftime('%d-%b-%y')}]
            print("Selisih: ", difference)
            print("Format Data: ", formatted_data)
            send_to_powerbi(formatted_data)  
        last_value = new_value  
        return jsonify({'status': 'Data berhasil dikirim ke Power BI'})
    else:
        return jsonify({'status': 'Tidak ada data yang dikirim ke Power BI'})

def run_flask():
    app.run(host='0.0.0.0', port=9011, debug=True)

if __name__ == '__main__':
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.start()

    while True:
        time.sleep(300)  
