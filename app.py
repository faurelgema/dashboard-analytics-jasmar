import psycopg2
from flask import Flask, jsonify
import requests
from datetime import datetime, date

app = Flask(__name__)
last_date = None
last_volume = None  # Use 'last_volume' for clarity

# Function to get data from the dashboard view
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


# Function to send data to Power BI
def send_to_powerbi(data_to_send):
    api_url = "https://api.powerbi.com/beta/fbf770ac-3ae0-464d-9457-cb04ad0b06f5/datasets/0808e827-3b79-4fc8-9d62-786e1e7fe7ec/rows?experience=power-bi&key=vNTkIIEAgH31W%2BHaurfZUXSVXgPrHPTs0Rax07s%2B%2BLOs6wu1R9elYkJ0z7Sfwzaj2RIB735AyRurHL6%2BoUo%2FQw%3D%3D"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(api_url, json=data_to_send, headers=headers)

    if response.status_code == 200:
        print('Data successfully sent to Power BI')
    else:
        print('Failed to send data to Power BI:', response.text)

@app.route('/get_dashboard_view_lalin_daily', methods=['GET'])
def send_to_powerbi_endpoint():
    global last_date, last_volume

    current_date = date.today()

    # Reset if date changes or on first run (last_date is None)
    if current_date != last_date or last_date is None:
        last_date = current_date
        last_volume = None  # Reset last volume on date change

    dashboard_view_lalin_daily_data = get_data_from_dashboard_view_lalin_daily()
    print("Previous volume:", last_volume)
    print("Current data:", dashboard_view_lalin_daily_data)

    if isinstance(dashboard_view_lalin_daily_data, list) and dashboard_view_lalin_daily_data:
        new_volume = float(dashboard_view_lalin_daily_data[0][0])

        if last_volume is not None:
            difference = new_volume - last_volume
            if difference < 0:
                difference = 0  # Send 0 for negative differences

            formatted_data = [{"volume_lalin": difference, "date": datetime.now().strftime('%d-%b-%y')}]
            print("Difference:", difference)
            print("Formatted data:", formatted_data)
            send_to_powerbi(formatted_data)
            last_volume = new_volume  # Update last volume only after sending
        else:
            # Handle first run scenario: send full volume and update last_volume
            formatted_data = [{"volume_lalin": new_volume, "date": datetime.now().strftime('%d-%b-%y')}]
            print("Formatted data (first run):", formatted_data)
            send_to_powerbi(formatted_data)
            last_volume = new_volume

        return jsonify({'status': 'Data successfully sent to Power BI'})
    else:
        return jsonify({'status': 'No data sent to Power BI'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9011, debug=True)
