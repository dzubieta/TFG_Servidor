from flask import Flask
from flask_cors import CORS
import requests
from Utils.load_preferences import get_preferences

app = Flask(__name__)
CORS(app)


@app.route('/tfg/webcsvinsertion/')
def get_sensor_data():
    params = get_preferences("webapp_conf.yaml")
    web_csv_microservice_server = params["web_csv_microservice_server"]
    web_csv_microservice_port = str(params["web_csv_microservice_port"])
    response = requests.get('http://' + web_csv_microservice_server + ':' + web_csv_microservice_port + '/webcsv/register/')
    return response.content


params = get_preferences("webapp_conf.yaml")
app.run(host=params["host"], port=params["port"], debug=True)