from flask import Flask, request
from flask_cors import CORS
from webcsv_manager import *
from Utils.load_preferences import get_preferences
import time

app = Flask(__name__)
CORS(app)


@app.route('/webcsv/register/', methods=['POST'])
def insert_csv_data():
    data = request.form

    if web_register(data):
        return {"result": "record inserted"}, 201
    else:
        return {"result": "Error: record not inserted"}, 500


params = get_preferences("microservice_conf.yaml")
app.run(host=params["host"], port=params["port"], debug=True)
