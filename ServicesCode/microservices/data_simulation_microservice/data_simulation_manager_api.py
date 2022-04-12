from flask import Flask, request
from flask_cors import CORS
from data_simulation_manager  import *
from Utils.load_preferences import get_preferences
import time

app = Flask(__name__)
CORS(app)


@app.route('/simulation/register/', methods=['POST'])
def insert_simulation_data():
    if simulation_register():
        return {"result": "record inserted"}, 201
    else:
        return {"result": "Error: record not inserted"}, 500


params = get_preferences("microservice_conf.yaml")
app.run(host=params["host"], port=params["port"], debug=True)