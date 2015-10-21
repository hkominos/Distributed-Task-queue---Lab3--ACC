from flask import Flask, jsonify
import json
from tasks import connect

app = Flask(__name__)

data = {"han": 0,
        "hon": 0,
        "den": 0,
        "det": 0,
        "denna":0,
        "denne": 0,
        "hen": 0,
        'alltweets': 0}


@app.route('/run')
def index():
    connect.delay()    
    return "Hello, World i will run bathc job come back in on hour!"



@app.route('/getresults', methods=['GET'])
def get_tasks():
    with open('data.txt','r') as outfile:    
        mydata=json.load(outfile)
        return jsonify(mydata)
