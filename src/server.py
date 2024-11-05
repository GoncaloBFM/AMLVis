import json

from flask import Flask
from flask_cors import CORS

from Neo4JConnector import Neo4JConnector

app = Flask(__name__)
connector = Neo4JConnector()

@app.route('/person/<name>', methods=['GET', 'OPTIONS'])
def search_person_by_name(name):
    return json.dumps(connector.search_person_by_name(name)), 200

@app.route('/graph/<node_ids>', methods=['GET', 'OPTIONS'])
def get_graph_data_by_node_ids(node_ids):
    return json.dumps(connector.get_graph_data_by_node_ids(node_ids.split(','))), 200

@app.route('/neighbors/<node_ids>', methods=['GET', 'OPTIONS'])
def get_neighbors_data_by_node_ids(node_ids):
    return json.dumps(connector.get_neighbors_data_by_node_ids(node_ids.split(','))), 200

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=8080)