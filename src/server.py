import json
import sys

from flask import Flask
from flask_cors import CORS

from Neo4JConnector import Neo4JConnector

app = Flask(__name__)
if len(sys.argv) == 2 and sys.argv[1] == 'dev':
    CORS(app, origins='http://localhost:3000')
else:
    CORS(app)

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

@app.route('/path/<node_id1>,<node_id2>,<n_nodes_in_path>', methods=['GET', 'OPTIONS'])
def get_path_data_by_node_ids(node_id1, node_id2, n_nodes_in_path):
    return json.dumps(connector.get_path_data_by_node_ids(node_id1, node_id2, n_nodes_in_path)), 200

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=5000)