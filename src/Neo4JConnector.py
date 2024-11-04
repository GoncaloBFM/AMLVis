from neo4j import GraphDatabase, RoutingControl

URL = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "password"
DATABASE = "neo4j"


class Neo4JConnector:
    def __init__(self, url=URL, user=USER, password=PASSWORD):
        self.driver = GraphDatabase.driver(url, auth=(user, password))

    def close(self):
        self.driver.close()

    def search_person_by_name(self, name):

        records, _, _ = self.driver.execute_query(
            f"MATCH (node:person) WHERE node.name =~ '.*{name}.*'  RETURN node",
            database_=DATABASE, routing_=RoutingControl.READ,
        )

        return [dict(record['node']) for record in records]

    def get_graph_data_by_node_ids(self, node_ids):

        neo4j_node_ids_list = ','.join(map(lambda x: '"' + x + '"', node_ids))

        edges_records, _, _ = self.driver.execute_query(
            f"MATCH (node1)-[edge]-(node2) WHERE node1.id IN [{neo4j_node_ids_list}] and node2.id IN [{neo4j_node_ids_list}] RETURN DISTINCT(edge);",
            database_=DATABASE, routing_=RoutingControl.READ,
        )

        node_records, _, _ = self.driver.execute_query(
            f"MATCH (node) WHERE node.id IN [{neo4j_node_ids_list}] RETURN DISTINCT(node);",
            database_=DATABASE, routing_=RoutingControl.READ,
        )

        nodes = [dict(record['node']) for record in node_records]
        edges = [dict(record['edge']) for record in edges_records]
        print(edges)
        return nodes, edges

    def get_neighbors_data_by_node_ids(self, node_ids):

        neo4j_node_ids_list = ','.join(map(lambda x: '"' + x + '"', node_ids))

        records, _, _ = self.driver.execute_query(
            f"MATCH (node1)-[edge]-(node2) WHERE node1.id IN [{neo4j_node_ids_list}] RETURN edge, node1, node2;",
            database_=DATABASE, routing_=RoutingControl.READ,
        )
        nodes = list({record['node2']['id']: dict(record['node2']) for record in records}.values())
        edges = list({record['edge']['id']: {**dict(record['edge']), **{'type': record['edge'].type, 'source': record['edge'].start_node['id'], 'target': record['edge'].end_node['id']}} for record in records}.values())
        return nodes, edges


if __name__ == "__main__":
    connector = Neo4JConnector()
    #print(connector.get_graph_data_by_node_ids(["a198920", "a117421", "p93740"]))
    #print(connector.search_person_by_name('John'))
    print(connector.get_neighbors_data_by_node_ids(['a1', 'a2']))
    connector.close()
