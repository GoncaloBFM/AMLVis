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

    def get_nodes(self, start, size, filters, sorting):
        descend_by = '' if not sorting else (f"ORDER BY n.{sorting[0]['id']}" + (' DESC' if sorting[0]['desc'] else ''))
        pagination = f'SKIP {start} LIMIT {size}'
        where = '' if not filters else 'WHERE ' + ' and '.join(map(lambda e: f'n.{e["id"]}="{e["value"]}"', filters))
        records, _, _ = self.driver.execute_query(
            "call() { MATCH (n) " + where + " with COLLECT(DISTINCT n) as ns RETURN ns} call (ns) { return size(ns) as totalCount} call (ns) {UNWIND ns as n " + descend_by + " RETURN n " + pagination + "} return totalCount, n",
            database_=DATABASE, routing_=RoutingControl.READ,
        )

        data = [dict(record['n']) for record in records]
        count = 0 if not records else records[0]['totalCount']

        return {'data':data, 'count':count}, 200


    def get_path_data_by_node_ids(self, node_id1, node_id2, n_nodes_in_path):
        records, _, _ = self.driver.execute_query(
            f"MATCH p=((node1)-[*1..{int(n_nodes_in_path) + 1}]-(node2)) WHERE node1.id='{node_id1}' AND node2.id='{node_id2}' RETURN p",
            database_=DATABASE, routing_=RoutingControl.READ,
        )
        nodes = dict()
        edges = dict()
        for record in records:
            for node in record[0].nodes:
                nodes[node['id']] = dict(node)
            for edge in record[0].relationships:
                edges[edge['id']] = {**dict(edge), **{'type': edge.type, 'source': edge.start_node['id'], 'target': edge.end_node['id']}}
        return list(nodes.values()), list(edges.values())

if __name__ == "__main__":
    connector = Neo4JConnector()
    #print(connector.get_graph_data_by_node_ids(["a198920", "a117421", "p93740"]))
    #print(connector.search_person_by_name('John'))
    #print(connector.get_neighbors_data_by_node_ids(['a1', 'a2']))
    #print(connector.get_path_data_by_node_ids('a206691', 'a18', 2))
    print(connector.get_nodes(0, 10, [], []))
    connector.close()
