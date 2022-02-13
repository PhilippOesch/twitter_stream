import csv
from dataclasses import dataclass
from neo4j import GraphDatabase

from general import utils
from matplotlib import pyplot as plt
import networkx as nx
from networkx.algorithms.community import k_clique_communities
from networkx.algorithms.community import greedy_modularity_communities
from enum import Enum

driver = GraphDatabase.driver('neo4j://localhost:7687', auth=("neo4j", "test"))


class NetworkXGraph:
    def __init__(self, load_from_neo4j: bool, neo4jquery: str = None, _nodes_filepath=None, _rel_filepath=None):
        self.lable_dict = None
        self.graph = None
        if not load_from_neo4j:
            self.setup_file(_nodes_filepath, _rel_filepath)
        else:
            self.setup_neo4j(neo4jquery)

    def setup_file(self, _nodes_filepath, _rel_filepath):
        nodes = utils.load_nodes(_nodes_filepath)
        rels = utils.load_relationships(_rel_filepath)
        self.create_graph(nodes, rels)

    def setup_neo4j(self, _query):
        results = driver.session().run(_query)
        nodes: list[utils.Node] = []
        rels: list[utils.Relationship] = []
        database_nodes = list(results.graph()._nodes.values())
        for node in database_nodes:
            new_node: utils.Node = utils.Node(int(node._properties["user_id"]),
                                  node._properties["screen_name"],
                                  list(node.labels)[0])
            nodes.append(new_node)

        database_rels = list(results.graph()._relationships.values())
        for rel in database_rels:
            new_rel: utils.Relationship = utils.Relationship(rel.nodes[0]._properties["user_id"],
                                                 rel.nodes[1]._properties["user_id"],
                                                 int(rel._properties["weight"]),
                                                 float(rel._properties["polarity"]),
                                                 float(rel._properties[
                                                           "subjectivity"]) if "subjectivity" in rel._properties else "undefined",
                                                 rel.type)
            rels.append(new_rel)

        self.create_graph(nodes, rels)

    def create_graph(self, _nodes, _rels) -> None:
        self.graph = nx.MultiDiGraph()
        self.lable_dict = {}

        for node in _nodes:
            self.lable_dict[node.id] = node.screen_name
            self.graph.add_node(node.id, type=node.type)

        rel_id = 0
        for rel in _rels:
            self.graph.add_edge(rel.start_id, rel.end_id, key=rel_id,
                                weight=rel.weight, polarity=rel.polarity, subjectivity=rel.subjectivity)
            rel_id += 1

        self.graph = nx.relabel.relabel_nodes(self.graph, self.lable_dict)

        self.extract_largest_graph()

        self.print_graph_info()

    def create_and_save_springgraph(self, _filename: str, _dpi=1000, _node_color=None) -> None:
        if _node_color is None:
            _node_color = []
        position = nx.spring_layout(self.graph, scale=1, iterations=15, seed=1721)
        nx.draw_networkx(self.graph, position, with_labels=True, node_color=_node_color, node_size=3,
                         width=0.03,
                         font_size=0.04, arrowsize=2)
        plt.savefig("./data/graphs/" + _filename, dpi=_dpi)

    def export_graph(self, filepath) -> None:
        print(self.graph)
        nx.write_graphml(self.graph, filepath)

    def calc_min_max_weight(self) -> tuple[float, float]:
        rel_weights: list[int] = []
        for start, end, data in list(self.graph.edges(data=True)):
            rel_weights.append(int(data["weight"]))

        return min(rel_weights), max(rel_weights)

    def filter_polarity(self, min_polarity: float, max_polarity: float) -> None:
        for start, end, data in list(self.graph.edges(data=True)):
            if not min_polarity <= float(data["polarity"]) <= max_polarity:
                self.graph.remove_edge(start, end)

        self.extract_largest_graph()
        self.print_graph_info()

    def get_average_Polarity(self):
        polarity_sum = 0
        weight_sum = 0
        for start, end, data in list(self.graph.edges(data=True)):
            polarity_sum += float(data["polarity"])
            weight_sum += float(data["weight"])

        return polarity_sum / weight_sum

    def filter_subjectivity(self, min_subjectivity: float, max_subjectivity: float):
        rels = list(self.graph.edges(data=True))
        if rels[0][2]["subjectivity"] != "undefined":
            for start, end, data in list(self.graph.edges(data=True)):
                if not min_subjectivity <= float(data["subjectivity"]) <= max_subjectivity:
                    self.graph.remove_edge(start, end)

        self.extract_largest_graph()
        self.print_graph_info()

    def extract_largest_graph(self):
        largest_component = max(nx.weakly_connected_components(self.graph), key=len)
        self.graph = nx.MultiDiGraph(self.graph.subgraph(largest_component))

    def print_graph_info(self):
        min_weight, max_weight = self.calc_min_max_weight()
        print("Min weight:", min_weight)
        print("Max weight:", max_weight)
        print("Number of nodes: ", len(self.graph.nodes))
        print("Number of edges: ", len(self.graph.edges))


def create_and_save_springgraph(filename: str, _graph, _label_dict: dict, _dpi=1000, _node_color=None):
    if _node_color is None:
        _node_color = []
    position = nx.spring_layout(_graph, scale=1, iterations=15, seed=1721)
    nx.draw_networkx(_graph, position, with_labels=True, node_color=_node_color, node_size=3,
                     width=0.03,
                     font_size=0.04, arrowsize=2)
    plt.savefig("./data/graphs/" + filename, dpi=_dpi)


def use_greedy_modularity_communities(_graph, _colors=None):
    if _colors is None:
        _colors = []

    c = list(greedy_modularity_communities(_graph, weight="weight"))
    sorted_c = sorted(c, key=len, reverse=True)

    node_color = append_node_colors(_graph, sorted_c, _colors)

    return node_color


def use_k_clique_communities(_graph, _k=4, _colors=None):
    if _colors is None:
        _colors = []
    c = list(k_clique_communities(_graph, _k))

    sorted_c = sorted(c, key=len, reverse=True)
    print(len(c))

    node_color = append_node_colors(_graph, sorted_c, _colors)

    return node_color


def append_node_colors(_graph, comms, _colors=None):
    if _colors is None:
        _colors = []

    node_color = []

    for node in _graph:
        is_node_in_cs = False
        for idx in range(len(_colors)):
            if node in comms[idx]:
                node_color.append(_colors[idx])
                is_node_in_cs = True

        if not is_node_in_cs:
            node_color.append(_colors[len(_colors) - 1])

    return node_color


folder_path: str = "./data/import_data/"

# G = NetworkXGraph(True, folder_path + "all_users_mentions.csv",
#                   folder_path + "user_relationships_mentions.csv")
query = """MATCH (u:User)-[r]-() RETURN *"""

# G = NetworkXGraph(load_from_neo4j=True, neo4jquery=query)
G = NetworkXGraph(load_from_neo4j=False, _nodes_filepath="./data/import_data/all_users_mentions_textblob.csv",
                  _rel_filepath="./data/import_data/user_relationships_mentions_textblob.csv")

print("average Polarity:", G.get_average_Polarity())
G.export_graph("./data/exported_graph_tables/graph_mentions_textblob.graphml")

# G.filter_polarity(0.5, 1)
# G.export_graph("./data/exported_graph_tables/graph_polarity_positive.graphml")
