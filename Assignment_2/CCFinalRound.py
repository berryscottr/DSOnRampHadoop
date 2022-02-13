class Graph:
    def __init__(self, vertices):
        self.vertices = vertices
        self.adj = [[] for _ in range(vertices)]

    def dfs_util(self, temp, v, visited):
        visited[v] = True
        temp.append(v)
        for i in self.adj[v]:
            if not visited[i]:
                temp = self.dfs_util(temp, i, visited)
        return temp

    def add_edge(self, v, w):
        self.adj[v].append(w)
        self.adj[w].append(v)

    def connected_components(self):
        visited = []
        connected_components = []
        for i in range(self.vertices):
            visited.append(False)
        for v in range(self.vertices):
            if not visited[v]:
                temp = []
                connected_components.append(self.dfs_util(temp, v, visited))
        return connected_components


def input2dict(input_txt_lines):
    input_lines = [input_txt_line.replace("\n", "", 1) for input_txt_line in input_txt_lines]
    input_lines = [input_txt_line.split("\t") for input_txt_line in input_lines]
    input_keys = [int(val[0]) for val in input_lines]
    input_values = [val[1] for val in input_lines]
    input_values = [val.split(",") for val in input_values]
    lines_dict = {}
    for i in range(0, len(input_keys)):
        lines_dict[i] = [int(val) for val in input_values[i]]
    return lines_dict


if __name__ == "__main__":
    # Read input into dictionary
    input_txt_lines = open("input.txt", "r").readlines()
    input_dict = input2dict(input_txt_lines)
    # Initialize graph object with all vertices as not visited
    g = Graph(len(input_dict))
    # Add every connection from input
    for input_key in input_dict:
        values = input_dict[input_key]
        for value in values:
            g.add_edge(input_key, value)
    # Gather connected components
    connected_components = g.connected_components()
    print("Following are connected components")
    print(connected_components)
