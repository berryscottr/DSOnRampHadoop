class Graph:
    def __init__(self, conns):
        self.vertices = len(input_dict)
        self.connections = conns
        self.walks = self.map_reduce()

    def map_reduce(self):
        # mapper
        walks = []
        for k, v in self.connections.items():
            walk = []
            walk.append(k)
            for vals in v:
                walk.append(vals)
            for w in walk:
                for vv in self.connections[w]:
                    if vv not in walk:
                        walk.append(vv)
            walks.append(walk)
        # reducer
        walks_reduced = []
        for walk_index in range(0, len(walks)):
            walk = walks[walk_index]
            if walk not in walks_reduced:
                walks_reduced.append(walk)
        return walks_reduced


def input2dict(input_txt_lines):
    input_lines = [input_txt_line.replace("\n", "", 1) for input_txt_line in input_txt_lines]
    input_lines = [input_txt_line.split("\t") for input_txt_line in input_lines]
    input_keys = [int(val[0]) for val in input_lines]
    input_values = [val[1] for val in input_lines]
    input_values = [val.split(",") for val in input_values]
    lines_dict = {}
    val_index = 0
    for i in input_keys:
        lines_dict[i] = [int(val) for val in input_values[val_index]]
        val_index += 1
    return lines_dict


if __name__ == "__main__":
    # Read input into dictionary
    input_txt_lines = open("input.txt", "r").readlines()
    input_dict = input2dict(input_txt_lines)
    # Initialize graph object with all vertices as not visited
    g = Graph(input_dict)
    # output to file
    with open("output.txt", "w") as output_file:
        output_text = ""
        for walk in g.walks:
            walk_text = "{}\t{}\n".format(
                str(walk[0]),
                str(walk[1:]).replace("[", "", 1).replace("]", "", 1).replace(" ", "", -1)
            )
            output_text += walk_text
        output_file.write(output_text)
    print("Connected Components\n{}".format(output_text))
