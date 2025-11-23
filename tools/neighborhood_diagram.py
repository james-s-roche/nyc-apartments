import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import igraph as ig
import plotly.graph_objects as go
from scraping.get_neighborhood_leaf_nodes import get_neighborhoods

def main():
    """
    Generates a tree diagram of the neighborhoods and saves it as an HTML file.
    """
    neighborhoods = get_neighborhoods()
    # print(neighborhoods)
    if not neighborhoods:
        print("No neighborhoods found to generate a diagram.")
        return

    # print(neighborhoods)
    # Create a mapping from neighborhood ID to its index in the list
    id_to_index = {hood['id']: i for i, hood in enumerate(neighborhoods)}
    all_ids = {hood['id'] for hood in neighborhoods}
    root_nodes = [hood for hood in neighborhoods if hood['parent_id'] not in all_ids]

    if not root_nodes:
        print("Could not determine the root of the neighborhood tree.")
        return

    # If there are multiple root nodes. Create a super root
    if len(root_nodes) > 1:
        super_root = {
            'id': -1,
            'name': 'All',
            'parent_id': -1,
            'level': -1
        }
        neighborhoods.append(super_root)
        all_ids.add(super_root['id'])
        id_to_index[super_root['id']] = len(neighborhoods) - 1
        for hood in root_nodes:
            hood['parent_id'] = super_root['id']
        root_id = super_root['id']
    else:
        # There is only one root node
        root_id = root_nodes[0]['id']

    # Create the graph
    g = ig.Graph()
    g.add_vertices(len(neighborhoods))
    
    edges = []
    for hood in neighborhoods:
        if hood['parent_id'] in id_to_index:
            parent_index = id_to_index[hood['parent_id']]
            child_index = id_to_index[hood['id']]
            edges.append((parent_index, child_index))
    
    g.add_edges(edges)

    # Set vertex labels
    g.vs['label'] = [hood['name'] for hood in neighborhoods]

    # Get the layout
    # layout = g.layout_drl(dim=3)
    layout = g.layout_reingold_tilford(root=[id_to_index[root_id]], mode='all')
    # layout = g.layout_reingold_tilford_circular(root=[id_to_index[root_id]], mode='all')
    # layout = g.layout_davidson_harel()
    # layout = g.layout_fruchterman_reingold(grid=True)
    # layout = g.layout_graphopt()
    # layout = g.layout_grid(dim=3)
    # layout = g.layout_kamada_kawai() # ok
    # layout = g.layout_lgl(root=root_id)
    # layout = g.layout_mds()
    # layout = g.layout_reingold_tilford_circular() # ok
    # layout = g.layout_umap()

    # Create the Plotly figure
    fig = go.Figure()

    # Add edges
    edge_x = []
    edge_y = []
    for edge in g.get_edgelist():
        edge_x.extend([layout[edge[0]][0], layout[edge[1]][0], None])
        edge_y.extend([layout[edge[0]][1], layout[edge[1]][1], None])
    fig.add_trace(go.Scatter(
        x=edge_x,
        y=edge_y,
        mode='lines',
        line=dict(width=1, color='black'),
        hoverinfo='none'
    ))

    # Add nodes
    node_x = [layout[i][0] for i in range(len(neighborhoods))]
    node_y = [layout[i][1] for i in range(len(neighborhoods))]
    
    fig.add_trace(go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers+text',
        text=g.vs['label'],
        textposition="bottom center",
        marker=dict(size=10, color='lightblue'),
        hoverinfo='text'
    ))

    fig.update_layout(
        title_text="Neighborhood Hierarchy",
        showlegend=False,
        xaxis=dict(showline=False, zeroline=False, showticklabels=False),
        yaxis=dict(showline=False, zeroline=False, showticklabels=False, autorange='reversed'),
        hovermode='closest'
    )

    # Save the figure
    output_file = 'img/neighborhood_diagram.html'
    fig.write_html(output_file)
    print(f"Diagram saved to {output_file}")

if __name__ == '__main__':
    main()