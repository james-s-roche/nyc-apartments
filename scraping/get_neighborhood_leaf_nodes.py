from collections import defaultdict

from database.utils import get_db_connection

def get_neighborhoods():
    """Fetches all neighborhoods from the database."""
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, name, level, parent_id FROM neighborhoods")
    neighborhoods = cursor.fetchall()
    cursor.close()
    conn.close()
    return neighborhoods

def build_tree(neighborhoods):
    """Builds a tree structure from a flat list of neighborhoods."""
    tree = defaultdict(list)
    for hood in neighborhoods:
        tree[hood['parent_id']].append(hood)
    return tree

def get_leaf_nodes(tree, neighborhoods):
    """Identifies the leaf nodes in the neighborhood tree."""
    parent_ids = {node['parent_id'] for node in neighborhoods if node['parent_id'] is not None}
    leaf_nodes = [node for node in neighborhoods if node['id'] not in parent_ids]
    return leaf_nodes

def get_leaf_neighborhoods():
    """Fetches neighborhoods and returns the leaf nodes."""
    neighborhoods = get_neighborhoods()
    if not neighborhoods:
        return []

    tree = build_tree(neighborhoods)
    return get_leaf_nodes(tree, neighborhoods)

def main():
    """Main function to get and print leaf nodes."""
    leaf_nodes = get_leaf_neighborhoods()
    if not leaf_nodes:
        print("No leaf nodes found.")
        return
    
    for node in leaf_nodes:
        print(node['name'])

if __name__ == "__main__":
    main()
