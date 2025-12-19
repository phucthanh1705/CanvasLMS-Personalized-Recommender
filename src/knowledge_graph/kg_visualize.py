import os
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit as st

DEFAULT_DIRS = ["data/triples", "data/processed/kg"]
NODES_FILE = None
EDGES_FILE = None

for path in DEFAULT_DIRS:
    nodes = os.path.join(path, "nodes.csv")
    edges = os.path.join(path, "edges.csv")
    if os.path.exists(nodes) and os.path.exists(edges):
        NODES_FILE, EDGES_FILE = nodes, edges
        break

if not NODES_FILE:
    st.error("❌ Không tìm thấy file nodes.csv / edges.csv trong data/triples hoặc data/processed/kg.")
    st.stop()

@st.cache_data
def load_graph():
    """Đọc nodes.csv và edges.csv → tạo NetworkX Graph"""
    nodes_df = pd.read_csv(NODES_FILE)
    edges_df = pd.read_csv(EDGES_FILE)

    G = nx.DiGraph()
    for _, row in nodes_df.iterrows():
        G.add_node(row["id"], label=row["label"], name=row.get("name", row["id"]))

    for _, row in edges_df.iterrows():
        G.add_edge(
            row["source"],
            row["target"],
            relation=row["relation"],
            score=row.get("score", None),
            mastery=row.get("mastery", None),
        )

    return G, nodes_df

def extract_subgraph(G, center_node, depth=2):
    """Trích xuất subgraph quanh 1 node theo bán kính depth"""
    if center_node not in G:
        st.warning(f"⚠️ Node '{center_node}' không tồn tại trong đồ thị.")
        return None
    nodes_to_include = nx.single_source_shortest_path_length(G, center_node, cutoff=depth).keys()
    subG = G.subgraph(nodes_to_include).copy()
    return subG

def render_pyvis_graph(G):
    """Render PyVis HTML và trả về nội dung nhúng Streamlit"""
    net = Network(
        height="850px",
        width="100%",
        directed=True,
        bgcolor="#0e1117",
        font_color="white"
    )

    net.repulsion(node_distance=200, spring_length=150, damping=0.85)

    color_map = {
        "Course": "#00BFFF",
        "Module": "#FFD700",
        "Lesson": "#FF7F7F",
        "Quiz": "#FFAA33",
        "Question": "#C0FF33",
        "Student": "#7FFF00",
        "Assignment": "#FF69B4",
        "Submission": "#ADFF2F",
        "Teacher": "#BA55D3",
    }

    rel_color = {
        "mastery_on": "#00FF99",
        "includes": "#999999",
        "has_lesson": "#CCCCCC",
        "has_quiz": "#FFA500",
        "has_question": "#FF6347",
        "attempted": "#66CDAA",
        "scored_on": "#00CED1"
    }

    for n, data in G.nodes(data=True):
        node_type = data.get("label", "")
        color = color_map.get(node_type, "#87CEFA")
        size = 20 if node_type in ["Module", "Student", "Course"] else 10
        net.add_node(
            n,
            label=data.get("name", n),
            color=color,
            title=f"🧩 {node_type}<br>ID: {n}",
            size=size
        )

    for u, v, d in G.edges(data=True):
        rel = d.get("relation", "")
        mastery = d.get("mastery", None)
        score = d.get("score", None)

        width = 1
        title = f"{rel}"
        if mastery:
            title += f" | mastery={mastery}"
        elif score:
            title += f" | score={score}"

        net.add_edge(u, v, label=rel, title=title, color=rel_color.get(rel, "#AAAAAA"), width=width)

    net.set_options("""
    {
      "nodes": {"shape": "dot", "font": {"size": 14, "face": "Tahoma"}},
      "edges": {"color": {"inherit": false}, "smooth": false},
      "physics": {
        "enabled": true,
        "solver": "forceAtlas2Based",
        "forceAtlas2Based": {"gravitationalConstant": -80, "springLength": 200},
        "stabilization": {"iterations": 150}
      },
      "interaction": {
        "hover": true,
        "navigationButtons": true,
        "keyboard": true
      }
    }
    """)

    html = net.generate_html()
    return net, html


st.set_page_config(page_title="Canvas EduKG Explorer", layout="wide")

st.title("🎓 Canvas EduKG Explorer – Subgraph Dashboard (v2)")
st.markdown("""
Công cụ trực quan hóa **Knowledge Graph** được trích xuất từ Canvas LMS.
- Chọn node trung tâm (ví dụ: `module_44`, `quiz_63`, `user_118788615`)
- Chọn **độ sâu (depth)** để mở rộng vùng tri thức
- Tô màu theo loại thực thể, độ dày theo `score/mastery`
---
""")

G, nodes_df = load_graph()

st.sidebar.header("⚙️ Cấu hình hiển thị")

all_nodes = sorted(G.nodes())
default_index = all_nodes.index("module_44") if "module_44" in all_nodes else 0
center_node = st.sidebar.selectbox("🔍 Chọn node trung tâm", options=all_nodes, index=default_index)
depth = st.sidebar.slider("🔢 Độ sâu liên kết", min_value=1, max_value=4, value=2, step=1)
show_full = st.sidebar.checkbox("🌐 Hiển thị toàn bộ đồ thị (Global KG)", value=False)


if show_full:
    st.subheader("🌐 Toàn bộ Knowledge Graph (Global KG)")
    net, html = render_pyvis_graph(G)
    st.components.v1.html(html, height=850, scrolling=True)

    if st.button("💾 Xuất HTML ra file"):
        export_path = os.path.join(os.path.dirname(NODES_FILE), "export_global_kg.html")
        net.save_graph(export_path)
        st.success(f"✅ Đã lưu file: {export_path}")

else:
    subG = extract_subgraph(G, center_node, depth)
    if subG is not None:
        st.subheader(f"🎯 Local Subgraph quanh `{center_node}` (Depth={depth})")
        st.caption(f"Số node: {len(subG.nodes())}, Số cạnh: {len(subG.edges())}")
        net, html = render_pyvis_graph(subG)
        st.components.v1.html(html, height=850, scrolling=True)

        if st.button("💾 Xuất HTML ra file"):
            export_path = os.path.join(os.path.dirname(NODES_FILE), f"export_subgraph_{center_node}.html")
            net.save_graph(export_path)
            st.success(f"✅ Đã lưu file: {export_path}")
