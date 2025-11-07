# Hiển thị đồ thị bằng NetworkX, pyvis
# -*- coding: utf-8 -*-
"""
📊 Streamlit Dashboard – Canvas EduKG Explorer
Khám phá đồ thị tri thức Canvas:
 - Hiển thị toàn bộ KG hoặc subgraph động quanh 1 node.
 - Tự động sinh đồ thị PyVis (zoom, kéo, hover, xem thông tin).
"""

import os
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit as st

# =========================
# 🔧 CONFIG
# =========================
KG_DIR = "data/triples"
NODES_FILE = os.path.join(KG_DIR, "nodes.csv")
EDGES_FILE = os.path.join(KG_DIR, "edges.csv")


# =========================
# 🧩 LOAD GRAPH
# =========================
@st.cache_data
def load_graph():
    """Đọc nodes.csv và edges.csv → tạo NetworkX Graph"""
    if not os.path.exists(NODES_FILE) or not os.path.exists(EDGES_FILE):
        st.error("❌ Không tìm thấy file nodes.csv hoặc edges.csv trong data/processed/kg/")
        st.stop()

    nodes_df = pd.read_csv(NODES_FILE)
    edges_df = pd.read_csv(EDGES_FILE)

    G = nx.DiGraph()
    for _, row in nodes_df.iterrows():
        G.add_node(row["id"], label=row["label"], name=row["name"])
    for _, row in edges_df.iterrows():
        G.add_edge(row["source"], row["target"],
                   relation=row["relation"],
                   score=row.get("score", ""))
    return G, nodes_df


# =========================
# 🎯 EXTRACT SUBGRAPH
# =========================
def extract_subgraph(G, center_node, depth=2):
    """Trích xuất subgraph quanh 1 node theo bán kính depth"""
    if center_node not in G:
        st.warning(f"⚠️ Node '{center_node}' không tồn tại trong đồ thị.")
        return None
    nodes_to_include = nx.single_source_shortest_path_length(G, center_node, cutoff=depth).keys()
    subG = G.subgraph(nodes_to_include).copy()
    return subG


# =========================
# 🌐 RENDER PYVIS GRAPH
# =========================
def render_pyvis_graph(G):
    """Render PyVis HTML và trả về nội dung nhúng Streamlit"""
    net = Network(
        height="800px",
        width="100%",
        directed=True,
        bgcolor="#181818",
        font_color="white"
    )

    net.repulsion(node_distance=250, spring_length=180, damping=0.85)

    color_map = {
        "Course": "#00BFFF",
        "Module": "#1E90FF",
        "Lesson": "#FF7F7F",
        "Quiz": "#FFAA33",
        "Question": "#FFD700",
        "Student": "#7FFF00",
        "Assignment": "#FF69B4",
        "Submission": "#ADFF2F",
        "Teacher": "#BA55D3",
    }

    for n, data in G.nodes(data=True):
        node_type = data.get("label", "")
        color = color_map.get(node_type, "#87CEFA")
        size = 18 if node_type in ["Module", "Lesson", "Quiz"] else 10
        net.add_node(
            n,
            label=data.get("name", n),
            color=color,
            title=f"🧩 {node_type}",
            size=size
        )

    for u, v, d in G.edges(data=True):
        rel = d.get("relation", "")
        score = d.get("score", "")
        title = f"{rel} | score={score}" if score else rel
        net.add_edge(u, v, label=rel, title=title, color="#AAAAAA")

    net.set_options("""
    {
      "nodes": {"shape": "dot", "font": {"size": 14, "face": "Tahoma"}},
      "edges": {"color": {"color": "#999999"}, "smooth": false},
      "physics": {
        "enabled": true,
        "solver": "forceAtlas2Based",
        "forceAtlas2Based": {"gravitationalConstant": -60, "springLength": 180},
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


# =========================
# 🚀 STREAMLIT UI
# =========================
st.set_page_config(page_title="Canvas EduKG Explorer", layout="wide")

st.title("🎓 Canvas EduKG Explorer – Subgraph Dashboard")
st.markdown("""
Công cụ trực quan hóa **Knowledge Graph** được trích xuất từ Canvas LMS.
- Chọn node trung tâm (ví dụ: `module_44`, `quiz_63`, `user_118788615`)
- Chọn **độ sâu (depth)** để mở rộng vùng tri thức
- Xem **đồ thị tương tác (PyVis)** hiển thị ngay bên dưới
---
""")

# Load đồ thị
G, nodes_df = load_graph()

# Sidebar: điều khiển
st.sidebar.header("⚙️ Cấu hình hiển thị")

all_nodes = sorted(G.nodes())
default_index = all_nodes.index("module_44") if "module_44" in all_nodes else 0
center_node = st.sidebar.selectbox("🔍 Chọn node trung tâm", options=all_nodes, index=default_index)
depth = st.sidebar.slider("🔢 Độ sâu liên kết", min_value=1, max_value=4, value=2, step=1)
show_full = st.sidebar.checkbox("🌐 Hiển thị toàn bộ đồ thị (Global KG)", value=False)

# =========================
# 🖥️ MAIN PANEL
# =========================
if show_full:
    st.subheader("🌐 Toàn bộ Knowledge Graph (Global KG)")
    net, html = render_pyvis_graph(G)
    st.components.v1.html(html, height=850, scrolling=True)

    # 💾 Nút lưu file HTML
    if st.button("💾 Xuất HTML ra file"):
        export_path = os.path.join(KG_DIR, "export_global_kg.html")
        net.save_graph(export_path)
        st.success(f"✅ Đã lưu file: {export_path}")

else:
    subG = extract_subgraph(G, center_node, depth)
    if subG is not None:
        st.subheader(f"🎯 Local Subgraph quanh `{center_node}` (Depth={depth})")
        st.caption(f"Số node: {len(subG.nodes())}, Số cạnh: {len(subG.edges())}")
        net, html = render_pyvis_graph(subG)
        st.components.v1.html(html, height=850, scrolling=True)

        # 💾 Nút lưu file HTML
        if st.button("💾 Xuất HTML ra file"):
            export_path = os.path.join(KG_DIR, f"export_subgraph_{center_node}.html")
            net.save_graph(export_path)
            st.success(f"✅ Đã lưu file: {export_path}")
