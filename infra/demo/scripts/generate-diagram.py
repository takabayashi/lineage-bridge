#!/usr/bin/env python3
"""Generate demo architecture diagram as PNG using Pillow."""

from PIL import Image, ImageDraw, ImageFont
import math

# ── Canvas ──────────────────────────────────────────────────────────────────

W, H = 2200, 1500
BG = "#0f1117"
img = Image.new("RGB", (W, H), BG)
draw = ImageDraw.Draw(img)

# ── Fonts ───────────────────────────────────────────────────────────────────

try:
    font_title = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 34)
    font_subtitle = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 16)
    font_header = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 20)
    font_label = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 14)
    font_small = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 12)
    font_tiny = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 11)
except OSError:
    font_title = ImageFont.load_default()
    font_subtitle = font_title
    font_header = font_title
    font_label = font_title
    font_small = font_title
    font_tiny = font_title

# ── Colors ──────────────────────────────────────────────────────────────────

CONFLUENT_BORDER = "#2e6bdf"
AWS_BORDER = "#ff9900"
DATABRICKS_BORDER = "#ff3621"
LINEAGE_BORDER = "#a855f7"

TOPIC = "#3b82f6"
CONNECTOR = "#22c55e"
FLINK = "#06b6d4"
KSQLDB = "#f59e0b"
TABLEFLOW = "#8b5cf6"
S3 = "#ff9900"
RDS = "#2563eb"
UC = "#ff3621"
GLUE = "#ff9900"
SCHEMA_REG = "#a78bfa"
NOTEBOOK = "#ff6b6b"

TEXT = "#e2e8f0"
DIM = "#64748b"
ARROW = "#94a3b8"


def hex_to_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def fill_color(hex_color, alpha=0.15):
    r, g, b = hex_to_rgb(hex_color)
    br, bg_, bb = hex_to_rgb(BG)
    return "#{:02x}{:02x}{:02x}".format(
        int(br + (r - br) * alpha),
        int(bg_ + (g - bg_) * alpha),
        int(bb + (b - bb) * alpha),
    )


# ── Drawing helpers ─────────────────────────────────────────────────────────

def region(xy, label, color, sublabel=""):
    draw.rounded_rectangle(xy, radius=14, fill=fill_color(color, 0.06), outline=color, width=2)
    x0, y0 = xy[0], xy[1]
    draw.text((x0 + 18, y0 + 14), label, fill=color, font=font_header)
    if sublabel:
        bbox = draw.textbbox((0, 0), label, font=font_header)
        tw = bbox[2] - bbox[0]
        draw.text((x0 + 24 + tw, y0 + 18), sublabel, fill=DIM, font=font_small)


def subregion(xy, label):
    draw.rounded_rectangle(xy, radius=8, fill=fill_color("#ffffff", 0.02), outline="#374151", width=1)
    draw.text((xy[0] + 10, xy[1] + 7), label, fill="#9ca3af", font=font_small)


def box(cx, cy, w, h, label, color, sublabel=""):
    """Draw box centered at (cx, cy)."""
    x, y = cx - w // 2, cy - h // 2
    draw.rounded_rectangle((x, y, x + w, y + h), radius=7,
                           fill=fill_color(color, 0.2), outline=color, width=2)
    if sublabel:
        draw.text((cx, cy - 7), label, fill=TEXT, font=font_label, anchor="mm")
        draw.text((cx, cy + 9), sublabel, fill=DIM, font=font_tiny, anchor="mm")
    else:
        draw.text((cx, cy), label, fill=TEXT, font=font_label, anchor="mm")
    return cx, cy  # return center for arrow connections


def arrow(start, end, label="", color=ARROW, dashed=False, label_offset=(-12, 0)):
    x1, y1 = start
    x2, y2 = end
    length = math.hypot(x2 - x1, y2 - y1)
    if length < 2:
        return

    # Shorten line slightly so it doesn't overlap arrowhead
    shorten = 10
    dx, dy = (x2 - x1) / length, (y2 - y1) / length
    ex, ey = x2 - dx * shorten, y2 - dy * shorten

    if dashed:
        pos = 0
        while pos < length - shorten:
            sx = x1 + dx * pos
            sy = y1 + dy * pos
            end_p = min(pos + 8, length - shorten)
            draw.line((sx, sy, x1 + dx * end_p, y1 + dy * end_p), fill=color, width=2)
            pos += 14
    else:
        draw.line((x1, y1, ex, ey), fill=color, width=2)

    # Arrowhead
    angle = math.atan2(y2 - y1, x2 - x1)
    draw.polygon([
        (x2, y2),
        (x2 - 12 * math.cos(angle - 0.35), y2 - 12 * math.sin(angle - 0.35)),
        (x2 - 12 * math.cos(angle + 0.35), y2 - 12 * math.sin(angle + 0.35)),
    ], fill=color)

    if label:
        mx = (x1 + x2) / 2 + label_offset[1]
        my = (x1 + x2) != 0 and (y1 + y2) / 2 + label_offset[0] or y1
        draw.text((mx, my), label, fill=DIM, font=font_tiny, anchor="mm")


# ── Position helpers ────────────────────────────────────────────────────────

def top_of(cx, cy, _w=0, h=25):
    return (cx, cy - h)


def bot_of(cx, cy, _w=0, h=25):
    return (cx, cy + h)


def left_of(cx, cy, w=80, _h=0):
    return (cx - w, cy)


def right_of(cx, cy, w=80, _h=0):
    return (cx + w, cy)


# ═════════════════════════════════════════════════════════════════════════════
# LAYOUT — Three vertical columns:
#   Left: Confluent Cloud (source data + processing)
#   Center: Storage + Catalogs (S3, Glue, Databricks)
#   Right: LineageBridge (extraction, graph, UI)
# ═════════════════════════════════════════════════════════════════════════════

# ── Title ───────────────────────────────────────────────────────────────────

draw.text((W // 2, 35), "LineageBridge Demo Architecture", fill=TEXT, font=font_title, anchor="mm")
draw.text((W // 2, 65),
          "Confluent Cloud  \u2192  Tableflow  \u2192  S3  \u2192  Databricks UC / AWS Glue  |  Extracted by LineageBridge",
          fill=DIM, font=font_subtitle, anchor="mm")

# ═════════════════════════════════════════════════════════════════════════════
# ROW 1: DATA SOURCES (Datagen connectors)
# ═════════════════════════════════════════════════════════════════════════════

CX, CY = 40, 90
CW, CH = 1300, 1360
region((CX, CY, CX + CW, CY + CH), "Confluent Cloud", CONFLUENT_BORDER, "(Environment + Kafka Cluster)")

# ── Row 1: Sources ──────────────────────────────────────────────────────────
row1_y = 170
subregion((CX + 20, row1_y - 35, CX + CW - 20, row1_y + 70), "Data Sources")

src1 = box(200, row1_y + 15, 160, 50, "Datagen Orders", CONNECTOR, sublabel="Source Connector")
src2 = box(430, row1_y + 15, 180, 50, "Datagen Customers", CONNECTOR, sublabel="Source Connector")
sr1 = box(700, row1_y + 15, 170, 50, "Schema Registry", SCHEMA_REG, sublabel="Avro schemas")
sr2 = box(940, row1_y + 15, 170, 50, "Stream Catalog", SCHEMA_REG, sublabel="Tags + metadata")

# ── Row 2: Source Topics ────────────────────────────────────────────────────
row2_y = 320
subregion((CX + 20, row2_y - 35, CX + 600, row2_y + 70), "Source Topics")

t_orders = box(200, row2_y + 15, 165, 50, "orders_v2", TOPIC, sublabel="Avro, 6 partitions")
t_customers = box(430, row2_y + 15, 165, 50, "customers_v2", TOPIC, sublabel="Avro, 6 partitions")

# Sources -> Topics
arrow(bot_of(*src1), top_of(*t_orders), color=CONNECTOR)
arrow(bot_of(*src2), top_of(*t_customers), color=CONNECTOR)

# ── Row 3: Processing (Flink + ksqlDB) ─────────────────────────────────────
row3_y = 475
subregion((CX + 20, row3_y - 35, CX + 680, row3_y + 75), "Flink SQL (Streaming Compute Pool)")
subregion((CX + 720, row3_y - 35, CX + CW - 20, row3_y + 75), "ksqlDB (4 CSU)")

flink1 = box(200, row3_y + 18, 260, 50, "enrich-orders", FLINK, sublabel="CTAS: LEFT JOIN orders + customers")
flink2 = box(520, row3_y + 18, 260, 50, "order-stats", FLINK, sublabel="CTAS: TUMBLE 1min window agg")
ksql1 = box(1000, row3_y + 18, 280, 50, "high_value_orders", KSQLDB, sublabel="CSAS: WHERE price > 50")

# Topics -> Flink
arrow(bot_of(*t_orders), top_of(*flink1), label="reads", color=FLINK)
arrow(bot_of(*t_orders), top_of(*flink2), label="reads", color=FLINK, dashed=True)
arrow(bot_of(*t_customers), (flink1[0] - 50, flink1[1] - 25), color=FLINK, dashed=True)

# Topics -> ksqlDB
arrow(bot_of(*t_orders), top_of(*ksql1), label="reads", color=KSQLDB, dashed=True)

# ── Row 4: Derived Topics ──────────────────────────────────────────────────
row4_y = 630
subregion((CX + 20, row4_y - 35, CX + CW - 20, row4_y + 70), "Derived Topics")

t_enriched = box(200, row4_y + 15, 175, 50, "enriched_orders", TOPIC, sublabel="Flink output")
t_stats = box(440, row4_y + 15, 160, 50, "order_stats", TOPIC, sublabel="Flink output")
t_hvo = box(700, row4_y + 15, 185, 50, "high_value_orders", TOPIC, sublabel="ksqlDB output")

# Flink/ksqlDB -> Derived Topics
arrow(bot_of(*flink1), top_of(*t_enriched), label="writes", color=FLINK)
arrow(bot_of(*flink2), top_of(*t_stats), label="writes", color=FLINK)
arrow(bot_of(*ksql1), top_of(*t_hvo), label="writes", color=KSQLDB)

# ── Row 5: Tableflow ───────────────────────────────────────────────────────
row5_y = 790
subregion((CX + 20, row5_y - 35, CX + CW - 20, row5_y + 70), "Tableflow (BYOB \u2192 S3)")

tf_orders = box(200, row5_y + 15, 175, 50, "orders_v2", TABLEFLOW, sublabel="Delta Lake")
tf_customers = box(440, row5_y + 15, 175, 50, "customers_v2", TABLEFLOW, sublabel="Delta Lake")
tf_stats = box(700, row5_y + 15, 175, 50, "order_stats", TABLEFLOW, sublabel="Iceberg")

ci_uc = box(980, row5_y + 5, 190, 35, "UC Integration", UC)
ci_glue = box(980, row5_y + 45, 190, 35, "Glue Integration", GLUE)
prov = box(1210, row5_y + 15, 100, 50, "Provider", CONFLUENT_BORDER, sublabel="AWS IAM")

# Source Topics -> Tableflow
arrow(bot_of(*t_orders), top_of(*tf_orders), color=TABLEFLOW, dashed=True)
arrow(bot_of(*t_customers), top_of(*tf_customers), color=TABLEFLOW, dashed=True)
arrow(bot_of(*t_stats), top_of(*tf_stats), color=TABLEFLOW, dashed=True)

# ── Row 6: Postgres Sink ───────────────────────────────────────────────────
row6_y = 950
subregion((CX + 20, row6_y - 35, CX + 400, row6_y + 60), "Managed Connector")

pg_sink = box(200, row6_y + 10, 190, 45, "Postgres Sink", CONNECTOR, sublabel="enriched_orders \u2192 RDS")

# enriched_orders -> Postgres Sink
arrow(bot_of(*t_enriched), top_of(*pg_sink), label="consumes", color=CONNECTOR, dashed=True)

# ═════════════════════════════════════════════════════════════════════════════
# ROW BOTTOM-LEFT: AWS
# ═════════════════════════════════════════════════════════════════════════════

AX, AY = 40, 1100
AW, AH = 640, 350
region((AX, AY, AX + AW, AY + AH), "AWS", AWS_BORDER, "(us-east-1)")

s3 = box(160, AY + 75, 175, 55, "S3 Bucket", S3, sublabel="Delta + Iceberg files")
iam = box(400, AY + 75, 175, 55, "IAM Role", "#f59e0b", sublabel="Shared trust policy")
rds = box(160, AY + 170, 175, 55, "RDS PostgreSQL", RDS, sublabel="db: lineage_bridge")
glue = box(400, AY + 170, 175, 55, "AWS Glue", GLUE, sublabel="Iceberg catalog")

# Tableflow -> S3
arrow(bot_of(*tf_orders), top_of(*s3), label="writes", color=TABLEFLOW)
arrow(bot_of(*tf_customers), (s3[0] + 20, s3[1] - 28), color=TABLEFLOW, dashed=True)
arrow(bot_of(*tf_stats), (s3[0] + 50, s3[1] - 28), color=TABLEFLOW, dashed=True)

# Provider -> IAM
arrow(bot_of(*prov), top_of(*iam), label="assumes", color="#f59e0b", dashed=True)

# Iceberg -> Glue
arrow(bot_of(*tf_stats), top_of(*glue), label="registers", color=GLUE, dashed=True)

# Postgres Sink -> RDS
arrow(bot_of(*pg_sink), top_of(*rds), label="writes rows", color=CONNECTOR)

# ═════════════════════════════════════════════════════════════════════════════
# ROW BOTTOM-CENTER: DATABRICKS
# ═════════════════════════════════════════════════════════════════════════════

DX, DY = 720, 1100
DW, DH = 620, 350
region((DX, DY, DX + DW, DY + DH), "Databricks", DATABRICKS_BORDER, "(Unity Catalog)")

ext_loc = box(DX + 110, DY + 75, 170, 55, "External Location", UC, sublabel="s3://...tableflow/")
storage_cred = box(DX + 310, DY + 75, 175, 55, "Storage Credential", UC, sublabel="IAM role ARN")
catalog = box(DX + 510, DY + 75, 165, 55, "Catalog + Schema", UC, sublabel="lb_demo_XXXX")

notebook = box(DX + 200, DY + 175, 200, 55, "Notebook Job", NOTEBOOK, sublabel="Customer Order Summary")
grants = box(DX + 460, DY + 175, 250, 45, "Grants", DIM, sublabel="USE_CATALOG, SELECT, ALL_PRIVILEGES")

# S3 -> External Location
arrow(right_of(*s3), left_of(*ext_loc), label="reads Delta", color=UC)

# IAM -> Storage Credential
arrow(right_of(*iam), left_of(*storage_cred), label="trust", color="#f59e0b", dashed=True)

# UC Integration -> Catalog
arrow(bot_of(*ci_uc), top_of(*catalog), label="syncs tables", color=UC, dashed=True)

# Notebook reads tables
arrow(right_of(*notebook, 100), left_of(*catalog, 83), label="reads", color=NOTEBOOK)

# ═════════════════════════════════════════════════════════════════════════════
# RIGHT COLUMN: LINEAGE BRIDGE
# ═════════════════════════════════════════════════════════════════════════════

LX, LY = 1385, 90
LW, LH = 775, 1360
region((LX, LY, LX + LW, LY + LH), "LineageBridge", LINEAGE_BORDER, "(Extraction & Visualization)")

# ── Extraction Pipeline ────────────────────────────────────────────────────
epx = LX + 30
subregion((epx, LY + 50, epx + 350, LY + 480), "Extraction Pipeline")

phase_w, phase_h = 310, 48
py0 = LY + 90
py_gap = 62
e1 = box(epx + 175, py0, phase_w, phase_h, "Phase 1: KafkaAdmin", TOPIC, sublabel="Topics + Consumer Groups")
e2 = box(epx + 175, py0 + py_gap, phase_w, phase_h, "Phase 2: Connectors", CONNECTOR, sublabel="Connect + ksqlDB + Flink")
e3 = box(epx + 175, py0 + py_gap * 2, phase_w, phase_h, "Phase 3: Enrichment", SCHEMA_REG, sublabel="Schema Registry + Stream Catalog")
e4 = box(epx + 175, py0 + py_gap * 3, phase_w, phase_h, "Phase 4: Tableflow", TABLEFLOW, sublabel="Topic \u2192 Table mapping")
e5 = box(epx + 175, py0 + py_gap * 4, phase_w, phase_h, "Phase 4b: Catalogs", UC, sublabel="UC + Glue metadata enrichment")
e6 = box(epx + 175, py0 + py_gap * 5, phase_w, phase_h, "Phase 5: Metrics", "#f472b6", sublabel="Throughput per topic/connector")

# ── Watcher ─────────────────────────────────────────────────────────────────
watch = box(LX + 580, LY + 120, 220, 60, "Watcher", "#f59e0b", sublabel="REST poll 10s + debounce 30s")
arrow(bot_of(*watch, 0, 30), (epx + 330, LY + 90), label="triggers re-extraction", color="#f59e0b")

# ── Graph ───────────────────────────────────────────────────────────────────
graph = box(epx + 175, LY + 540, 310, 65, "LineageGraph", LINEAGE_BORDER, sublabel="networkx directed graph (DAG)")
arrow(bot_of(*e6), top_of(*graph, 0, 33), color=LINEAGE_BORDER)

# ── UI ──────────────────────────────────────────────────────────────────────
ui = box(epx + 175, LY + 660, 310, 65, "Streamlit UI", "#22d3ee", sublabel="vis.js interactive visualization")
arrow(bot_of(*graph, 0, 33), top_of(*ui, 0, 33), color="#22d3ee")

# ── Push to UC ──────────────────────────────────────────────────────────────
push_uc = box(LX + 580, LY + 540, 220, 60, "Push to UC", UC, sublabel="ALTER TABLE TBLPROPERTIES")
push_glue = box(LX + 580, LY + 660, 220, 60, "Push to Glue", GLUE, sublabel="update_table params")

arrow(right_of(*graph, 155), left_of(*push_uc, 110), color=UC, dashed=True)
arrow(right_of(*graph, 155), left_of(*push_glue, 110), color=GLUE, dashed=True)

# Push -> external targets
arrow(bot_of(*push_uc, 0, 30), (DX + 510, DY + 48), label="lineage metadata", color=UC, dashed=True)
arrow(bot_of(*push_glue, 0, 30), (400, AY + 143), label="table properties", color=GLUE, dashed=True)

# ── Node Types Legend (inside LB) ───────────────────────────────────────────
nty = LY + 770
subregion((epx, nty, epx + 720, nty + 200), "Supported Node Types")

node_types = [
    ("KAFKA_TOPIC", TOPIC), ("CONNECTOR", CONNECTOR), ("KSQLDB_QUERY", KSQLDB),
    ("FLINK_JOB", FLINK), ("TABLEFLOW_TABLE", TABLEFLOW), ("SCHEMA", SCHEMA_REG),
    ("UC_TABLE", UC), ("GLUE_TABLE", GLUE), ("CONSUMER_GROUP", DIM),
    ("EXTERNAL_DATASET", "#94a3b8"),
]
edge_types = [
    "PRODUCES", "CONSUMES", "TRANSFORMS", "MATERIALIZES", "HAS_SCHEMA", "MEMBER_OF",
]

for i, (name, color) in enumerate(node_types):
    col = i % 3
    row = i // 3
    nx = epx + 20 + col * 240
    ny = nty + 35 + row * 30
    draw.rectangle((nx, ny, nx + 14, ny + 14), fill=color)
    draw.text((nx + 20, ny + 7), name, fill=TEXT, font=font_small, anchor="lm")

draw.text((epx + 20, nty + 150), "Edge types:  ", fill=DIM, font=font_small)
draw.text((epx + 100, nty + 150),
          "  \u2022  ".join(edge_types), fill=TEXT, font=font_small)

# ── Arrow from Confluent to LineageBridge ───────────────────────────────────
arrow((CX + CW, 400), (LX, e1[1]), label="Confluent REST APIs", color=LINEAGE_BORDER, dashed=True)
arrow((CX + CW, 700), (LX, e4[1]), color=LINEAGE_BORDER, dashed=True)

# ═════════════════════════════════════════════════════════════════════════════
# BOTTOM LEGEND
# ═════════════════════════════════════════════════════════════════════════════

LGX, LGY = 1385, 1100
region((LGX, LGY, LGX + LW, LGY + 120), "Legend", DIM)

items = [
    ("Kafka Topic", TOPIC), ("Connector", CONNECTOR), ("Flink SQL", FLINK),
    ("ksqlDB", KSQLDB), ("Tableflow", TABLEFLOW),
    ("Unity Catalog", UC), ("AWS Glue / S3", GLUE), ("Schema Reg", SCHEMA_REG),
]
for i, (name, color) in enumerate(items):
    col = i % 4
    row = i // 4
    lx = LGX + 25 + col * 190
    ly = LGY + 45 + row * 28
    draw.rectangle((lx, ly, lx + 14, ly + 14), fill=color)
    draw.text((lx + 20, ly + 7), name, fill=TEXT, font=font_small, anchor="lm")

# ── Save ────────────────────────────────────────────────────────────────────

out = "docs/demo-architecture.png"
img.save(out, "PNG", dpi=(150, 150))
print(f"Saved: {out} ({W}x{H})")
