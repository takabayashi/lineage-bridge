#!/usr/bin/env python3
"""Generate demo architecture diagram as PNG using Pillow."""

import math

from PIL import Image, ImageDraw, ImageFont

# ── Canvas ──────────────────────────────────────────────────────────────────

W, H = 1800, 1100
BG = "#ffffff"
img = Image.new("RGB", (W, H), BG)
draw = ImageDraw.Draw(img)

# ── Fonts ───────────────────────────────────────────────────────────────────

try:
    font_title = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 26)
    font_region = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 13)
    font_label = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 12)
    font_small = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 10)
except OSError:
    font_title = font_region = font_label = font_small = ImageFont.load_default()

# ── Palette (white background, muted accents) ──────────────────────────────

TEXT = "#374151"
DIM = "#6b7280"
BOX_BORDER = "#d1d5db"
BOX_FILL = "#ffffff"
ARROW_COL = "#9ca3af"


def region(xy, label, bg, border, text_col):
    draw.rounded_rectangle(xy, radius=10, fill=bg, outline=border, width=1)
    draw.text((xy[0] + 14, xy[1] + 9), label, fill=text_col, font=font_region)


def box(cx, cy, w, h, label, sub=""):
    x, y = cx - w // 2, cy - h // 2
    draw.rounded_rectangle(
        (x, y, x + w, y + h), radius=5, fill=BOX_FILL, outline=BOX_BORDER, width=1
    )
    if sub:
        draw.text((cx, cy - 6), label, fill=TEXT, font=font_label, anchor="mm")
        draw.text((cx, cy + 8), sub, fill=DIM, font=font_small, anchor="mm")
    else:
        draw.text((cx, cy), label, fill=TEXT, font=font_label, anchor="mm")
    return cx, cy


def arr(s, e, label="", dash=False):
    x1, y1 = s
    x2, y2 = e
    ln = math.hypot(x2 - x1, y2 - y1)
    if ln < 2:
        return
    dx, dy = (x2 - x1) / ln, (y2 - y1) / ln
    if dash:
        p = 0
        while p < ln - 8:
            ss = min(p + 5, ln - 8)
            draw.line(
                (x1 + dx * p, y1 + dy * p, x1 + dx * ss, y1 + dy * ss),
                fill=ARROW_COL,
                width=2,
            )
            p += 10
    else:
        draw.line((x1, y1, x2 - dx * 8, y2 - dy * 8), fill=ARROW_COL, width=2)
    a = math.atan2(y2 - y1, x2 - x1)
    draw.polygon(
        [
            (x2, y2),
            (x2 - 9 * math.cos(a - 0.32), y2 - 9 * math.sin(a - 0.32)),
            (x2 - 9 * math.cos(a + 0.32), y2 - 9 * math.sin(a + 0.32)),
        ],
        fill=ARROW_COL,
    )
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2 - 10
        draw.text((mx, my), label, fill=DIM, font=font_small, anchor="mm")


# ── Title ───────────────────────────────────────────────────────────────────

draw.text(
    (W // 2, 22),
    "LineageBridge Demo Architecture",
    fill=TEXT,
    font=font_title,
    anchor="mm",
)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFLUENT CLOUD
# ═══════════════════════════════════════════════════════════════════════════════

region((25, 44, 1060, 640), "Confluent Cloud", "#eef2ff", "#93c5fd", "#1e40af")

BH = 42

# Row 1: Source Connectors
dg_o = box(200, 112, 160, BH, "Datagen Orders", "Source Connector")
dg_c = box(470, 112, 180, BH, "Datagen Customers", "Source Connector")

# Row 2: Source Topics
t_o = box(200, 200, 135, BH, "orders_v2", "Kafka Topic (Avro)")
t_c = box(470, 200, 140, BH, "customers_v2", "Kafka Topic (Avro)")

# Row 3: Stream Processing (individual jobs)
f_eo = box(200, 320, 185, BH, "enrich-orders", "Flink CTAS (JOIN)")
f_os = box(470, 320, 175, BH, "order-stats", "Flink CTAS (TUMBLE 1min)")
k_hv = box(760, 320, 185, BH, "high_value_orders", "ksqlDB CSAS (price > 50)")

# Row 4: Output Topics
dt_eo = box(200, 420, 160, BH, "enriched_orders", "Kafka Topic")
dt_os = box(470, 420, 140, BH, "order_stats", "Kafka Topic")
dt_hv = box(760, 420, 180, BH, "high_value_orders", "Kafka Topic")

# Row 5: Outputs
tf = box(300, 555, 220, BH, "Tableflow", "orders_v2, customers_v2, order_stats")
pg = box(600, 555, 170, BH, "Postgres Sink", "Managed Connector")

# ── Confluent internal arrows ───────────────────────────────────────────────

# Connectors -> Topics (straight down)
arr((dg_o[0], dg_o[1] + 21), (t_o[0], t_o[1] - 21))
arr((dg_c[0], dg_c[1] + 21), (t_c[0], t_c[1] - 21))

# Topics -> Processing
arr((t_o[0], t_o[1] + 21), (f_eo[0], f_eo[1] - 21))
arr((t_c[0], t_c[1] + 21), (f_eo[0] + 60, f_eo[1] - 21), dash=True)
arr((t_o[0] + 40, t_o[1] + 21), (f_os[0] - 30, f_os[1] - 21), dash=True)
arr((t_o[0] + 55, t_o[1] + 21), (k_hv[0] - 50, k_hv[1] - 21), dash=True)

# Processing -> Output Topics (straight down)
arr((f_eo[0], f_eo[1] + 21), (dt_eo[0], dt_eo[1] - 21))
arr((f_os[0], f_os[1] + 21), (dt_os[0], dt_os[1] - 21))
arr((k_hv[0], k_hv[1] + 21), (dt_hv[0], dt_hv[1] - 21))

# Output Topics -> Sinks
arr((dt_os[0] + 20, dt_os[1] + 21), (tf[0] + 10, tf[1] - 21))
arr((dt_eo[0] + 40, dt_eo[1] + 21), (pg[0] - 30, pg[1] - 21))

# ═══════════════════════════════════════════════════════════════════════════════
# AWS
# ═══════════════════════════════════════════════════════════════════════════════

region((25, 680, 480, 1060), "AWS", "#fffbeb", "#fcd34d", "#92400e")

s3 = box(250, 760, 200, BH, "S3 Bucket", "Delta + Iceberg files")
glu = box(140, 920, 170, BH, "AWS Glue", "order_stats (Iceberg)")
rds = box(370, 920, 150, BH, "RDS PostgreSQL")

# S3 -> Glue
arr((s3[0] - 30, s3[1] + 21), (glu[0] + 10, glu[1] - 21))

# ═══════════════════════════════════════════════════════════════════════════════
# DATABRICKS
# ═══════════════════════════════════════════════════════════════════════════════

region((510, 680, 1060, 1060), "Databricks Unity Catalog", "#fdf2f8", "#f9a8d4", "#9f1239")

uc_o = box(650, 760, 155, BH, "orders_v2", "UC Table (Delta)")
uc_c = box(850, 760, 160, BH, "customers_v2", "UC Table (Delta)")
nb = box(650, 900, 185, BH, "Notebook Job", "PySpark (every 5min)")
uc_sum = box(910, 900, 210, BH, "customer_order_summary", "UC Table (managed)")

# UC tables -> Notebook
arr((uc_o[0], uc_o[1] + 21), (nb[0], nb[1] - 21))
arr((uc_c[0] - 20, uc_c[1] + 21), (nb[0] + 50, nb[1] - 21), dash=True)

# Notebook -> summary table
arr((nb[0] + 93, nb[1]), (uc_sum[0] - 105, uc_sum[1]))

# ═══════════════════════════════════════════════════════════════════════════════
# LINEAGEBRIDGE
# ═══════════════════════════════════════════════════════════════════════════════

region((1100, 44, 1775, 1060), "LineageBridge", "#f5f3ff", "#c4b5fd", "#5b21b6")

PW = 240

# 5-phase extraction pipeline
p1 = box(1440, 115, PW, BH, "Phase 1: KafkaAdmin", "Topics + Consumer Groups")
p2 = box(1440, 195, PW, BH + 4, "Phase 2: Connectors", "Connect + ksqlDB + Flink")
p3 = box(1440, 275, PW, BH, "Phase 3: Enrichment", "Schemas + Tags + Metadata")
p4 = box(1440, 355, PW, BH, "Phase 4: Tableflow", "Topic-to-table mapping")
p4b = box(1440, 435, PW, BH + 4, "Phase 4b: Catalogs", "UC + Glue enrichment")
p5 = box(1440, 515, PW, BH, "Phase 5: Metrics", "Throughput per topic")

# Graph + UI
grp = box(1440, 670, PW, 55, "LineageGraph", "networkx directed graph")
uii = box(1440, 840, PW, 55, "Streamlit UI", "vis.js visualization")

# Phase arrows
arr((p1[0], p1[1] + 21), (p2[0], p2[1] - 23))
arr((p2[0], p2[1] + 23), (p3[0], p3[1] - 21))
arr((p3[0], p3[1] + 21), (p4[0], p4[1] - 21))
arr((p4[0], p4[1] + 21), (p4b[0], p4b[1] - 23))
arr((p4b[0], p4b[1] + 23), (p5[0], p5[1] - 21))
arr((p5[0], p5[1] + 21), (grp[0], grp[1] - 28))
arr((grp[0], grp[1] + 28), (uii[0], uii[1] - 28))

# ═══════════════════════════════════════════════════════════════════════════════
# CROSS-SECTION ARROWS
# ═══════════════════════════════════════════════════════════════════════════════

# Tableflow -> S3
arr((tf[0] - 20, tf[1] + 21), (s3[0], s3[1] - 21))

# Postgres Sink -> RDS
arr((pg[0] + 20, pg[1] + 21), (rds[0], rds[1] - 21))

# S3 -> UC tables (reads materialised Delta — clean horizontal path)
arr((s3[0] + 100, s3[1]), (uc_o[0] - 78, uc_o[1]), "reads")

# Confluent APIs -> LB Extraction
arr((1060, 300), (p2[0] - 120, p2[1]), "REST APIs", dash=True)

# ═══════════════════════════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════════════════════════

out = "docs/demo-architecture.png"
img.save(out, "PNG", dpi=(150, 150))
print(f"Saved: {out} ({W}x{H})")
