# Supply Chain Analytics — PySpark Schema Definitions
# Sample input for SEG Demo — demonstrates PySpark schema parsing

from pyspark.sql.types import *

suppliers_schema = StructType([
    StructField("supplier_id", LongType(), False),
    StructField("supplier_name", StringType(), False),
    StructField("contact_name", StringType(), True),
    StructField("contact_email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("country", StringType(), False),
    StructField("region", StringType(), True),
    StructField("lead_time_days", IntegerType(), True),
    StructField("reliability_score", DecimalType(5, 2), True),
    StructField("payment_terms", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("onboarded_date", DateType(), True),
])

purchase_orders_schema = StructType([
    StructField("po_id", LongType(), False),
    StructField("supplier_id", LongType(), False),
    StructField("warehouse_id", StringType(), False),
    StructField("order_date", TimestampType(), False),
    StructField("expected_delivery", DateType(), True),
    StructField("actual_delivery", DateType(), True),
    StructField("status", StringType(), False),  # draft, submitted, confirmed, shipped, received, cancelled
    StructField("total_amount", DecimalType(12, 2), False),
    StructField("currency", StringType(), True),
    StructField("freight_cost", DecimalType(10, 2), True),
    StructField("buyer_id", StringType(), True),
    StructField("notes", StringType(), True),
])

po_line_items_schema = StructType([
    StructField("line_id", LongType(), False),
    StructField("po_id", LongType(), False),
    StructField("material_id", LongType(), False),
    StructField("quantity_ordered", IntegerType(), False),
    StructField("quantity_received", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), False),
    StructField("line_total", DecimalType(12, 2), False),
    StructField("quality_status", StringType(), True),  # pending, passed, failed, waived
])

materials_schema = StructType([
    StructField("material_id", LongType(), False),
    StructField("material_code", StringType(), False),
    StructField("description", StringType(), False),
    StructField("category", StringType(), True),
    StructField("unit_of_measure", StringType(), True),
    StructField("standard_cost", DecimalType(10, 2), True),
    StructField("weight_kg", DecimalType(8, 3), True),
    StructField("hazmat_class", StringType(), True),
    StructField("shelf_life_days", IntegerType(), True),
    StructField("min_order_qty", IntegerType(), True),
    StructField("reorder_point", IntegerType(), True),
])

warehouses_schema = StructType([
    StructField("warehouse_id", StringType(), False),
    StructField("warehouse_name", StringType(), False),
    StructField("location", StringType(), True),
    StructField("country", StringType(), False),
    StructField("capacity_units", IntegerType(), True),
    StructField("current_utilization", DecimalType(5, 2), True),
    StructField("manager", StringType(), True),
    StructField("is_active", BooleanType(), True),
])

inventory_schema = StructType([
    StructField("inventory_id", LongType(), False),
    StructField("material_id", LongType(), False),
    StructField("warehouse_id", StringType(), False),
    StructField("lot_number", StringType(), True),
    StructField("quantity_on_hand", IntegerType(), False),
    StructField("quantity_reserved", IntegerType(), True),
    StructField("quantity_available", IntegerType(), True),
    StructField("expiry_date", DateType(), True),
    StructField("last_count_date", DateType(), True),
    StructField("location_bin", StringType(), True),
])

shipments_schema = StructType([
    StructField("shipment_id", LongType(), False),
    StructField("po_id", LongType(), True),
    StructField("carrier", StringType(), True),
    StructField("tracking_number", StringType(), True),
    StructField("ship_date", TimestampType(), True),
    StructField("delivery_date", TimestampType(), True),
    StructField("origin_warehouse", StringType(), True),
    StructField("destination_warehouse", StringType(), True),
    StructField("status", StringType(), True),  # in_transit, delivered, delayed, lost, returned
    StructField("weight_kg", DecimalType(10, 2), True),
    StructField("freight_cost", DecimalType(10, 2), True),
    StructField("customs_cleared", BooleanType(), True),
])

quality_inspections_schema = StructType([
    StructField("inspection_id", LongType(), False),
    StructField("po_id", LongType(), False),
    StructField("material_id", LongType(), False),
    StructField("inspector_id", StringType(), True),
    StructField("inspection_date", TimestampType(), False),
    StructField("sample_size", IntegerType(), True),
    StructField("defect_count", IntegerType(), True),
    StructField("defect_rate", DecimalType(5, 4), True),
    StructField("result", StringType(), False),  # pass, fail, conditional
    StructField("notes", StringType(), True),
])
