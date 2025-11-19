"""Shared merge configuration constants that do not require app.config."""

from __future__ import annotations

from datetime import datetime

YMD_TODAY = datetime.now().strftime("%Y%m%d")

MERGED_NAMES = {
    "missed_leads": f"merged_missed_leads_{YMD_TODAY}.csv",
    "undelivered_all": f"merged_undelivered_all_{YMD_TODAY}.csv",
    "repeat_customers": f"merged_repeat_customers_{YMD_TODAY}.csv",
}

MERGE_BUCKET_DB_SPECS = {
    "missed_leads": {
        "table_name": "missed_leads",
        # dedupe by store_code + mobile_number per upsert requirements.
        "dedupe_keys": ["store_code", "mobile_number"],
        # Columns that must contain values for the row to be ingested.
        "required_columns": ["pickup_row_id", "store_code", "mobile_number"],
        "column_map": {
            ("id", "pickup_row_id", "pickup row id"): "pickup_row_id",  # numeric id in CSV
            "mobile_number": "mobile_number",
            "pickup_no": "pickup_no",
            "pickup_created_date": "pickup_created_date",
            "pickup_created_time": "pickup_created_time",
            "store_code": "store_code",
            "store_name": "store_name",
            "pickup_date": "pickup_date",
            "pickup_time": "pickup_time",
            "customer_name": "customer_name",
            "special_instruction": "special_instruction",
            "source": "source",
            "final_source": "final_source",
            "customer_type": "customer_type",
            "is_order_placed": "is_order_placed",
        },
        "coerce": {
            "pickup_row_id": "int",        # safe to keep int; change to "str" if IDs can exceed bigint
            "mobile_number": "str",        # keep phone as TEXT to preserve formatting
            "pickup_no": "str",
            "pickup_created_date": "date",
            "pickup_created_time": "str",  # keep as text; or add "time" support if you extend coercer
            "store_code": "str",
            "store_name": "str",
            "pickup_date": "date",
            "pickup_time": "str",
            "customer_name": "str",
            "special_instruction": "str",
            "source": "str",
            "final_source": "str",
            "customer_type": "str",
            "is_order_placed": "bool",
        },
    },

    "undelivered_all": {
        "table_name": "undelivered_orders",
        # order_id uniquely identifies the record across stores.
        "dedupe_keys": ["store_code", "order_id"],
        "required_columns": ["order_id"],
        "column_map": {
            ("order_id", "order_no"): "order_id",
            "order_date": "order_date",
            "store_code": "store_code",
            "store_name": "store_name",
            "taxable_amount": "taxable_amount",
            "net_amount": "net_amount",
            "service_code": "service_code",
            "mobile_no": "mobile_no",
            "status": "status",
            "customer_id": "customer_id",
            "expected_deliver_on": "expected_deliver_on",
            "actual_deliver_on": "actual_deliver_on",
        },
        "coerce": {
            "order_id": "str",
            "order_date": "date",
            "store_code": "str",
            "store_name": "str",
            "taxable_amount": "float",
            "net_amount": "float",
            "service_code": "str",
            "mobile_no": "str",
            "status": "str",
            "customer_id": "str",
            "expected_deliver_on": "date",
            "actual_deliver_on": "date",
        },
    },

    "repeat_customers": {
        "table_name": "repeat_customers",
        # Only three columns; dedupe on store+mobile. Status is 'Yes' now but may change.
        "dedupe_keys": ["store_code", "mobile_no"],
        "required_columns": ["store_code", "mobile_no"],
        "column_map": {
            "Store Code": "store_code",
            "Mobile No.": "mobile_no",
            "Status": "status",
        },
        "coerce": {
            "store_code": "str",
            "mobile_no": "str",   # CSV parsed as int, but store as TEXT to avoid issues
            "status": "str",
        },
    },
}

__all__ = ["MERGED_NAMES", "MERGE_BUCKET_DB_SPECS"]
