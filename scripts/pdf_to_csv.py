from pypdf import PdfReader
import csv


def get_field_value(field_obj):
    """
    Safely extract the '/V' (value) from a field object.
    """
    if field_obj is None:
        return ""
    # field_obj is usually a dict-like
    v = field_obj.get("/V", "")
    # Sometimes it's a NameObject starting with '/'
    if isinstance(v, str) and v.startswith("/"):
        return v[1:]
    return str(v)


def pdf_to_csv(pdf_path: str, csv_path: str, num_rows: int = 10) -> None:
    reader = PdfReader(pdf_path)

    # Get all fields from the form
    fields = reader.get_fields()

    # Prepare CSV
    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["order_id", "order_date", "delivery_date", "delivered", "comments"]
        )

        for idx in range(1, num_rows + 1):
            order_id_field = fields.get(f"order_id_{idx}")
            order_date_field = fields.get(f"order_date_{idx}")
            delivery_date_field = fields.get(f"delivery_date_{idx}")
            delivered_field = fields.get(f"delivered_{idx}")
            comment_field = fields.get(f"comment_{idx}")

            order_id = get_field_value(order_id_field)
            order_date = get_field_value(order_date_field)
            delivery_date = get_field_value(delivery_date_field)

            # Checkbox: treat Yes/On as Y, anything else as N
            delivered_raw = get_field_value(delivered_field).lower()
            if delivered_raw in ("yes", "on", "true", "1"):
                delivered = "Y"
            else:
                delivered = "N"

            comments = get_field_value(comment_field)

            writer.writerow(
                [order_id, order_date, delivery_date, delivered, comments]
            )


if __name__ == "__main__":
    pdf_to_csv("undelivered-orders.pdf", "undelivered-orders.csv", num_rows=10)
    print("Wrote undelivered-orders.csv")

