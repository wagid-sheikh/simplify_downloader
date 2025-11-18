from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.pdfgen import canvas


def build_undelivered_orders_pdf(output_path: str) -> None:
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4

    # Title
    title = "Undelivered Orders"
    c.setFont("Helvetica-Bold", 16)
    c.drawString(50, height - 50, title)

    # Table header
    y_start = height - 90
    headers = ["Order ID", "Order Date", "Delivery Date", "Delivered (Y/N)", "Comments"]
    x_positions = [50, 140, 230, 340, 430]

    c.setFont("Helvetica-Bold", 10)
    for x, header in zip(x_positions, headers):
        c.drawString(x, y_start, header)

    # Header underline
    c.setLineWidth(0.5)
    c.line(50, y_start - 3, width - 50, y_start - 3)

    form = c.acroForm

    # Dummy static data for 10 rows
    dummy_rows = []
    for i in range(10):
        dummy_rows.append({
            "order_id": f"ORD-{1000 + i}",
            "order_date": f"2025-11-{i + 1:02d}",
            "delivery_date": f"2025-11-{i + 2:02d}",
        })

    y = y_start - 25
    c.setFont("Helvetica", 9)

    # Read-only flag bit in PDF spec = 1
    READ_ONLY_FLAG = 1

    for idx, row in enumerate(dummy_rows, start=1):
        # Draw labels (just for visual clarity)
        c.drawString(x_positions[0], y, row["order_id"])
        c.drawString(x_positions[1], y, row["order_date"])
        c.drawString(x_positions[2], y, row["delivery_date"])

        # Read-only text fields for static data
        form.textfield(
            name=f"order_id_{idx}",
            tooltip="Order ID (read-only)",
            value=row["order_id"],
            x=x_positions[0] - 2,
            y=y - 4,
            width=80,
            height=14,
            borderWidth=0,
            textColor=colors.black,
            fieldFlags=READ_ONLY_FLAG,  # mark as read-only for compliant viewers
        )
        form.textfield(
            name=f"order_date_{idx}",
            tooltip="Order Date (read-only)",
            value=row["order_date"],
            x=x_positions[1] - 2,
            y=y - 4,
            width=80,
            height=14,
            borderWidth=0,
            textColor=colors.black,
            fieldFlags=READ_ONLY_FLAG,
        )
        form.textfield(
            name=f"delivery_date_{idx}",
            tooltip="Delivery Date (read-only)",
            value=row["delivery_date"],
            x=x_positions[2] - 2,
            y=y - 4,
            width=90,
            height=14,
            borderWidth=0,
            textColor=colors.black,
            fieldFlags=READ_ONLY_FLAG,
        )

        # Editable checkbox for Delivered (Y/N)
        form.checkbox(
            name=f"delivered_{idx}",
            tooltip="Delivered?",
            x=x_positions[3] + 5,
            y=y - 2,
            size=12,
            borderColor=colors.black,
            fillColor=colors.white,
            buttonStyle="check",
            borderWidth=1,
        )

        # Editable comments textbox
        form.textfield(
            name=f"comment_{idx}",
            tooltip="Comments",
            x=x_positions[4] - 2,
            y=y - 4,
            width=120,
            height=14,
            borderWidth=1,
            borderColor=colors.black,
            textColor=colors.black,
        )

        y -= 22  # move to next row

    c.showPage()
    c.save()


if __name__ == "__main__":
    build_undelivered_orders_pdf("undelivered-orders.pdf")
    print("Generated undelivered-orders.pdf")

