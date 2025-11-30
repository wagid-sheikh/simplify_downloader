from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class MissedLead(Base):
    __tablename__ = "missed_leads"
    __table_args__ = (
        UniqueConstraint("store_code", "mobile_number", name="uq_missed_leads_store_mobile"),
    )

    pickup_row_id: Mapped[int | None] = mapped_column(Integer, primary_key=True, autoincrement=False)
    mobile_number: Mapped[str] = mapped_column(String, nullable=False)
    pickup_no: Mapped[str | None] = mapped_column(String)
    pickup_created_date: Mapped[Date | None] = mapped_column(Date)
    pickup_created_time: Mapped[str | None] = mapped_column(String)
    store_code: Mapped[str] = mapped_column(String, nullable=False)
    store_name: Mapped[str | None] = mapped_column(String)
    pickup_date: Mapped[Date | None] = mapped_column(Date)
    pickup_time: Mapped[str | None] = mapped_column(String)
    customer_name: Mapped[str | None] = mapped_column(String)
    special_instruction: Mapped[str | None] = mapped_column(String)
    source: Mapped[str | None] = mapped_column(String)
    final_source: Mapped[str | None] = mapped_column(String)
    customer_type: Mapped[str | None] = mapped_column(String)
    is_order_placed: Mapped[bool | None] = mapped_column(Boolean)
    run_id: Mapped[str | None] = mapped_column(String(64))
    run_date: Mapped[Date | None] = mapped_column(Date)


class UndeliveredOrder(Base):
    __tablename__ = "undelivered_orders"
    __table_args__ = (
        PrimaryKeyConstraint("order_id", "store_code", name="pk_undelivered_order"),
    )

    order_id: Mapped[str] = mapped_column(String, nullable=False)
    order_date: Mapped[Date | None] = mapped_column(Date)
    store_code: Mapped[str] = mapped_column(String, nullable=False)
    store_name: Mapped[str | None] = mapped_column(String)
    taxable_amount: Mapped[float | None] = mapped_column(Float)
    net_amount: Mapped[float | None] = mapped_column(Float)
    service_code: Mapped[str | None] = mapped_column(String)
    mobile_no: Mapped[str | None] = mapped_column(String)
    status: Mapped[str | None] = mapped_column(String)
    customer_id: Mapped[str | None] = mapped_column(String)
    expected_deliver_on: Mapped[Date | None] = mapped_column(Date)
    actual_deliver_on: Mapped[Date | None] = mapped_column(Date)
    run_id: Mapped[str | None] = mapped_column(String(64))
    run_date: Mapped[Date | None] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class RepeatCustomer(Base):
    __tablename__ = "repeat_customers"
    __table_args__ = (
        UniqueConstraint("store_code", "mobile_no", name="uq_repeat_store_mobile"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    store_code: Mapped[str] = mapped_column(String, nullable=False)
    mobile_no: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str | None] = mapped_column(String)
    run_id: Mapped[str | None] = mapped_column(String(64))
    run_date: Mapped[Date | None] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class NonPackageOrder(Base):
    __tablename__ = "nonpackage_orders"
    __table_args__ = (
        UniqueConstraint("store_code", "mobile_no", name="uq_nonpackage_store_mobile"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    store_code: Mapped[str] = mapped_column(String, nullable=False)
    store_name: Mapped[str | None] = mapped_column(String)
    mobile_no: Mapped[str] = mapped_column(String, nullable=False)
    taxable_amount: Mapped[float | None] = mapped_column(Float)
    order_date: Mapped[Date] = mapped_column(Date, nullable=False)
    expected_delivery_date: Mapped[Date | None] = mapped_column(Date)
    actual_delivery_date: Mapped[Date | None] = mapped_column(Date)
    run_id: Mapped[str | None] = mapped_column(String(64))
    run_date: Mapped[Date | None] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


BUCKET_MODEL_MAP = {
    "missed_leads": MissedLead,
    "undelivered_all": UndeliveredOrder,
    "repeat_customers": RepeatCustomer,
    "nonpackage_all": NonPackageOrder,
}
