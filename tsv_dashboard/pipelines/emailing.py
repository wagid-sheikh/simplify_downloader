from __future__ import annotations

import os
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Iterable, Sequence


@dataclass
class EmailConfig:
    to: Sequence[str]
    cc: Sequence[str]
    sender: str
    subject_template: str
    host: str
    port: int
    username: str | None
    password: str | None
    use_tls: bool


def _parse_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def load_email_config() -> EmailConfig | None:
    to = _parse_list(os.getenv("REPORT_EMAIL_TO"))
    host = os.getenv("REPORT_EMAIL_SMTP_HOST")
    port = os.getenv("REPORT_EMAIL_SMTP_PORT")
    if not to or not host or not port:
        return None
    return EmailConfig(
        to=to,
        cc=_parse_list(os.getenv("REPORT_EMAIL_CC")),
        sender=os.getenv("REPORT_EMAIL_FROM", "reports@tsv.com"),
        subject_template=os.getenv("REPORT_EMAIL_SUBJECT_TEMPLATE", "[TSV] {{pipeline_name}} – {{period_label}}"),
        host=host,
        port=int(port),
        username=os.getenv("REPORT_EMAIL_SMTP_USERNAME"),
        password=os.getenv("REPORT_EMAIL_SMTP_PASSWORD"),
        use_tls=os.getenv("REPORT_EMAIL_USE_TLS", "true").lower() == "true",
    )


def build_subject(config: EmailConfig, *, pipeline_name: str, period_label: str) -> str:
    subject = config.subject_template
    subject = subject.replace("{{pipeline_name}}", pipeline_name)
    subject = subject.replace("{{period_label}}", period_label)
    return subject


def send_report_email(
    *,
    config: EmailConfig,
    pipeline_name: str,
    period_label: str,
    summary_text: str,
    artifacts: Sequence[tuple[str, Path]],
) -> None:
    if not artifacts:
        return
    message = EmailMessage()
    message["Subject"] = build_subject(config, pipeline_name=pipeline_name, period_label=period_label)
    message["From"] = config.sender
    message["To"] = ", ".join(config.to)
    if config.cc:
        message["Cc"] = ", ".join(config.cc)
    message.set_content(summary_text)
    for store_code, path in artifacts:
        data = path.read_bytes()
        message.add_attachment(
            data,
            maintype="application",
            subtype="pdf",
            filename=path.name,
        )

    recipients = list(config.to) + list(config.cc)
    if config.use_tls:
        with smtplib.SMTP(config.host, config.port) as client:
            client.starttls()
            if config.username and config.password:
                client.login(config.username, config.password)
            client.send_message(message, to_addrs=recipients)
    else:
        with smtplib.SMTP(config.host, config.port) as client:
            if config.username and config.password:
                client.login(config.username, config.password)
            client.send_message(message, to_addrs=recipients)
