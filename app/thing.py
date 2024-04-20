import asyncio
import logging

from aiosmtplib import SMTP
from configs.settings import env_parameters

logger = logging.getLogger(__name__)


async def send_email_with_code(email: str, code: str):
    message = f"""From: {env_parameters.SENDER_EMAIL}
To: {email}
Subject: Verification code for X10

Verification code for X10: {code}"""
    async with SMTP(
        hostname=env_parameters.SMTP_SERVER,
        port=env_parameters.SMTP_PORT,
        use_tls=True,
        timeout=5,
    ) as smtp:
        await smtp.login(
            env_parameters.SMTP_USERNAME, env_parameters.SMTP_PASSWORD, timeout=5
        )
        await smtp.sendmail(env_parameters.SENDER_EMAIL, [email], message)


if __name__ == "__main__":
    asyncio.run(send_email_with_code("khromov05@gmail.com", "123"))
