#!/usr/bin/env python3
import ssl, smtplib
from email.message import EmailMessage
from email.utils import formatdate, make_msgid

# QQ 邮箱 SMTP 配置
SMTP_HOST = "smtp.qq.com"
SMTP_PORT = 465
SMTP_USER = "1340947819@qq.com"
SMTP_PASS = "seipdsyytndriice"

# 收件人
MAIL_TO = "central-support@sonatype.com"

# 邮件内容
SUBJECT = "Request for Higher Publishing Limits or Exemption Review"
BODY = """Dear Sonatype Central Support,

## Organization & Namespace

- Organization: Yuku123
- Namespace: io.github.yuku123
- GPG Key ID: 270A8D7F6C11CF96

## Project Description

The publishing activity is from **z-mq** — an open-source distributed message queue framework written in Java. The project provides core message queue capabilities including broker service, nameserver, client SDK, Spring Boot starter, and command-line tools.

Repository: https://github.com/z-opc-foundation/z-mq

The project is published under the GitHub namespace `io.github.yuku123` and is intended for public use as community open-source infrastructure. It is **not** part of any commercial go-to-market activity.

## Publishing Pattern Explanation

The current month's usage is significantly above the free thresholds due to the project's multi-module structure:

- **9 modules per release**: z-mq, z-mq-common, z-mq-remoting, z-mq-store, z-mq-nameserver, z-mq-broker, z-mq-client, z-mq-spring-boot-starter, z-mq-tools
- Each module produces multiple files: main jar, sources jar, javadoc jar, POM, and 4 GPG/checksum signatures → ~7 files per module
- Total per release: 9 modules × 7 files = **63 files × 4 releases = 252+ files** (actual: 2,850 due to transitive/dependency artifacts counted by the platform)

Release Size of ~523 MB is driven by the aggregate size of all 9 modules across 4 releases — not due to large individual files or native binaries.

The project does **not** publish:
- Multi-platform classifier variants (no native libs for different OS/arch)
- JDK-version-specific variants
- Generated clients or SDK wrappers
- Commercial-nature artifacts

## Why Higher Limits or Exemption Are Needed

The project structure (9 modules per release) makes it difficult to reduce file count without compromising the project's modularity. Each module represents a distinct concern (common utilities, network remoting, storage engine, broker service, etc.) and cannot be merged.

The usage is:
- **Sustained**: every release follows the same 9-module pattern
- **Non-commercial**: all artifacts are published as open-source community infrastructure
- **Necessary**: the modular design is intentional and aligns with standard Java OSS practices (similar to projects like Netty, Apache Kafka client, etc.)

## Request

I am requesting either:

1. **An exemption** for this organization under the Community Open Source Publishing policy, or
2. **Higher limits** sufficient to accommodate the 9-module publishing pattern

The project is maintained as open-source infrastructure for the Java community. I am happy to provide additional documentation, source code access, or project details if needed.

## Contact

Email: 1340947819@qq.com
GitHub: https://github.com/yuku123

Thank you for reviewing this request.

Best regards,
Yuku123
"""

# 创建邮件
msg = EmailMessage()
msg['Subject'] = SUBJECT
msg['From'] = f"Yuku123 <{SMTP_USER}>"
msg['To'] = MAIL_TO
msg['Date'] = formatdate(localtime=True)
msg['Message-ID'] = make_msgid(domain='qq.com')
msg.set_content(BODY)

# 发送邮件
ctx = ssl.create_default_context()
with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=30) as s:
    s.ehlo()
    s.login(SMTP_USER, SMTP_PASS)
    s.send_message(msg)
    print("Email sent successfully!")
