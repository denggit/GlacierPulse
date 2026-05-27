#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""发送本地文件到指定邮箱的命令行工具。"""
import argparse
import asyncio
import re
import sys
from datetime import datetime
from pathlib import Path


def _setup_project_path() -> Path:
    """自动定位项目根目录并加入 sys.path，支持从任意目录执行。"""
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    return project_root


PROJECT_ROOT = _setup_project_path()

from src.utils.email_sender import EmailSender  # noqa: E402


def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(description='发送指定文件到目标邮箱。')
    parser.add_argument('--file_path', required=True, help='要发送的本地文件路径')
    parser.add_argument('--email', required=True, help='目标邮箱地址')
    return parser.parse_args()


def validate_email(email: str) -> str:
    """校验邮箱参数。"""
    value = (email or '').strip()
    if not value:
        raise ValueError('邮箱参数不能为空。')

    pattern = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
    if not re.match(pattern, value):
        raise ValueError(f'邮箱格式不合法: {value}')
    return value


def validate_file_path(file_path: str) -> Path:
    """校验文件路径是否存在且为文件。"""
    path = Path(file_path).expanduser().resolve()

    if not path.exists():
        raise FileNotFoundError(f'文件不存在: {path}')
    if path.is_dir():
        raise IsADirectoryError(f'给定路径是目录而不是文件: {path}')
    return path


def format_file_size(size_bytes: int) -> str:
    """格式化文件大小，便于阅读。"""
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f'{size:.2f} {unit}' if unit != 'B' else f'{int(size)} {unit}'
        size /= 1024
    return f'{size_bytes} B'


def build_mail_content(file_path: Path) -> tuple[str, str]:
    """构建邮件主题和正文。"""
    file_size = format_file_size(file_path.stat().st_size)
    send_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subject = f'GlacierPulse 文件发送 - {file_path.name}'
    content = (
        f'文件名: {file_path.name}\n'
        f'文件绝对路径: {file_path}\n'
        f'文件大小: {file_size}\n'
        f'发送时间: {send_time}\n\n'
        '提示：此邮件由 GlacierPulse tools/send_file.py 自动发送'
    )
    return subject, content


async def _send(file_path: Path, receiver_email: str) -> bool:
    """调用 EmailSender 发送附件邮件。"""
    subject, content = build_mail_content(file_path)
    sender = EmailSender(receiver=receiver_email)
    return await sender.send_file_async(
        file_path=str(file_path),
        subject=subject,
        content=content,
    )


def main() -> int:
    """主入口：参数校验 + 发送。"""
    try:
        args = parse_args()
        receiver_email = validate_email(args.email)
        file_path = validate_file_path(args.file_path)

        success = asyncio.run(_send(file_path=file_path, receiver_email=receiver_email))
        if success:
            print(f'✅ 文件发送成功: {file_path.name} -> {receiver_email}')
            return 0

        print('❌ 文件发送失败，请检查 SMTP 配置、网络或邮箱参数。')
        return 1
    except Exception as e:
        print(f'❌ 错误: {e}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
