"""
alerts/line_alert.py — LINE Messaging API 推播
使用 LINE Messaging API 的 push message endpoint 發送警報。
"""

import json
import logging
import requests

import config

logger = logging.getLogger(__name__)


def send_line_alert(message: str) -> bool:
    """
    透過 LINE Messaging API 發送單則推播訊息。

    Parameters
    ----------
    message : str（上限 5000 字元）

    Returns
    -------
    bool : True = 發送成功
    """
    if not config.LINE_CHANNEL_ACCESS_TOKEN:
        logger.error("LINE_CHANNEL_ACCESS_TOKEN 未設定")
        return False
    if not config.LINE_USER_ID:
        logger.error("LINE_USER_ID 未設定")
        return False

    if len(message) > 5000:
        message = message[:4950] + "\n\n... (訊息過長已截斷)"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.LINE_CHANNEL_ACCESS_TOKEN}",
    }

    payload = {
        "to": config.LINE_USER_ID,
        "messages": [{"type": "text", "text": message}],
    }

    try:
        resp = requests.post(
            config.LINE_PUSH_URL,
            headers=headers,
            data=json.dumps(payload),
            timeout=10,
        )
        if resp.status_code == 200:
            logger.info("✅ LINE 推播成功")
            return True
        else:
            logger.error(f"❌ LINE 推播失敗: HTTP {resp.status_code}, {resp.text}")
            return False
    except requests.exceptions.Timeout:
        logger.error("❌ LINE 推播逾時")
        return False
    except Exception as e:
        logger.error(f"❌ LINE 推播異常: {e}")
        return False


def send_multi_messages(messages: list) -> bool:
    """
    發送多段訊息（LINE 單次最多 5 則）。
    """
    if not messages:
        return True

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.LINE_CHANNEL_ACCESS_TOKEN}",
    }

    success = True
    for i in range(0, len(messages), 5):
        batch = messages[i:i + 5]
        line_msgs = []
        for msg in batch:
            if len(msg) > 5000:
                msg = msg[:4950] + "\n\n... (截斷)"
            line_msgs.append({"type": "text", "text": msg})

        payload = {"to": config.LINE_USER_ID, "messages": line_msgs}

        try:
            resp = requests.post(
                config.LINE_PUSH_URL,
                headers=headers,
                data=json.dumps(payload),
                timeout=10,
            )
            if resp.status_code != 200:
                logger.error(f"LINE 批次推播失敗: {resp.status_code}")
                success = False
        except Exception as e:
            logger.error(f"LINE 批次推播異常: {e}")
            success = False

    return success
