# bot.py
import os
import re
import asyncio
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# =========================================================
# CONFIG
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN")

NETWORK = "mainnet"
WALLET_VERSION = "V5R1"

# =========================================================
# VANITY RULES
# =========================================================

def normalize(address: str) -> str:
    return address.replace(":", "").lower()


def contains_pattern(address: str, pattern: str) -> bool:
    return pattern.lower() in normalize(address)


def has_repeated_chars(address: str, count: int = 4) -> bool:
    s = normalize(address)

    for i in range(len(s) - count + 1):
        if len(set(s[i:i + count])) == 1:
            return True

    return False


def is_palindrome_part(address: str, length: int = 5) -> bool:
    s = normalize(address)

    for i in range(len(s) - length + 1):
        part = s[i:i + length]

        if part == part[::-1]:
            return True

    return False


def is_round(address: str) -> bool:
    s = normalize(address)

    # AAAA
    if has_repeated_chars(address, 4):
        return True

    # AABB
    for i in range(len(s) - 3):
        if (
            s[i] == s[i + 1]
            and s[i + 2] == s[i + 3]
        ):
            return True

    # ABAB
    for i in range(len(s) - 3):
        if (
            s[i] == s[i + 2]
            and s[i + 1] == s[i + 3]
        ):
            return True

    # palindrome
    return is_palindrome_part(address, 5)


# =========================================================
# ADDRESS GENERATOR
# =========================================================

def generate_wallet_locally():
    """
    این تابع باید روی دستگاه/سیستم امن کاربر به
    کتابخانه TON متصل شود.

    عمداً mnemonic/private-key از این تابع به ربات
    ارسال نمی‌شود.
    """

    raise NotImplementedError(
        "Generate the V5R1 wallet locally and return only "
        "the public address to the bot."
    )


# =========================================================
# VANITY SEARCH
# =========================================================

def vanity_search(pattern=None, attempts=500):

    for attempt in range(1, attempts + 1):

        try:
            address = generate_wallet_locally()

        except NotImplementedError:
            return {
                "error":
                "Wallet generation must run locally."
            }

        if pattern and contains_pattern(
            address,
            pattern
        ):
            return {
                "address": address,
                "attempt": attempt
            }

        if is_round(address):
            return {
                "address": address,
                "attempt": attempt
            }

    return None


# =========================================================
# TELEGRAM COMMAND
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        "🔐 TON Wallet\n\n"
        "Network: MAINNET\n"
        "Wallet: V5R1\n\n"
        "دستور Vanity:\n"
        "/vanity TON 500\n\n"
        "Seed و Private Key هیچ‌وقت "
        "به ربات ارسال نمی‌شود."
    )


async def vanity(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if len(context.args) < 1:

        await update.message.reply_text(
            "فرمت:\n"
            "/vanity PATTERN ATTEMPTS\n\n"
            "مثال:\n"
            "/vanity TON 500"
        )

        return

    pattern = context.args[0]

    attempts = 500

    if len(context.args) >= 2:

        try:
            attempts = int(
                context.args[1]
            )

        except ValueError:

            await update.message.reply_text(
                "تعداد تلاش باید عدد باشد."
            )

            return

    attempts = max(
        1,
        min(attempts, 100000)
    )

    await update.message.reply_text(
        f"🔎 شروع Vanity Search\n\n"
        f"🌐 Network: MAINNET\n"
        f"📦 Wallet: V5R1\n"
        f"🎯 Pattern: {pattern}\n"
        f"🔢 Attempts: {attempts}"
    )

    result = await asyncio.to_thread(
        vanity_search,
        pattern,
        attempts
    )

    if not result:

        await update.message.reply_text(
            "❌ آدرس مناسب پیدا نشد."
        )

        return

    if "error" in result:

        await update.message.reply_text(
            "⚠️ تولید کیف پول باید روی "
            "دستگاه امن کاربر انجام شود."
        )

        return

    await update.message.reply_text(
        "🎯 Vanity Address Found!\n\n"
        f"📍 Address:\n"
        f"{result['address']}\n\n"
        f"🔢 Attempts: {result['attempt']}\n"
        f"🌐 MAINNET\n"
        f"📦 V5R1"
    )


# =========================================================
# MAIN
# =========================================================

def main():

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN is not set."
        )

    app = (
        Application
        .builder()
        .token(BOT_TOKEN)
        .build()
    )

    app.add_handler(
        CommandHandler(
            "start",
            start
        )
    )

    app.add_handler(
        CommandHandler(
            "vanity",
            vanity
        )
    )

    print(
        "TON V5R1 MAINNET BOT STARTED"
    )

    app.run_polling()


if __name__ == "__main__":
    main()
