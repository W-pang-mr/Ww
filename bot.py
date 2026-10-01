import asyncio
import importlib
import inspect
import multiprocessing as mp
import os
import pkgutil
import re
import time
from typing import List, Optional, Tuple

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import (
    BufferedInputFile,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

# ───────────────────────── تنظیمات ─────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)

# تنظیمات جستجو:
TOTAL_ATTEMPTS = 5000  # تعداد کل تلاش‌ها
TARGET_WORD = "SADRA"  # متنی که می‌خواهید در آدرس باشد (یا None بگذارید)

router = Router()


# ───────────────────────── توابع ساخت و بررسی ولت ─────────────────────────
def _find_wallet_class():
    for name in (
        "tonutils.wallet",
        "tonutils.wallets",
        "tonutils.contracts.wallet",
        "tonutils.contracts",
    ):
        try:
            mod = importlib.import_module(name)
        except Exception:
            continue
        cls = getattr(mod, "WalletV5R1", None)
        if cls is not None:
            return cls
    import tonutils

    for info in pkgutil.walk_packages(tonutils.__path__, "tonutils."):
        try:
            mod = importlib.import_module(info.name)
        except Exception:
            continue
        cls = getattr(mod, "WalletV5R1", None)
        if cls is not None and hasattr(cls, "create"):
            return cls
    raise RuntimeError("WalletV5R1 پیدا نشد.")


def _create_one(cls) -> Tuple[str, str]:
    res = cls.create(None)
    if inspect.iscoroutine(res):
        res = asyncio.run(res)
    items = list(res) if isinstance(res, (tuple, list)) else [res]
    wallet = next((x for x in items if hasattr(x, "address")), None)
    mnemonic = next((x for x in items if isinstance(x, (list, str))), None)

    words = " ".join(mnemonic) if isinstance(mnemonic, list) else str(mnemonic)
    words = " ".join(words.split())

    try:
        addr = wallet.address.to_str(is_bounceable=False, is_test_only=False)
    except TypeError:
        addr = wallet.address.to_str()

    return addr, words


def is_pretty_address(
    addr: str, target_word: Optional[str] = None, min_repeat: int = 4
) -> Tuple[bool, str]:
    clean_addr = addr[2:] if addr.startswith(("UQ", "EQ")) else addr

    # ۱. بررسی کلمه متنی دلخواه
    if target_word and target_word.lower() in clean_addr.lower():
        return True, f"حاوی متن '{target_word}'"

    # ۲. کاراکترهای تکراری (مثل 7777 یا AAAA)
    pattern_repeat = rf"(.)\1{{{min_repeat - 1},}}"
    match = re.search(pattern_repeat, clean_addr)
    if match:
        return True, f"تکراری '{match.group(0)}'"

    # ۳. شروع رند
    if len(set(clean_addr[:min_repeat])) == 1:
        return True, f"شروع با '{clean_addr[:min_repeat]}'"

    return False, ""


def worker_task(
    attempts_per_worker: int,
    target_word: Optional[str],
    queue: mp.Queue,
):
    try:
        cls = _find_wallet_class()
    except Exception:
        queue.put([])
        return

    found = []
    for _ in range(attempts_per_worker):
        addr, words = _create_one(cls)
        pretty, reason = is_pretty_address(addr, target_word=target_word)
        if pretty:
            found.append((addr, words, reason))

    queue.put(found)


def run_vanity_search(
    total_attempts: int, target_word: Optional[str] = None
) -> Tuple[List[Tuple[str, str, str]], float]:
    num_cores = max(1, mp.cpu_count())
    attempts_per_worker = total_attempts // num_cores
    remainder = total_attempts % num_cores

    processes = []
    queue = mp.Queue()
    start_time = time.time()

    for i in range(num_cores):
        count = (
            attempts_per_worker + remainder if i == 0 else attempts_per_worker
        )
        p = mp.Process(
            target=worker_task,
            args=(count, target_word, queue),
        )
        processes.append(p)
        p.start()

    all_results = []
    for _ in range(num_cores):
        res = queue.get()
        all_results.extend(res)

    for p in processes:
        p.join()

    elapsed = time.time() - start_time
    return all_results, elapsed


# ───────────────────────── کیبورد و هندلرهای ربات ─────────────────────────
def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚀 شروع کن")]],
        resize_keyboard=True,
    )


@router.message(CommandStart())
async def cmd_start(m: Message):
    await m.answer(
        "👋 **به ربات ساخت آدرس‌های رند TON خوش آمدید.**\n\n"
        "برای شروع جستجوی آدرس‌های اسم‌دار و رند، دکمه **«شروع کن»** را بزنید.",
        reply_markup=main_keyboard(),
    )


@router.message(F.text.in_({"شروع کن", "🚀 شروع کن"}))
async def start_search_handler(m: Message):
    if OWNER_ID and m.from_user.id != OWNER_ID:
        await m.answer("⛔ دسترسی محدود است.")
        return

    status_msg = await m.answer(
        f"⏳ **فرآیند جستجو آغاز شد...**\n\n"
        f"🎯 تعداد کل بررسی‌ها: `{TOTAL_ATTEMPTS:,}`\n"
        f"🔎 کلمه درخواستی: `{TARGET_WORD or 'ندارد'}`\n\n"
        "لطفاً صبور باشید، پس از اتمام نتیجه ارسال می‌شود."
    )

    # اجرا در ترد/پردازش مجزا برای جلوگیری از بلاک شدن ربات
    loop = asyncio.get_running_loop()
    results, elapsed = await loop.run_in_executor(
        None, run_vanity_search, TOTAL_ATTEMPTS, TARGET_WORD
    )

    if not results:
        await status_msg.edit_text(
            f"❌ در `{TOTAL_ATTEMPTS:,}` تلاش، آدرس رندی یافت نشد.\n"
            f"⏱️ زمان پردازش: {elapsed:.2f} ثانیه\n\n"
            "نکته: می‌توانید تعداد تلاش‌ها را در تنظیمات کد افزایش دهید."
        )
        return

    # آماده‌سازی گزارش متنی
    response_text = (
        f"✅ **جستجو با موفقیت پایان یافت!**\n\n"
        f"📊 یافته‌ها: `{len(results)}` مورد رند از بین `{TOTAL_ATTEMPTS:,}` آدرس\n"
        f"⏱️ زمان اجرا: `{elapsed:.2f}` ثانیه\n"
        f"⚡ سرعت: `{TOTAL_ATTEMPTS / elapsed:.1f}` آدرس/ثانیه\n"
        f"───────────────\n\n"
    )

    file_content = "آدرس,کلمات کلیدی,علت رند بودن\n"

    for i, (addr, words, reason) in enumerate(results, 1):
        file_content += f'"{addr}","{words}","{reason}"\n'
        if i <= 5:  # فقط ۵ مورد اول در متن چت نمایش داده می‌شود
            response_text += (
                f"📌 **مورد #{i}** ({reason})\n"
                f"📍 آدرس:\n`{addr}`\n"
                f"🔐 کلمات:\n`{words}`\n\n"
            )

    if len(results) > 5:
        response_text += f"⚠️ و {len(results) - 5} مورد دیگر در فایل خروجی پیوست شده است."

    await status_msg.delete()
    await m.answer(response_text, parse_mode=ParseMode.MARKDOWN)

    # ارسال خروجی فایل برای بک‌آپ
    file_bytes = file_content.encode("utf-8-sig")
    document = BufferedInputFile(file_bytes, filename="pretty_wallets.csv")
    await m.answer_document(
        document, caption="📁 لیست کامل ولت‌های رند پیدا شده (فرمت CSV)"
    )


# ───────────────────────── اجرای اصلی ─────────────────────────
async def main():
    if not BOT_TOKEN:
        raise SystemExit("لطفا BOT_TOKEN را تنظیم کنید.")

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
