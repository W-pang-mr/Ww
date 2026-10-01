import asyncio
import html
import io
import logging
import os
import sqlite3
import time
from decimal import Decimal, InvalidOperation

import httpx
import qrcode
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from tonsdk.contract.wallet import Wallets, WalletVersionEnum
from tonsdk.utils import Address, bytes_to_b64str

load_dotenv()
logging.basicConfig(level=logging.INFO)

# ───────────────────────── تنظیمات ─────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "").strip()
TONCENTER_API_KEY = os.getenv("TONCENTER_API_KEY", "").strip()
NETWORK = os.getenv("NETWORK", "mainnet").strip().lower()  # mainnet | testnet
DB_PATH = os.getenv("DB_PATH", "wallet.db")

if not BOT_TOKEN or not ENCRYPTION_KEY:
    raise SystemExit("BOT_TOKEN و ENCRYPTION_KEY باید در فایل .env تنظیم شوند.")

IS_TEST = NETWORK == "testnet"
API_BASE = (
    "https://testnet.toncenter.com/api/v2"
    if IS_TEST
    else "https://toncenter.com/api/v2"
)
FEE_RESERVE = Decimal("0.01")  # کارمزد تقریبی شبکه
NANO = Decimal(10**9)
VERSION = WalletVersionEnum.v4r2

fernet = Fernet(ENCRYPTION_KEY.encode())
router = Router()
router.message.filter(F.chat.type == "private")
SENDING: set[int] = set()


# ───────────────────────── دیتابیس ─────────────────────────
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                address TEXT NOT NULL,
                mnemonic_enc TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )"""
        )


def get_user(uid: int):
    with db() as conn:
        return conn.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()


def save_user(uid: int, address: str, mnemonic_enc: str) -> None:
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, address, mnemonic_enc, created_at) VALUES (?,?,?,?)",
            (uid, address, mnemonic_enc, int(time.time())),
        )


# ───────────────────────── رمزنگاری ─────────────────────────
def encrypt(text: str) -> str:
    return fernet.encrypt(text.encode()).decode()


def decrypt(token: str) -> str:
    return fernet.decrypt(token.encode()).decode()


# ───────────────────────── ولت TON ─────────────────────────
def create_wallet():
    mnemonics, _pub, _priv, wallet = Wallets.create(VERSION, workchain=0)
    address = wallet.address.to_string(True, True, False, IS_TEST)
    return mnemonics, address


def load_wallet(mnemonic_str: str):
    _m, _pub, _priv, wallet = Wallets.from_mnemonics(
        mnemonic_str.split(), VERSION, 0
    )
    return wallet


async def call(method: str, params: dict | None = None, body: dict | None = None):
    headers = {"X-API-Key": TONCENTER_API_KEY} if TONCENTER_API_KEY else {}
    url = f"{API_BASE}/{method}"
    async with httpx.AsyncClient(timeout=25) as client:
        for _ in range(4):
            if body is not None:
                r = await client.post(url, json=body, headers=headers)
            else:
                r = await client.get(url, params=params, headers=headers)
            if r.status_code == 429:
                await asyncio.sleep(1.3)
                continue
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                raise RuntimeError(str(data.get("error")))
            return data["result"]
    raise RuntimeError("Toncenter rate limit")


async def get_balance(address: str) -> Decimal:
    nano = await call("getAddressBalance", {"address": address})
    return Decimal(int(nano)) / NANO


async def get_ton_price():
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.coingecko.com/api/v3/simple/price",
                params={"ids": "the-open-network", "vs_currencies": "usd"},
            )
            return Decimal(str(r.json()["the-open-network"]["usd"]))
    except Exception:
        return None


async def send_ton(user_row, to_addr: str, amount: Decimal, comment: str | None):
    wallet = load_wallet(decrypt(user_row["mnemonic_enc"]))
    info = await call("getWalletInformation", {"address": user_row["address"]})
    seqno = info.get("seqno") or 0
    nano = int(amount * NANO)
    query = wallet.create_transfer_message(
        to_addr, nano, seqno, payload=comment or None, send_mode=3
    )
    boc = bytes_to_b64str(query["message"].to_boc(False))
    await call("sendBoc", body={"boc": boc})


def fmt(nano) -> str:
    v = Decimal(int(nano)) / NANO
    return f"{v:.4f}".rstrip("0").rstrip(".")


def short(addr: str) -> str:
    return f"{addr[:6]}…{addr[-6:]}"


# ───────────────────────── رابط ─────────────────────────
class SendFlow(StatesGroup):
    address = State()
    amount = State()
    comment = State()
    confirm = State()


def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("💰 موجودی", "balance"), btn("📥 دریافت", "receive")],
            [btn("📤 ارسال", "send"), btn("📜 تاریخچه", "history")],
            [btn("🔐 کلید بازیابی", "export")],
        ]
    )


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("❌ انصراف", "cancel")]])


async def delete_later(msg: Message, seconds: int) -> None:
    await asyncio.sleep(seconds)
    try:
        await msg.delete()
    except Exception:
        pass


async def need_user(c: CallbackQuery):
    user = get_user(c.from_user.id)
    if not user:
        await c.message.answer("اول /start رو بزن و ولت بساز.")
    return user


# ───────────────────────── هندلرها ─────────────────────────
@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    if not get_user(m.from_user.id):
        kb = InlineKeyboardMarkup(inline_keyboard=[[btn("✨ ساخت ولت", "create")]])
        net = "🧪 (شبکه تست)" if IS_TEST else ""
        await m.answer(
            f"سلام 👋\nبه ربات ولت TON خوش اومدی {net}\nبرای شروع یه ولت بساز:",
            reply_markup=kb,
        )
    else:
        await m.answer("🏠 منوی اصلی", reply_markup=main_menu())


@router.callback_query(F.data == "create")
async def cb_create(c: CallbackQuery):
    await c.answer()
    if get_user(c.from_user.id):
        await c.message.answer("🏠 منوی اصلی", reply_markup=main_menu())
        return
    mnemonics, address = create_wallet()
    save_user(c.from_user.id, address, encrypt(" ".join(mnemonics)))
    sent = await c.message.answer(
        "✅ ولت ساخته شد!\n\n"
        f"📍 آدرس:\n<code>{address}</code>\n\n"
        "🔐 <b>۲۴ کلمه بازیابی (همین الان جایی امن ذخیره کن):</b>\n"
        f"<code>{' '.join(mnemonics)}</code>\n\n"
        "⚠️ این پیام بعد از ۹۰ ثانیه پاک میشه. این کلمات رو به هیچ‌کس نده."
    )
    await c.message.answer("🏠 منوی اصلی", reply_markup=main_menu())
    asyncio.create_task(delete_later(sent, 90))


@router.callback_query(F.data == "cancel")
async def cb_cancel(c: CallbackQuery, state: FSMContext):
    await c.answer("لغو شد")
    await state.clear()
    await c.message.answer("🏠 منوی اصلی", reply_markup=main_menu())


@router.callback_query(F.data == "balance")
async def cb_balance(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    user = await need_user(c)
    if not user:
        return
    try:
        bal = await get_balance(user["address"])
    except Exception:
        await c.message.answer("⚠️ خطا در ارتباط با شبکه TON. دوباره تلاش کن.")
        return
    text = f"💰 موجودی: <b>{bal:.4f} TON</b>"
    if not IS_TEST:
        price = await get_ton_price()
        if price:
            text += f"\n≈ ${bal * price:,.2f}"
    await c.message.answer(text, reply_markup=main_menu())


@router.callback_query(F.data == "receive")
async def cb_receive(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    user = await need_user(c)
    if not user:
        return
    addr = user["address"]
    img = qrcode.make(f"ton://transfer/{addr}")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    await c.message.answer_photo(
        BufferedInputFile(buf.getvalue(), filename="qr.png"),
        caption=f"📥 آدرس ولت تو:\n<code>{addr}</code>",
        reply_markup=main_menu(),
    )


@router.callback_query(F.data == "history")
async def cb_history(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    user = await need_user(c)
    if not user:
        return
    try:
        txs = await call("getTransactions", {"address": user["address"], "limit": 8})
    except Exception:
        await c.message.answer("⚠️ خطا در دریافت تاریخچه.")
        return
    lines = []
    for tx in txs:
        t = time.strftime("%Y-%m-%d %H:%M", time.gmtime(tx.get("utime", 0)))
        outs = [o for o in (tx.get("out_msgs") or []) if o.get("destination")]
        inm = tx.get("in_msg") or {}
        if outs:
            for o in outs:
                lines.append(
                    f"➖ {fmt(o['value'])} TON → <code>{short(o['destination'])}</code>\n    🕒 {t} UTC"
                )
        elif inm.get("source"):
            lines.append(
                f"➕ {fmt(inm['value'])} TON ← <code>{short(inm['source'])}</code>\n    🕒 {t} UTC"
            )
    text = "📜 آخرین تراکنش‌ها:\n\n" + "\n\n".join(lines) if lines else "📜 هنوز تراکنشی نداری."
    await c.message.answer(text, reply_markup=main_menu())


@router.callback_query(F.data == "export")
async def cb_export(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    if not await need_user(c):
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[btn("✅ نمایش کلمات", "export_yes"), btn("❌ انصراف", "cancel")]]
    )
    await c.message.answer(
        "⚠️ هر کسی این ۲۴ کلمه رو ببینه کل دارایی تو رو برمی‌داره. مطمئنی؟",
        reply_markup=kb,
    )


@router.callback_query(F.data == "export_yes")
async def cb_export_yes(c: CallbackQuery):
    await c.answer()
    user = await need_user(c)
    if not user:
        return
    sent = await c.message.answer(
        "🔐 کلمات بازیابی:\n\n"
        f"<code>{decrypt(user['mnemonic_enc'])}</code>\n\n"
        "این پیام بعد از ۶۰ ثانیه پاک میشه."
    )
    asyncio.create_task(delete_later(sent, 60))


# ───────────── جریان ارسال ─────────────
@router.callback_query(F.data == "send")
async def cb_send(c: CallbackQuery, state: FSMContext):
    await c.answer()
    if not await need_user(c):
        return
    await state.set_state(SendFlow.address)
    await c.message.answer("📤 آدرس مقصد رو بفرست:", reply_markup=cancel_kb())


@router.message(StateFilter(SendFlow.address), F.text)
async def st_address(m: Message, state: FSMContext):
    addr = m.text.strip()
    try:
        Address(addr)
    except Exception:
        await m.answer("❌ آدرس معتبر نیست. دوباره بفرست:", reply_markup=cancel_kb())
        return
    await state.update_data(address=addr)
    await state.set_state(SendFlow.amount)
    await m.answer("💎 مقدار TON رو بفرست (مثلا 1.5):", reply_markup=cancel_kb())


@router.message(StateFilter(SendFlow.amount), F.text)
async def st_amount(m: Message, state: FSMContext):
    try:
        amount = Decimal(m.text.strip().replace(",", ".")).quantize(Decimal("0.000000001"))
        if amount <= 0:
            raise InvalidOperation
    except (InvalidOperation, ValueError):
        await m.answer("❌ عدد معتبر نیست. دوباره بفرست:", reply_markup=cancel_kb())
        return
    user = get_user(m.from_user.id)
    try:
        bal = await get_balance(user["address"])
    except Exception:
        await m.answer("⚠️ خطا در ارتباط با شبکه TON. دوباره تلاش کن.")
        return
    if amount + FEE_RESERVE > bal:
        await m.answer(
            f"❌ موجودی کافی نیست.\nموجودی: {bal:.4f} TON\n(حدود {FEE_RESERVE} TON هم برای کارمزد لازمه)",
            reply_markup=cancel_kb(),
        )
        return
    await state.update_data(amount=str(amount))
    await state.set_state(SendFlow.comment)
    await m.answer(
        "📝 کامنت (Memo) رو بفرست، یا برای رد کردن بزن <code>-</code>",
        reply_markup=cancel_kb(),
    )


@router.message(StateFilter(SendFlow.comment), F.text)
async def st_comment(m: Message, state: FSMContext):
    comment = m.text.strip()
    comment = None if comment == "-" else comment[:120]
    await state.update_data(comment=comment)
    data = await state.get_data()
    await state.set_state(SendFlow.confirm)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[btn("✅ تایید و ارسال", "confirm_send"), btn("❌ انصراف", "cancel")]]
    )
    await m.answer(
        "🔎 بررسی نهایی:\n\n"
        f"به: <code>{data['address']}</code>\n"
        f"مقدار: <b>{data['amount']} TON</b>\n"
        f"کامنت: {html.escape(comment) if comment else '—'}\n\n"
        "تراکنش برگشت‌پذیر نیست.",
        reply_markup=kb,
    )


@router.callback_query(F.data == "confirm_send", StateFilter(SendFlow.confirm))
async def cb_confirm(c: CallbackQuery, state: FSMContext):
    await c.answer()
    uid = c.from_user.id
    if uid in SENDING:
        return
    user = get_user(uid)
    data = await state.get_data()
    await state.clear()
    if not user or "address" not in data:
        await c.message.answer("⚠️ اطلاعات ناقص بود. دوباره از اول شروع کن.")
        return
    SENDING.add(uid)
    try:
        await send_ton(user, data["address"], Decimal(data["amount"]), data.get("comment"))
        await c.message.answer(
            "✅ تراکنش ارسال شد. چند ثانیه تا چند دقیقه طول می‌کشه تا تایید بشه.",
            reply_markup=main_menu(),
        )
    except Exception as e:
        logging.exception("send failed")
        await c.message.answer(f"❌ ارسال ناموفق بود: {html.escape(str(e))[:200]}", reply_markup=main_menu())
    finally:
        SENDING.discard(uid)


@router.message()
async def fallback(m: Message, state: FSMContext):
    if await state.get_state() is None:
        await m.answer("از منو استفاده کن 👇", reply_markup=main_menu() if get_user(m.from_user.id) else None)


# ───────────────────────── اجرا ─────────────────────────
async def main():
    init_db()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Bot started (%s)", NETWORK)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
