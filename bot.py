import asyncio
import base64
import hashlib
import html
import io
import logging
import math
import multiprocessing as mp
import os
import queue as pyqueue
import re
import sqlite3
import time
from decimal import Decimal, InvalidOperation

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
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
from aiohttp import web
from cryptography.fernet import Fernet
from tonsdk.boc import Cell
from tonsdk.contract import Contract
from tonsdk.contract.wallet import Wallets, WalletVersionEnum
from tonsdk.utils import Address, bytes_to_b64str

logging.basicConfig(level=logging.INFO)

# ───────────────────────── تنظیمات (از Environment) ─────────────────────────
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MNEMONIC = " ".join(os.getenv("MNEMONIC", "").lower().split())
TONCENTER_API_KEY = os.getenv("TONCENTER_API_KEY", "").strip()  # اختیاری
NETWORK = os.getenv("NETWORK", "mainnet").strip().lower()  # اختیاری
try:
    OWNER_ID = int(os.getenv("OWNER_ID", "0") or 0)  # بعد از اولین اجرا پر می‌کنی
except ValueError:
    OWNER_ID = 0

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN تنظیم نشده.")
if len(MNEMONIC.split()) != 24:
    raise SystemExit("MNEMONIC باید دقیقا ۲۴ کلمه باشد (با فاصله بین کلمات).")

IS_TEST = NETWORK == "testnet"
API_BASE = (
    "https://testnet.toncenter.com/api/v2"
    if IS_TEST
    else "https://toncenter.com/api/v2"
)
NANO = Decimal(10**9)
FEE_PER_MSG = Decimal("0.01")  # تخمین محافظه‌کارانه کارمزد برای هر گیرنده
BATCH_SIZE = 4  # حداکثر پیام در هر تراکنش ولت V4R2
MAX_LINES = 200

_m, _pub, _priv, WALLET = Wallets.from_mnemonics(
    MNEMONIC.split(), WalletVersionEnum.v4r2, 0
)
ADDRESS = WALLET.address.to_string(True, True, False, IS_TEST)

SEND_LOCK = asyncio.Lock()


# ───────────────────────── ارتباط با شبکه TON ─────────────────────────
async def call(method: str, params: dict | None = None, body: dict | None = None):
    headers = {"X-API-Key": TONCENTER_API_KEY} if TONCENTER_API_KEY else {}
    url = f"{API_BASE}/{method}"
    async with httpx.AsyncClient(timeout=25) as client:
        for _ in range(5):
            if body is not None:
                r = await client.post(url, json=body, headers=headers)
            else:
                r = await client.get(url, params=params, headers=headers)
            if r.status_code == 429:
                await asyncio.sleep(1.5)
                continue
            r.raise_for_status()
            data = r.json()
            if not data.get("ok"):
                raise RuntimeError(str(data.get("error")))
            return data["result"]
    raise RuntimeError("Toncenter rate limit")


async def get_balance(addr: str | None = None) -> Decimal:
    nano = await call("getAddressBalance", {"address": addr or ADDRESS})
    return Decimal(int(nano)) / NANO


async def get_seqno() -> int:
    info = await call("getWalletInformation", {"address": ADDRESS})
    return int(info.get("seqno") or 0)


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


def build_order(to_addr: str, nano: int, comment: str | None):
    payload = Cell()
    if comment:
        payload.bits.write_uint(0, 32)
        payload.bits.write_bytes(comment.encode("utf-8"))
    header = Contract.create_internal_message_header(Address(to_addr), nano)
    return Contract.create_common_msg_info(header, None, payload)


async def send_batch(chunk: list, seqno: int) -> None:
    signing = WALLET.create_signing_message(seqno)
    for addr, nano, comment in chunk:
        signing.bits.write_uint8(3)  # pay fees separately + ignore errors
        signing.refs.append(build_order(addr, nano, comment))
    ext = WALLET.create_external_message(signing, seqno)
    boc = bytes_to_b64str(ext["message"].to_boc(False))
    await call("sendBoc", body={"boc": boc})


async def wait_seqno(old: int, timeout: int = 90) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        await asyncio.sleep(4)
        try:
            if await get_seqno() > old:
                return True
        except Exception:
            continue
    return False


# ───────────────────────── ابزارها ─────────────────────────
def fmt(nano) -> str:
    v = Decimal(int(nano)) / NANO
    return f"{v:.4f}".rstrip("0").rstrip(".")


def short(addr: str) -> str:
    return f"{addr[:6]}…{addr[-6:]}"


def parse_lines(text: str):
    orders, errors = [], []
    for i, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 2:
            errors.append(f"خط {i}: فرمت اشتباه (آدرس و مبلغ لازمه)")
            continue
        addr, amt = parts[0], parts[1]
        comment = parts[2].strip() if len(parts) > 2 else None
        try:
            Address(addr)
        except Exception:
            errors.append(f"خط {i}: آدرس نامعتبر")
            continue
        try:
            amount = Decimal(amt.replace(",", ".")).quantize(Decimal("0.000000001"))
            if amount <= 0:
                raise InvalidOperation
        except (InvalidOperation, ValueError):
            errors.append(f"خط {i}: مبلغ نامعتبر")
            continue
        if comment and len(comment.encode("utf-8")) > 120:
            errors.append(f"خط {i}: کامنت خیلی طولانیه")
            continue
        orders.append((addr, int(amount * NANO), comment))
    if len(orders) > MAX_LINES:
        errors.append(f"حداکثر {MAX_LINES} گیرنده مجازه.")
    return orders, errors


# ───────────────────────── رابط ─────────────────────────
class Multi(StatesGroup):
    waiting = State()
    confirm = State()


class GenWallets(StatesGroup):
    count = State()


class Vanity(StatesGroup):
    pattern = State()
    confirm = State()


def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("💰 موجودی", "balance"), btn("📍 آدرس", "address")],
            [btn("📤 ارسال (تکی / گروهی)", "multi")],
            [btn("📜 تاریخچه", "history"), btn("🪪 ولت‌های V5", "wl_menu")],
        ]
    )


def cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[btn("❌ انصراف", "cancel")]])


# ───────────────────────── قفل مالک ─────────────────────────
guard = Router()


def not_owner(event) -> bool:
    return event.from_user is None or event.from_user.id != OWNER_ID


@guard.message(F.chat.type == "private", not_owner)
async def deny_msg(m: Message):
    if OWNER_ID == 0:
        await m.answer(
            "🔧 تنظیم اولیه:\n"
            f"آیدی عددی تلگرام تو: <code>{m.from_user.id}</code>\n\n"
            "این عدد رو توی Render با اسم <code>OWNER_ID</code> اضافه کن و سرویس رو دوباره دیپلوی کن."
        )
    else:
        await m.answer("⛔ این ربات خصوصیه.")


@guard.callback_query(not_owner)
async def deny_cb(c: CallbackQuery):
    await c.answer("⛔", show_alert=True)


# ───────────────────────── هندلرها ─────────────────────────
router = Router()
router.message.filter(F.chat.type == "private")


@router.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    net = "🧪 تست‌نت" if IS_TEST else "🌐 مین‌نت"
    await m.answer(
        f"👋 ولت TON ({net})\n📍 آدرس:\n<code>{ADDRESS}</code>",
        reply_markup=main_menu(),
    )


@router.callback_query(F.data == "cancel")
async def cb_cancel(c: CallbackQuery, state: FSMContext):
    await c.answer("لغو شد")
    await state.clear()
    await c.message.answer("🏠 منوی اصلی", reply_markup=main_menu())


@router.callback_query(F.data == "address")
async def cb_address(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    await c.message.answer(f"📍 آدرس ولت:\n<code>{ADDRESS}</code>", reply_markup=main_menu())


@router.callback_query(F.data == "balance")
async def cb_balance(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    try:
        bal = await get_balance()
    except Exception:
        await c.message.answer("⚠️ خطا در ارتباط با شبکه TON. دوباره تلاش کن.")
        return
    text = f"💰 موجودی: <b>{bal:.4f} TON</b>"
    if not IS_TEST:
        price = await get_ton_price()
        if price:
            text += f"\n≈ ${bal * price:,.2f}"
    await c.message.answer(text, reply_markup=main_menu())


@router.callback_query(F.data == "history")
async def cb_history(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    try:
        txs = await call("getTransactions", {"address": ADDRESS, "limit": 10})
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
                    f"➖ {fmt(o['value'])} TON → <code>{short(o['destination'])}</code>  🕒 {t} UTC"
                )
        elif inm.get("source"):
            lines.append(
                f"➕ {fmt(inm['value'])} TON ← <code>{short(inm['source'])}</code>  🕒 {t} UTC"
            )
    text = "📜 آخرین تراکنش‌ها:\n\n" + "\n".join(lines) if lines else "📜 هنوز تراکنشی نیست."
    await c.message.answer(text, reply_markup=main_menu())


# ───────────── ارسال گروهی ─────────────
@router.callback_query(F.data == "multi")
async def cb_multi(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(Multi.waiting)
    await c.message.answer(
        "📤 <b>ارسال</b>\n\n"
        "هر خط یک گیرنده، با این فرمت:\n"
        "<code>آدرس مبلغ</code>\n"
        "یا با کامنت:\n"
        "<code>آدرس مبلغ کامنت</code>\n\n"
        "مثال:\n"
        "<code>UQAbc...xyz 1.5\nUQDef...uvw 0.3 سلام</code>\n\n"
        f"برای ارسال تکی فقط یک خط بفرست. حداکثر {MAX_LINES} خط.\n"
        "می‌تونی لیست رو به‌صورت فایل .txt هم بفرستی.",
        reply_markup=cancel_kb(),
    )


async def handle_list(m: Message, state: FSMContext, text: str):
    orders, errors = parse_lines(text)
    if errors or not orders:
        msg = "❌ مشکل در لیست:\n" + "\n".join(html.escape(e) for e in errors[:10])
        if not orders and not errors:
            msg = "❌ لیست خالیه."
        await m.answer(msg + "\n\nاصلاح کن و دوباره بفرست:", reply_markup=cancel_kb())
        return
    try:
        bal = await get_balance()
    except Exception:
        await m.answer("⚠️ خطا در ارتباط با شبکه TON. دوباره تلاش کن.")
        return
    total = sum(Decimal(n) for _, n, _ in orders) / NANO
    fees = FEE_PER_MSG * len(orders)
    batches = math.ceil(len(orders) / BATCH_SIZE)
    if total + fees > bal:
        await m.answer(
            f"❌ موجودی کافی نیست.\nموجودی: {bal:.4f}\nجمع ارسال: {total:.4f}\n"
            f"کارمزد تخمینی: {fees:.2f}\n\nلیست جدید بفرست:",
            reply_markup=cancel_kb(),
        )
        return
    await state.update_data(orders=[[a, str(n), c] for a, n, c in orders])
    await state.set_state(Multi.confirm)
    preview = "\n".join(
        f"{i}. <code>{short(a)}</code> ← {fmt(n)} TON" + (f" 📝 {html.escape(c)}" if c else "")
        for i, (a, n, c) in enumerate(orders[:10], 1)
    )
    if len(orders) > 10:
        preview += f"\n… و {len(orders) - 10} گیرنده دیگر"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[btn("✅ تایید و ارسال", "confirm_send"), btn("❌ انصراف", "cancel")]]
    )
    await m.answer(
        f"🔎 <b>بررسی نهایی</b>\n\n{preview}\n\n"
        f"👥 گیرنده‌ها: {len(orders)}\n"
        f"💎 جمع: <b>{total:.4f} TON</b>\n"
        f"⛽ کارمزد تقریبی: کمتر از {fees:.2f} TON\n"
        f"📦 تعداد تراکنش: {batches}\n\n"
        "تراکنش‌ها برگشت‌پذیر نیستند.",
        reply_markup=kb,
    )


@router.message(StateFilter(Multi.waiting), F.text)
async def st_text(m: Message, state: FSMContext):
    await handle_list(m, state, m.text)


@router.message(StateFilter(Multi.waiting), F.document)
async def st_doc(m: Message, state: FSMContext):
    if m.document.file_size and m.document.file_size > 1_000_000:
        await m.answer("❌ فایل خیلی بزرگه.", reply_markup=cancel_kb())
        return
    buf = io.BytesIO()
    await m.bot.download(m.document, destination=buf)
    try:
        text = buf.getvalue().decode("utf-8")
    except UnicodeDecodeError:
        await m.answer("❌ فایل باید متنی (UTF-8) باشه.", reply_markup=cancel_kb())
        return
    await handle_list(m, state, text)


@router.callback_query(F.data == "confirm_send", StateFilter(Multi.confirm))
async def cb_confirm(c: CallbackQuery, state: FSMContext):
    await c.answer()
    data = await state.get_data()
    await state.clear()
    raw = data.get("orders")
    if not raw:
        await c.message.answer("⚠️ اطلاعات ناقص بود. دوباره شروع کن.", reply_markup=main_menu())
        return
    if SEND_LOCK.locked():
        await c.message.answer("⏳ یک ارسال دیگه در حال انجامه. صبر کن تموم شه.")
        return
    orders = [(a, int(n), cm) for a, n, cm in raw]
    batches = [orders[i : i + BATCH_SIZE] for i in range(0, len(orders), BATCH_SIZE)]
    status = await c.message.answer(f"⏳ شروع ارسال… (0/{len(batches)})")
    async with SEND_LOCK:
        for idx, chunk in enumerate(batches, 1):
            try:
                seqno = await get_seqno()
                await send_batch(chunk, seqno)
                ok = await wait_seqno(seqno)
            except Exception as e:
                logging.exception("batch failed")
                await status.edit_text(
                    f"❌ خطا در تراکنش {idx}/{len(batches)}:\n{html.escape(str(e))[:200]}\n"
                    "ارسال متوقف شد. قبل از تلاش مجدد تاریخچه رو چک کن."
                )
                await c.message.answer("🏠 منوی اصلی", reply_markup=main_menu())
                return
            if not ok:
                await status.edit_text(
                    f"⚠️ تراکنش {idx}/{len(batches)} هنوز تایید نشد.\n"
                    "برای جلوگیری از ارسال تکراری، ادامه داده نشد. تاریخچه رو چک کن."
                )
                await c.message.answer("🏠 منوی اصلی", reply_markup=main_menu())
                return
            await status.edit_text(f"⏳ در حال ارسال… ({idx}/{len(batches)})")
    await status.edit_text(f"✅ همه {len(orders)} انتقال ارسال شد.")
    await c.message.answer("🏠 منوی اصلی", reply_markup=main_menu())


# ───────────── ولت‌های V5R1 (ساخت، ذخیره، حذف) ─────────────
MAX_GEN = 500
PAGE_SIZE = 10
DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
DB_PATH = os.getenv("DB_PATH", "wallets.db")
PERSISTENT = os.path.isabs(DB_PATH)  # مثلا /data/wallets.db روی دیسک ماندگار
FERNET = Fernet(
    base64.urlsafe_b64encode(hashlib.sha256(("walletbot:" + MNEMONIC).encode()).digest())
)


def encrypt(text: str) -> str:
    return FERNET.encrypt(text.encode()).decode()


def decrypt(token: str) -> str:
    try:
        return FERNET.decrypt(token.encode()).decode()
    except Exception:
        return "❌ رمزگشایی ناموفق (MNEMONIC عوض شده؟)"


def db_exec(sql: str, args=(), fetch: bool = False):
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(sql, args)
        rows = cur.fetchall() if fetch else None
        conn.commit()
        return rows
    finally:
        conn.close()


def init_db() -> None:
    db_exec(
        """CREATE TABLE IF NOT EXISTS wallets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT UNIQUE NOT NULL,
            mnemonic_enc TEXT NOT NULL,
            created_at INTEGER NOT NULL
        )"""
    )


def save_wallets(items) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        now = int(time.time())
        conn.executemany(
            "INSERT OR IGNORE INTO wallets (address, mnemonic_enc, created_at) VALUES (?,?,?)",
            [(a, encrypt(w), now) for a, w in items],
        )
        conn.commit()
    finally:
        conn.close()


def count_wallets() -> int:
    return db_exec("SELECT COUNT(*) AS c FROM wallets", fetch=True)[0]["c"]


def list_wallets(page: int):
    return db_exec(
        "SELECT id, address FROM wallets ORDER BY id LIMIT ? OFFSET ?",
        (PAGE_SIZE, page * PAGE_SIZE),
        True,
    )


def get_wallet(wid: int):
    rows = db_exec("SELECT * FROM wallets WHERE id=?", (wid,), True)
    return rows[0] if rows else None


def all_wallets():
    rows = db_exec("SELECT address, mnemonic_enc FROM wallets ORDER BY id", fetch=True)
    return [(r["address"], decrypt(r["mnemonic_enc"])) for r in rows]


def _find_wallet_class():
    """کلاس WalletV5R1 را در هر نسخه‌ی tonutils پیدا می‌کند."""
    import importlib
    import pkgutil

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
    try:
        from importlib.metadata import version

        ver = version("tonutils")
    except Exception:
        ver = "?"
    raise RuntimeError(f"WalletV5R1 not found (tonutils {ver})")


def _network_value(testnet: bool):
    try:
        from tonutils.types import NetworkGlobalID

        return NetworkGlobalID.TESTNET if testnet else NetworkGlobalID.MAINNET
    except Exception:
        return -3 if testnet else -239


def _build_clients():
    """کلاینت‌های tonutils را (بدون اتصال به شبکه) می‌سازد؛ فقط برای ساختن ولت لازم است."""
    import importlib
    import inspect
    import pkgutil

    import tonutils

    found = []
    for info in pkgutil.walk_packages(tonutils.__path__, "tonutils."):
        if "client" not in info.name.lower():
            continue
        try:
            mod = importlib.import_module(info.name)
        except Exception:
            continue
        for name, obj in vars(mod).items():
            if (
                inspect.isclass(obj)
                and name.endswith("Client")
                and getattr(obj, "__module__", "").startswith("tonutils")
                and obj not in found
                and not inspect.isabstract(obj)
            ):
                found.append(obj)
    found.sort(key=lambda c: (0 if "Toncenter" in c.__name__ else 1, c.__name__))
    clients, errors = [], []
    for cls in found:
        try:
            sig = inspect.signature(cls.__init__)
            kwargs = {}
            for pname, prm in list(sig.parameters.items())[1:]:
                if prm.kind in (prm.VAR_POSITIONAL, prm.VAR_KEYWORD):
                    continue
                needed = prm.default is inspect.Parameter.empty
                if "network" in pname:
                    if needed or IS_TEST:
                        kwargs[pname] = _network_value(IS_TEST)
                elif pname == "is_testnet":
                    if needed or IS_TEST:
                        kwargs[pname] = IS_TEST
                elif needed:
                    kwargs[pname] = None
            clients.append(cls(**kwargs))
        except Exception as e:
            errors.append(f"{cls.__name__}: {e!r}"[:150])
    return clients, errors, [c.__name__ for c in found]


def _create_one(cls, client):
    import inspect

    res = cls.create(client)
    if inspect.iscoroutine(res):
        res = asyncio.run(res)
    items = list(res) if isinstance(res, (tuple, list)) else [res]
    wallet = next((x for x in items if hasattr(x, "address")), None)
    mnemonic = next((x for x in items if isinstance(x, (list, str))), None)
    if wallet is None or mnemonic is None:
        raise RuntimeError(
            "unexpected create() result: " + ", ".join(type(x).__name__ for x in items)
        )
    words = " ".join(mnemonic) if isinstance(mnemonic, list) else str(mnemonic)
    words = " ".join(words.split())
    if len(words.split()) != 24:
        raise RuntimeError("mnemonic is not 24 words")
    try:
        addr = wallet.address.to_str(is_bounceable=False, is_test_only=IS_TEST)
    except TypeError:
        addr = wallet.address.to_str()
    return addr, words


def _get_creator():
    """تابعی برمی‌گرداند که هر بار یک ولت V5R1 (address, words) می‌سازد."""
    import inspect

    cls = _find_wallet_class()
    clients, errors, client_names = _build_clients()
    first, working = None, None
    for client in clients + [None]:
        try:
            first = _create_one(cls, client)
            working = client
            break
        except Exception as e:
            errors.append(f"create({type(client).__name__}): {e!r}"[:220])
    if first is None:
        try:
            sig = str(inspect.signature(cls.create))
        except Exception:
            sig = "?"
        raise RuntimeError(
            f"create failed | sig={sig} | clients={','.join(client_names)} | "
            + " || ".join(errors)
        )
    return (lambda: _create_one(cls, working)), first


def generate_wallets(n: int):
    """n ولت V5R1 با ۲۴ کلمه می‌سازد. خروجی: لیست (address, mnemonic)."""
    make, first = _get_creator()
    return [first] + [make() for _ in range(n - 1)]


# ───────────── جستجوی آدرس خاص (Vanity) — چندالگویی و چندپردازشی ─────────────
# تا ۵ الگو هم‌زمان؛ هر آدرس ساخته‌شده با همه‌ی الگوها مقایسه می‌شود.
# ساخت ولت (PBKDF2 سنگین) روی چند پردازش جدا موازی می‌شود تا از همه‌ی هسته‌ها استفاده شود.
VN_TRIES = (1000, 5000, 20000, 100000)
VN_MAX_PATTERNS = 5  # حداکثر تعداد الگو در یک جستجو
VN_MAX_FOUND = 300  # سقف نتایج؛ با رسیدن به آن جستجو خودکار متوقف می‌شود
VN_CHUNK = 4  # هر پردازش در هر نوبت این تعداد تلاش را برمی‌دارد
VN_UPDATE_EVERY = 3.0  # فاصله‌ی به‌روزرسانی پیام وضعیت (ثانیه)
VN_PROGRESS: dict = {}
VN_STATS: dict = {}  # سرعت آخرین جستجو (برای تخمین زمان)
SEARCH_LOCK = asyncio.Lock()

MODE_FA = {
    "prefix": "شروع آدرس (بعد از UQ)",
    "suffix": "پایان آدرس",
    "contains": "داخل آدرس",
}
MODE_SHORT = {"prefix": "شروع", "suffix": "پایان", "contains": "داخل"}


def _cpu_count() -> int:
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 1


def _workers() -> int:
    """تعداد پردازش‌ها: پیش‌فرض min(4, هسته‌ها)؛ با Environment به اسم VN_WORKERS قابل تغییر."""
    try:
        env = int(os.getenv("VN_WORKERS", "0") or 0)
    except ValueError:
        env = 0
    n = env if env > 0 else min(4, _cpu_count())
    return max(1, min(n, 16))


VN_WORKERS = _workers()


def vanity_hits(addr: str, specs: list, cs: bool) -> list:
    """اندیس همه‌ی الگوهایی که با این آدرس جور هستند (لیست خالی = هیچ‌کدام)."""
    a = addr if cs else addr.lower()
    hits = []
    for i, (mode, q) in enumerate(specs):
        if mode == "prefix":
            ok = a[2:].startswith(q)
        elif mode == "suffix":
            ok = a.endswith(q)
        else:
            ok = q in a[3:]
        if ok:
            hits.append(i)
    return hits


def vanity_probability(mode: str, pat: str, cs: bool) -> float:
    p = 1.0
    for i, ch in enumerate(pat):
        if mode == "prefix" and i == 0:
            ok = ch in "ABCD" or (not cs and ch.upper() in "ABCD")
            if not ok:
                return 0.0
            p *= 1 / 4
        elif not cs and ch.isalpha():
            p *= 2 / 64
        else:
            p *= 1 / 64
    if mode == "contains":
        p = min(1.0, p * max(1, 46 - len(pat) + 1))
    return p


def vanity_worker(specs, cs, tries, claimed, checked, nfound, stop, out_q, err_q) -> None:
    """یک پردازش جدا: ولت می‌سازد و با همه‌ی الگوها مقایسه می‌کند."""
    try:
        make, pending = _get_creator()
        while not stop.is_set():
            with claimed.get_lock():
                take = min(VN_CHUNK, tries - claimed.value)
                if take <= 0:
                    break
                claimed.value += take
            for _ in range(take):
                if stop.is_set():
                    break
                if pending is not None:
                    item, pending = pending, None
                else:
                    item = make()
                with checked.get_lock():
                    checked.value += 1
                hits = vanity_hits(item[0], specs, cs)
                if hits:
                    out_q.put((item[0], item[1], hits))
                    with nfound.get_lock():
                        nfound.value += 1
                        if nfound.value >= VN_MAX_FOUND:
                            stop.set()
    except Exception as e:  # noqa: BLE001
        err_q.put(repr(e)[:1500])


def vn_start_workers(specs: list, cs: bool, tries: int) -> dict:
    methods = mp.get_all_start_methods()
    ctx = mp.get_context("forkserver" if "forkserver" in methods else "spawn")
    st = {
        "claimed": ctx.Value("q", 0),
        "checked": ctx.Value("q", 0),
        "nfound": ctx.Value("q", 0),
        "stop": ctx.Event(),
        "out_q": ctx.Queue(),
        "err_q": ctx.Queue(),
        "procs": [],
    }
    n = max(1, min(VN_WORKERS, math.ceil(tries / VN_CHUNK)))
    try:
        for _ in range(n):
            p = ctx.Process(
                target=vanity_worker,
                args=(
                    specs,
                    cs,
                    tries,
                    st["claimed"],
                    st["checked"],
                    st["nfound"],
                    st["stop"],
                    st["out_q"],
                    st["err_q"],
                ),
                daemon=True,
            )
            p.start()
            st["procs"].append(p)
    except Exception:
        vn_stop_workers(st)
        raise
    return st


def vn_stop_workers(st: dict) -> None:
    st["stop"].set()
    deadline = time.time() + 8
    for p in st["procs"]:
        p.join(max(0.1, deadline - time.time()))
    for p in st["procs"]:
        if p.is_alive():
            p.terminate()
    for p in st["procs"]:
        p.join(2)
    for q in (st["out_q"], st["err_q"]):
        try:
            q.close()
            q.cancel_join_thread()
        except Exception:  # noqa: BLE001
            pass


def vn_drain(q) -> list:
    items = []
    while True:
        try:
            items.append(q.get_nowait())
        except (pyqueue.Empty, OSError, EOFError):
            break
    return items


def fmt_dur(sec: float) -> str:
    sec = int(max(0, sec))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def vn_label(mode: str, pat: str) -> str:
    return f"{pat} ({MODE_SHORT[mode]})"


def vn_bar(frac: float, n: int = 16) -> str:
    f = max(0, min(n, int(frac * n)))
    return "█" * f + "░" * (n - f)


def vn_progress_text(
    patterns: list,
    tries: int,
    checked: int,
    elapsed: float,
    workers: int,
    counts: list,
    last_addr: str,
    stopping: bool,
) -> str:
    frac = min(1.0, checked / tries) if tries else 0.0
    speed = checked / elapsed if elapsed > 0 else 0.0
    head = "🛑 در حال توقف…" if stopping else "🔎 <b>جستجوی آدرس خاص</b>"
    time_line = f"⏱ گذشته: {fmt_dur(elapsed)}"
    if speed > 0 and checked > 0 and not stopping:
        time_line += f" | باقی‌مانده (حداکثر): ~{fmt_dur((tries - checked) / speed)}"
    lines = [
        head,
        "",
        f"{vn_bar(frac)} {frac * 100:.1f}%",
        f"📊 بررسی‌شده: {checked:,} / {tries:,}",
        f"⚡ سرعت: {speed:.1f} آدرس در ثانیه ({workers} پردازش)",
        time_line,
        "",
        f"🎯 پیدا شده: <b>{sum(counts)}</b>",
    ]
    for (mode, pat), n in zip(patterns, counts):
        lines.append(f"{'✅' if n else '▫️'} <code>{html.escape(pat)}</code> ({MODE_SHORT[mode]}): {n}")
    if last_addr:
        lines += ["", "🆕 آخرین آدرس پیدا شده:", f"<code>{last_addr}</code>"]
    return "\n".join(lines)


async def vn_run(patterns: list, cs: bool, tries: int, progress: dict, status: Message, stop_kb) -> dict:
    """جستجو را اجرا می‌کند و پیام وضعیت را زنده به‌روز نگه می‌دارد."""
    specs = [(m, p if cs else p.lower()) for m, p in patterns]
    counts = [0] * len(patterns)
    found: list = []
    labels: dict = {}
    last_addr = ""
    err = None
    warn = None
    checked = 0
    capped = False
    workers = 0
    st = None
    t0 = time.time()

    def absorb(items: list) -> None:
        nonlocal last_addr
        for addr, words, hits in items:
            found.append((addr, words))
            labels[addr] = " | ".join(vn_label(*patterns[i]) for i in hits)
            for i in hits:
                counts[i] += 1
            last_addr = addr

    try:
        st = await asyncio.to_thread(vn_start_workers, specs, cs, tries)
        workers = len(st["procs"])
        last_edit = 0.0
        while True:
            await asyncio.sleep(0.5)
            absorb(vn_drain(st["out_q"]))
            if progress["stop"] and not st["stop"].is_set():
                st["stop"].set()
            alive = any(p.is_alive() for p in st["procs"])
            now = time.time()
            if alive and now - last_edit >= VN_UPDATE_EVERY:
                last_edit = now
                text = vn_progress_text(
                    patterns,
                    tries,
                    st["checked"].value,
                    now - t0,
                    workers,
                    counts,
                    last_addr,
                    st["stop"].is_set(),
                )
                try:
                    await status.edit_text(text, reply_markup=stop_kb)
                except Exception:  # noqa: BLE001
                    pass
            if not alive:
                break
        await asyncio.sleep(0.3)
        absorb(vn_drain(st["out_q"]))
        checked = st["checked"].value
        capped = st["nfound"].value >= VN_MAX_FOUND
        errs = list(dict.fromkeys(vn_drain(st["err_q"])))
        crashed = [p.exitcode for p in st["procs"] if p.exitcode not in (0, None)]
        if errs or crashed:
            msg = " || ".join(errs) if errs else f"worker exit codes: {crashed}"
            if checked == 0:
                err = msg
            else:
                warn = msg
    except Exception as e:  # noqa: BLE001
        logging.exception("vanity search failed")
        err = repr(e)
    finally:
        if st is not None:
            await asyncio.to_thread(vn_stop_workers, st)
    elapsed = time.time() - t0
    if checked > 20 and elapsed > 0:
        VN_STATS["speed"] = checked / elapsed
    return {
        "found": found,
        "labels": labels,
        "counts": counts,
        "checked": checked,
        "elapsed": elapsed,
        "workers": workers,
        "error": err,
        "warn": warn,
        "stopped": progress["stop"],
        "capped": capped,
    }


async def delete_later(msg: Message, seconds: int) -> None:
    await asyncio.sleep(seconds)
    try:
        await msg.delete()
    except Exception:
        pass


async def send_wallet_files(m: Message, wallets: list, caption: str, labels: dict | None = None) -> None:
    labels = labels or {}
    txt = "\n".join(
        f"#{i}\nAddress: {a}\n"
        + (f"Match: {labels[a]}\n" if a in labels else "")
        + f"Mnemonic: {w}\n"
        for i, (a, w) in enumerate(wallets, 1)
    )
    if labels:
        csv = "index,address,mnemonic,match\n" + "\n".join(
            f"{i},{a},{w},{labels.get(a, '')}" for i, (a, w) in enumerate(wallets, 1)
        )
    else:
        csv = "index,address,mnemonic\n" + "\n".join(
            f"{i},{a},{w}" for i, (a, w) in enumerate(wallets, 1)
        )
    stamp = time.strftime("%Y%m%d-%H%M%S")
    await m.answer_document(
        BufferedInputFile(txt.encode("utf-8"), filename=f"wallets-{stamp}.txt"),
        caption=caption,
    )
    await m.answer_document(
        BufferedInputFile(csv.encode("utf-8"), filename=f"wallets-{stamp}.csv"),
        caption="همان لیست به فرمت CSV",
    )


def wl_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("➕ ساخت ولت جدید", "gen"), btn("🎯 آدرس خاص", "vn")],
            [btn("📋 لیست ولت‌ها", "wl_list:0"), btn("📥 دریافت همه (فایل)", "wl_export")],
            [btn("🗑 حذف همه", "wl_delall")],
            [btn("🏠 منوی اصلی", "home")],
        ]
    )


@router.callback_query(F.data == "home")
async def cb_home(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    await c.message.answer("🏠 منوی اصلی", reply_markup=main_menu())


@router.callback_query(F.data == "wl_menu")
async def cb_wl_menu(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    text = f"🪪 <b>ولت‌های V5R1</b>\nتعداد ذخیره‌شده: <b>{count_wallets()}</b>"
    if not PERSISTENT:
        text += (
            "\n\n⚠️ دیتابیس روی دیسک موقتی است و با ریستارت یا دیپلوی پاک می‌شود. "
            "فایل‌هایی که موقع ساخت می‌گیری را حتما نگه دار."
        )
    await c.message.answer(text, reply_markup=wl_menu_kb())


@router.callback_query(F.data == "gen")
async def cb_gen(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.set_state(GenWallets.count)
    await c.message.answer(
        f"🪪 چند ولت V5R1 بسازم؟\nیک عدد بفرست (۱ تا {MAX_GEN}):",
        reply_markup=cancel_kb(),
    )


@router.message(StateFilter(GenWallets.count), F.text)
async def st_gen_count(m: Message, state: FSMContext):
    try:
        n = int(m.text.strip().translate(DIGITS))
    except ValueError:
        n = 0
    if not 1 <= n <= MAX_GEN:
        await m.answer(f"❌ یک عدد بین ۱ تا {MAX_GEN} بفرست:", reply_markup=cancel_kb())
        return
    await state.clear()
    status = await m.answer(f"⏳ در حال ساخت {n} ولت… (ممکنه چند دقیقه طول بکشه)")
    try:
        wallets = await asyncio.to_thread(generate_wallets, n)
    except Exception as e:
        logging.exception("wallet generation failed")
        await status.edit_text(
            f"❌ ساخت ناموفق بود:\n<code>{html.escape(repr(e))[:1500]}</code>"
        )
        await m.answer("🏠 منوی اصلی", reply_markup=main_menu())
        return
    save_wallets(wallets)
    await send_wallet_files(
        m,
        wallets,
        f"✅ {n} ولت V5R1 ساخته شد (آدرس + ۲۴ کلمه).\n"
        "⚠️ فایل رو همین الان جای امن ذخیره کن و بعدش پیام رو از چت پاک کن.",
    )
    if n <= 10:
        body = "\n\n".join(
            f"<b>#{i}</b>\n<code>{a}</code>\n<code>{w}</code>"
            for i, (a, w) in enumerate(wallets, 1)
        )
        shown = await m.answer(body + "\n\n⚠️ این پیام بعد از ۲ دقیقه پاک میشه.")
        asyncio.create_task(delete_later(shown, 120))
    await status.delete()
    await m.answer("🪪 ولت‌ها", reply_markup=wl_menu_kb())


@router.callback_query(F.data.startswith("wl_list:"))
async def cb_wl_list(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    total = count_wallets()
    if total == 0:
        await c.message.answer("هنوز ولتی ذخیره نشده.", reply_markup=wl_menu_kb())
        return
    pages = max(1, math.ceil(total / PAGE_SIZE))
    page = min(max(int(c.data.split(":")[1]), 0), pages - 1)
    rows = [
        [btn(f"#{r['id']}  {short(r['address'])}", f"wl_view:{r['id']}")]
        for r in list_wallets(page)
    ]
    nav = []
    if page > 0:
        nav.append(btn("◀️ قبلی", f"wl_list:{page - 1}"))
    if page < pages - 1:
        nav.append(btn("بعدی ▶️", f"wl_list:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([btn("⬅️ برگشت", "wl_menu")])
    await c.message.answer(
        f"📋 ولت‌ها ({total}) — صفحه {page + 1}/{pages}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@router.callback_query(F.data.startswith("wl_view:"))
async def cb_wl_view(c: CallbackQuery):
    await c.answer()
    wid = int(c.data.split(":")[1])
    row = get_wallet(wid)
    if not row:
        await c.message.answer("❌ این ولت پیدا نشد.", reply_markup=wl_menu_kb())
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🗑 حذف این ولت", f"wl_del:{wid}")],
            [btn("📋 لیست", "wl_list:0")],
        ]
    )
    sent = await c.message.answer(
        f"🪪 <b>ولت #{wid}</b>\n\n"
        f"📍 آدرس:\n<code>{row['address']}</code>\n\n"
        f"🔐 ۲۴ کلمه:\n<code>{decrypt(row['mnemonic_enc'])}</code>\n\n"
        "⚠️ این پیام بعد از ۶۰ ثانیه پاک میشه.",
        reply_markup=kb,
    )
    asyncio.create_task(delete_later(sent, 60))


@router.callback_query(F.data.startswith("wl_del:"))
async def cb_wl_del(c: CallbackQuery):
    await c.answer()
    wid = int(c.data.split(":")[1])
    row = get_wallet(wid)
    if not row:
        await c.message.answer("❌ این ولت پیدا نشد.", reply_markup=wl_menu_kb())
        return
    try:
        bal = await get_balance(row["address"])
        bal_text = f"💰 موجودی این ولت: <b>{bal:.4f} TON</b>"
        if bal > 0:
            bal_text += "\n🚨 این ولت پول دارد!"
    except Exception:
        bal_text = "💰 موجودی: نامشخص (خطای شبکه)"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ بله، حذف کن", f"wl_delyes:{wid}"), btn("❌ انصراف", "wl_list:0")]
        ]
    )
    await c.message.answer(
        f"🗑 حذف ولت #{wid}\n<code>{short(row['address'])}</code>\n\n{bal_text}\n\n"
        "با حذف، ۲۴ کلمه از ربات پاک می‌شود و برگشتی ندارد. "
        "اگر کلمات را جای دیگری نداری، ممکنه پولت برای همیشه از دست بره.",
        reply_markup=kb,
    )


@router.callback_query(F.data.startswith("wl_delyes:"))
async def cb_wl_delyes(c: CallbackQuery):
    await c.answer()
    wid = int(c.data.split(":")[1])
    db_exec("DELETE FROM wallets WHERE id=?", (wid,))
    await c.message.answer(f"✅ ولت #{wid} حذف شد.", reply_markup=wl_menu_kb())


@router.callback_query(F.data == "wl_export")
async def cb_wl_export(c: CallbackQuery):
    await c.answer()
    wallets = all_wallets()
    if not wallets:
        await c.message.answer("هنوز ولتی ذخیره نشده.", reply_markup=wl_menu_kb())
        return
    await send_wallet_files(
        c.message,
        wallets,
        f"📥 همه {len(wallets)} ولت ذخیره‌شده.\n⚠️ جای امن ذخیره کن و پیام رو پاک کن.",
    )
    await c.message.answer("🪪 ولت‌ها", reply_markup=wl_menu_kb())


@router.callback_query(F.data == "wl_delall")
async def cb_wl_delall(c: CallbackQuery):
    await c.answer()
    total = count_wallets()
    if total == 0:
        await c.message.answer("هنوز ولتی ذخیره نشده.", reply_markup=wl_menu_kb())
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("📥 اول فایل بگیر", "wl_export")],
            [btn(f"🗑 حذف همه {total} ولت", "wl_delallyes"), btn("❌ انصراف", "wl_menu")],
        ]
    )
    await c.message.answer(
        f"🚨 حذف <b>همه {total}</b> ولت؟\n"
        "کلمات از ربات پاک می‌شوند و برگشتی ندارد. اگر ولتی پول دارد و فایل نداری، اول فایل بگیر.",
        reply_markup=kb,
    )


@router.callback_query(F.data == "wl_delallyes")
async def cb_wl_delallyes(c: CallbackQuery):
    await c.answer()
    db_exec("DELETE FROM wallets")
    await c.message.answer("✅ همه ولت‌ها حذف شدند.", reply_markup=wl_menu_kb())


# ───────────── هندلرهای آدرس خاص (تا ۵ الگو) ─────────────
async def safe_edit(msg: Message, text: str, kb) -> None:
    try:
        await msg.edit_text(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "not modified" not in str(e):
            await msg.answer(text, reply_markup=kb)


def vn_mode_kb(back: bool) -> InlineKeyboardMarkup:
    last = [btn("❌ انصراف", "cancel")]
    if back:
        last.insert(0, btn("⬅️ برگشت", "vn_back"))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🔚 پایان آدرس", "vn_m:suffix"), btn("🔝 شروع آدرس", "vn_m:prefix")],
            [btn("🔍 داخل آدرس", "vn_m:contains")],
            last,
        ]
    )


def vn_mode_text(patterns: list) -> str:
    slots = VN_MAX_PATTERNS - len(patterns)
    text = "🎯 <b>آدرس خاص</b>\nالگوی بعدی را کجای آدرس می‌خواهی؟\n\n"
    if patterns:
        text += "الگوهای انتخاب‌شده:\n"
        text += "\n".join(
            f"{i}. <code>{html.escape(p)}</code> — {MODE_FA[m]}"
            for i, (m, p) in enumerate(patterns, 1)
        )
        text += "\n\n"
    text += (
        f"ظرفیت باقی‌مانده: {slots} از {VN_MAX_PATTERNS} الگو.\n"
        "پیشنهاد: «پایان آدرس» بهترین و راحت‌ترین حالت است."
    )
    return text


def vn_view(data: dict):
    patterns = data["patterns"]
    tries, cs = data["tries"], data["cs"]
    probs = [vanity_probability(m, p, cs) for m, p in patterns]
    p_any = 1 - math.prod(1 - min(1.0, x) for x in probs)
    expected = tries * sum(probs)
    if p_any >= 1:
        any_p = 1.0
    else:
        any_p = -math.expm1(tries * math.log1p(-p_any))

    plines = []
    for i, ((mode, pat), p) in enumerate(zip(patterns, probs), 1):
        odds = f"۱ از {1 / p:,.0f}" if p else "غیرممکن"
        plines.append(f"{i}. <code>{html.escape(pat)}</code> — {MODE_FA[mode]} — {odds}")

    rows = [
        [btn(("✅ " if tries == t else "") + f"{t:,}", f"vn_t:{t}") for t in VN_TRIES],
        [btn(f"🔠 حساس به حروف: {'✅' if cs else '❌'}", "vn_cs")],
    ]
    if len(patterns) < VN_MAX_PATTERNS:
        rows.append([btn("➕ افزودن الگوی دیگر", "vn_add")])
    rows.append([btn(f"🗑 {i}", f"vn_rm:{i - 1}") for i in range(1, len(patterns) + 1)])
    rows.append([btn("▶️ شروع جستجو", "vn_go"), btn("❌ انصراف", "cancel")])

    speed = VN_STATS.get("speed")
    if speed:
        time_line = f"⏱ زمان تخمینی (حداکثر، طبق سرعت جستجوی قبلی): ~{fmt_dur(tries / speed)}\n"
    else:
        time_line = "⏱ سرعت واقعی بعد از شروع جستجو نمایش داده می‌شود.\n"

    text = (
        "🎯 <b>جستجوی آدرس خاص</b>\n\n"
        f"📌 الگوها ({len(patterns)} از {VN_MAX_PATTERNS}):\n" + "\n".join(plines) + "\n"
        "(با دکمه‌های 🗑 شماره‌دار می‌توانی الگو را حذف کنی)\n\n"
        f"حساس به حروف: {'✅' if cs else '❌ (بزرگ/کوچک فرقی ندارد)'}\n"
        f"تعداد تلاش: <b>{tries:,}</b>\n"
        f"🧵 پردازش هم‌زمان: {VN_WORKERS}\n\n"
        f"🎲 احتمال هر تلاش (جور شدن با حداقل یکی از الگوها): ۱ از {1 / p_any:,.0f}\n"
        f"📊 تعداد مورد انتظار: {expected:.2f}\n"
        f"📈 احتمال پیدا شدن حداقل یکی: {any_p * 100:.1f}%\n"
        f"{time_line}"
    )
    if expected < 0.5:
        text += "\n⚠️ با این تعداد تلاش احتمالا چیزی پیدا نمی‌شود. الگوها را کوتاه‌تر کن یا تعداد را بیشتر.\n"
    text += (
        "\nفقط ولت‌هایی که حداقل با یکی از الگوها جور باشند ذخیره و ارسال می‌شوند، بقیه دور ریخته می‌شوند.\n"
        "هر وقت خواستی با دکمه‌ی «توقف» جستجو را متوقف کن."
    )
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def vn_show(target: Message, state: FSMContext, edit: bool) -> None:
    text, kb = vn_view(await state.get_data())
    if edit:
        await safe_edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data == "vn")
async def cb_vn(c: CallbackQuery, state: FSMContext):
    await c.answer()
    await state.clear()
    await state.update_data(patterns=[], tries=VN_TRIES[0], cs=False)
    await c.message.answer(vn_mode_text([]), reply_markup=vn_mode_kb(back=False))


@router.callback_query(F.data.startswith("vn_m:"))
async def cb_vn_mode(c: CallbackQuery, state: FSMContext):
    mode = c.data.split(":")[1]
    if mode not in MODE_FA:
        await c.answer()
        return
    data = await state.get_data()
    patterns = [list(x) for x in data.get("patterns", [])]
    if len(patterns) >= VN_MAX_PATTERNS:
        await c.answer(f"حداکثر {VN_MAX_PATTERNS} الگو.", show_alert=True)
        return
    await c.answer()
    await state.update_data(
        cur_mode=mode,
        patterns=patterns,
        tries=data.get("tries", VN_TRIES[0]),
        cs=data.get("cs", False),
    )
    await state.set_state(Vanity.pattern)
    slots = VN_MAX_PATTERNS - len(patterns)
    extra = (
        "\n⚠️ حرف اول بعد از UQ فقط می‌تواند یکی از A تا D باشد."
        if mode == "prefix"
        else ""
    )
    kb_rows = [[btn("❌ انصراف", "cancel")]]
    if patterns:
        kb_rows[0].insert(0, btn("⬅️ برگشت", "vn_back"))
    await safe_edit(
        c.message,
        f"✍️ الگو را بفرست ({MODE_FA[mode]}).\n"
        "هر الگو ۱ تا ۸ کاراکتر: حروف انگلیسی، عدد، - و _\n"
        f"می‌توانی چند الگو را با فاصله، ویرگول یا خط جدید جدا کنی؛ ظرفیت باقی‌مانده: {slots} از {VN_MAX_PATTERNS}."
        f"{extra}\n"
        "مثال: <code>TON 777 ABC</code>",
        InlineKeyboardMarkup(inline_keyboard=kb_rows),
    )


@router.message(StateFilter(Vanity.pattern), F.text)
async def st_vn_pattern(m: Message, state: FSMContext):
    data = await state.get_data()
    mode = data.get("cur_mode")
    if mode not in MODE_FA:
        await state.clear()
        await m.answer("⚠️ اطلاعات ناقص بود. دوباره شروع کن.", reply_markup=main_menu())
        return
    patterns = [list(x) for x in data.get("patterns", [])]
    cs = data.get("cs", False)
    slots = VN_MAX_PATTERNS - len(patterns)

    raw = [x for x in re.split(r"[\s,،;؛]+", m.text.strip()) if x]
    if not raw:
        await m.answer("❌ الگویی نفرستادی. دوباره بفرست:", reply_markup=cancel_kb())
        return
    bad = [x for x in raw if not re.fullmatch(r"[A-Za-z0-9_-]{1,8}", x)]
    if bad:
        shown = "، ".join(html.escape(x[:20]) for x in bad[:5])
        await m.answer(
            f"❌ الگوی نامعتبر: {shown}\nفقط حروف انگلیسی، عدد، - و _ (حداکثر ۸ کاراکتر). دوباره بفرست:",
            reply_markup=cancel_kb(),
        )
        return

    norm = (lambda s: s) if cs else (lambda s: s.lower())
    seen = {(pm, norm(pp)) for pm, pp in patterns}
    new = []
    for x in raw:
        key = (mode, norm(x))
        if key in seen:
            continue
        seen.add(key)
        new.append(x)
    if not new:
        await m.answer("❌ این الگوها قبلا اضافه شده‌اند. الگوی دیگری بفرست:", reply_markup=cancel_kb())
        return
    if len(new) > slots:
        await m.answer(
            f"❌ فقط {slots} جای خالی داری ولی {len(new)} الگوی جدید فرستادی. تعدادش را کم کن:",
            reply_markup=cancel_kb(),
        )
        return
    impossible = [x for x in new if vanity_probability(mode, x, cs) == 0]
    if impossible:
        hint = (
            "حرف اول بعد از UQ فقط می‌تواند A تا D باشد"
            + (" (در حالت حساس به حروف، بزرگ)" if cs else "")
        )
        await m.answer(
            f"❌ {hint}. الگوی نامعتبر: {'، '.join(html.escape(x) for x in impossible)}\nدوباره بفرست:",
            reply_markup=cancel_kb(),
        )
        return

    patterns += [[mode, x] for x in new]
    await state.update_data(patterns=patterns, tries=data.get("tries", VN_TRIES[0]), cs=cs)
    await state.set_state(Vanity.confirm)
    await vn_show(m, state, edit=False)


@router.callback_query(F.data == "vn_back", StateFilter(Vanity.confirm, Vanity.pattern))
async def cb_vn_back(c: CallbackQuery, state: FSMContext):
    await c.answer()
    data = await state.get_data()
    if not data.get("patterns"):
        await safe_edit(c.message, vn_mode_text([]), vn_mode_kb(back=False))
        return
    await state.set_state(Vanity.confirm)
    await vn_show(c.message, state, edit=True)


@router.callback_query(F.data == "vn_add", StateFilter(Vanity.confirm))
async def cb_vn_add(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    patterns = data.get("patterns", [])
    if len(patterns) >= VN_MAX_PATTERNS:
        await c.answer(f"حداکثر {VN_MAX_PATTERNS} الگو.", show_alert=True)
        return
    await c.answer()
    await safe_edit(c.message, vn_mode_text(patterns), vn_mode_kb(back=True))


@router.callback_query(F.data.startswith("vn_rm:"), StateFilter(Vanity.confirm))
async def cb_vn_rm(c: CallbackQuery, state: FSMContext):
    await c.answer()
    try:
        idx = int(c.data.split(":")[1])
    except ValueError:
        return
    data = await state.get_data()
    patterns = [list(x) for x in data.get("patterns", [])]
    if not 0 <= idx < len(patterns):
        return
    patterns.pop(idx)
    await state.update_data(patterns=patterns)
    if patterns:
        await vn_show(c.message, state, edit=True)
    else:
        await safe_edit(c.message, vn_mode_text([]), vn_mode_kb(back=False))


@router.callback_query(F.data.startswith("vn_t:"), StateFilter(Vanity.confirm))
async def cb_vn_tries(c: CallbackQuery, state: FSMContext):
    await c.answer()
    t = int(c.data.split(":")[1])
    if t in VN_TRIES:
        await state.update_data(tries=t)
        await vn_show(c.message, state, edit=True)


@router.callback_query(F.data == "vn_cs", StateFilter(Vanity.confirm))
async def cb_vn_cs(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    new_cs = not data["cs"]
    if any(vanity_probability(m, p, new_cs) == 0 for m, p in data["patterns"]):
        await c.answer("با این حالت یکی از الگوها امکان‌پذیر نیست.", show_alert=True)
        return
    await c.answer()
    await state.update_data(cs=new_cs)
    await vn_show(c.message, state, edit=True)


@router.callback_query(F.data == "vn_stop")
async def cb_vn_stop(c: CallbackQuery):
    cur = VN_PROGRESS.get("cur")
    if cur:
        cur["stop"] = True
    await c.answer("در حال توقف…")


def vn_summary_text(patterns: list, r: dict) -> str:
    if r["stopped"]:
        head = "⏹ جستجو متوقف شد."
    elif r["capped"]:
        head = f"🛑 به سقف {VN_MAX_FOUND} نتیجه رسید؛ جستجو متوقف شد."
    else:
        head = "✅ جستجو تمام شد."
    speed = r["checked"] / r["elapsed"] if r["elapsed"] > 0 else 0.0
    lines = [
        head,
        "",
        f"📊 بررسی‌شده: {r['checked']:,}",
        f"⏱ زمان: {fmt_dur(r['elapsed'])}",
        f"⚡ میانگین سرعت: {speed:.1f} آدرس در ثانیه ({r['workers']} پردازش)",
        f"🎯 مجموع پیدا شده: <b>{len(r['found'])}</b>",
    ]
    for (mode, pat), n in zip(patterns, r["counts"]):
        lines.append(f"{'✅' if n else '❌'} <code>{html.escape(pat)}</code> ({MODE_SHORT[mode]}): {n}")
    if r.get("warn"):
        lines.append("\n⚠️ بعضی پردازش‌ها حین کار خطا دادند (نتایج ثبت‌شده معتبرند).")
    return "\n".join(lines)


@router.callback_query(F.data == "vn_go", StateFilter(Vanity.confirm))
async def cb_vn_go(c: CallbackQuery, state: FSMContext):
    await c.answer()
    data = await state.get_data()
    patterns = [(m, p) for m, p in data.get("patterns", [])]
    if not patterns:
        await c.message.answer("❌ هیچ الگویی انتخاب نشده.", reply_markup=cancel_kb())
        return
    tries, cs = data["tries"], data["cs"]
    await state.clear()
    if SEARCH_LOCK.locked():
        await c.message.answer("⏳ یک جستجوی دیگر در حال انجام است. صبر کن تمام شود.")
        return
    progress = {"stop": False}
    VN_PROGRESS["cur"] = progress
    stop_kb = InlineKeyboardMarkup(inline_keyboard=[[btn("⏹ توقف", "vn_stop")]])
    status = await c.message.answer(
        f"⏳ در حال راه‌اندازی {VN_WORKERS} پردازش…", reply_markup=stop_kb
    )
    async with SEARCH_LOCK:
        r = await vn_run(patterns, cs, tries, progress, status, stop_kb)
    VN_PROGRESS.pop("cur", None)

    if r["error"]:
        await status.edit_text(
            f"❌ جستجو ناموفق بود:\n<code>{html.escape(r['error'])[:1500]}</code>"
        )
        await c.message.answer("🪪 ولت‌ها", reply_markup=wl_menu_kb())
        return
    found = r["found"]
    summary = vn_summary_text(patterns, r)
    if not found:
        await status.edit_text(
            summary + "\n\nآدرسی پیدا نشد. الگوها را کوتاه‌تر کن یا تعداد تلاش را بیشتر."
        )
        await c.message.answer("🪪 ولت‌ها", reply_markup=wl_menu_kb())
        return
    save_wallets(found)
    await status.edit_text(summary)
    await send_wallet_files(
        c.message,
        found,
        f"🎯 {len(found)} آدرس خاص (آدرس + ۲۴ کلمه).\n"
        "⚠️ فایل را جای امن ذخیره کن و بعدش پیام را از چت پاک کن.",
        r["labels"],
    )
    if len(found) <= 10:
        body = "\n\n".join(
            f"<b>#{i}</b> — {html.escape(r['labels'].get(a, ''))}\n<code>{a}</code>\n<code>{w}</code>"
            for i, (a, w) in enumerate(found, 1)
        )
        shown = await c.message.answer(body + "\n\n⚠️ این پیام بعد از ۲ دقیقه پاک می‌شود.")
        asyncio.create_task(delete_later(shown, 120))
    await c.message.answer("🪪 ولت‌ها", reply_markup=wl_menu_kb())


@router.message()
async def fallback(m: Message, state: FSMContext):
    if await state.get_state() is None:
        await m.answer("از منو استفاده کن 👇", reply_markup=main_menu())


# ───────────────────────── اجرا ─────────────────────────
async def health(_request):
    return web.Response(text="ok")


async def start_web():
    port = os.getenv("PORT")  # Render روی Web Service این رو خودش می‌گذارد
    if not port:
        return
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(port)).start()


async def main():
    init_db()
    await start_web()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(guard)
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Bot started | wallet %s | owner %s", ADDRESS, OWNER_ID or "NOT SET")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
