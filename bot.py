import asyncio
import base64
import hashlib
import html
import io
import logging
import math
import os
import sqlite3
import time
from decimal import Decimal, InvalidOperation

import httpx
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


def generate_wallets(n: int):
    """n ولت V5R1 با ۲۴ کلمه می‌سازد. خروجی: لیست (address, mnemonic)."""
    import inspect

    cls = _find_wallet_class()
    result = []
    for _ in range(n):
        try:
            res = cls.create(None)
        except (TypeError, AttributeError):
            res = cls.create()
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
        result.append((addr, words))
    return result


async def delete_later(msg: Message, seconds: int) -> None:
    await asyncio.sleep(seconds)
    try:
        await msg.delete()
    except Exception:
        pass


async def send_wallet_files(m: Message, wallets: list, caption: str) -> None:
    txt = "\n".join(
        f"#{i}\nAddress: {a}\nMnemonic: {w}\n" for i, (a, w) in enumerate(wallets, 1)
    )
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
            [btn("➕ ساخت ولت جدید", "gen")],
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
            f"❌ ساخت ناموفق بود:\n<code>{html.escape(repr(e))[:300]}</code>"
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
