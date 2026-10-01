import asyncio
import html
import io
import logging
import math
import os
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
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiohttp import web
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


async def get_balance() -> Decimal:
    nano = await call("getAddressBalance", {"address": ADDRESS})
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


def btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("💰 موجودی", "balance"), btn("📍 آدرس", "address")],
            [btn("📤 ارسال (تکی / گروهی)", "multi")],
            [btn("📜 تاریخچه", "history")],
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
