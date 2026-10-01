import asyncio
import importlib
import inspect
import multiprocessing as mp
import pkgutil
import re
import time
from typing import List, Tuple, Optional


# ───────────────────────── توابع زیرساختی TON ─────────────────────────
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


# ───────────────────────── بررسی رند بودن آدرس ─────────────────────────
def is_pretty_address(
    addr: str, target_word: Optional[str] = None, min_repeat: int = 4
) -> Tuple[bool, str]:
    """
    بررسی می‌کند آیا آدرس شرایط «خوشگل/رند» بودن را دارد یا خیر.
    """
    # حذف پیشوند‌های عمومی UQ یا EQ برای بررسی بهتر
    clean_addr = addr[2:] if addr.startswith(("UQ", "EQ")) else addr

    # ۱. اگر کلمه خاصی مدنظر باشد (Case-Insensitive)
    if target_word and target_word.lower() in clean_addr.lower():
        return True, f"حاوی کلمه/اسم '{target_word}'"

    # ۲. کاراکترهای تکراری پشت سر هم (مثلاً 7777 یا AAAA)
    pattern_repeat = rf"(.)\1{{{min_repeat - 1},}}"
    match = re.search(pattern_repeat, clean_addr)
    if match:
        return True, f"کاراکتر تکراری '{match.group(0)}'"

    # ۳. شروع یا پایان با عدد/حرف یکسان به تعداد زیاد
    if len(set(clean_addr[:min_repeat])) == 1:
        return True, f"شروع رند با '{clean_addr[:min_repeat]}'"

    return False, ""


# ───────────────────────── پردازش موازی ─────────────────────────
def worker_task(
    attempts_per_worker: int,
    target_word: Optional[str],
    queue: mp.Queue,
):
    try:
        cls = _find_wallet_class()
    except Exception as e:
        queue.put([])
        return

    found = []
    for _ in range(attempts_per_worker):
        addr, words = _create_one(cls)
        pretty, reason = is_pretty_address(addr, target_word=target_word)
        if pretty:
            found.append((addr, words, reason))

    queue.put(found)


def search_vanity_wallets(
    total_attempts: int, target_word: Optional[str] = None
) -> List[Tuple[str, str, str]]:
    num_cores = mp.cpu_count()
    attempts_per_worker = total_attempts // num_cores
    remainder = total_attempts % num_cores

    print(
        f"⚡ شروع جستجو بین {total_attempts:,} حالت روی {num_cores} هسته پردازشی CPU..."
    )

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
    print(
        f"⏱️ زمان کل: {elapsed:.2f} ثانیه | سرعت: {total_attempts / elapsed:.1f} آدرس/ثانیه\n"
    )

    return all_results


# ───────────────────────── اجرای اصلی ─────────────────────────
if __name__ == "__main__":
    # 🔴 تنظیمات شما:
    TOTAL_ATTEMPTS = 1000  # تعداد کل تلاش‌ها
    TARGET_WORD = "SADRA"  # کلمه یا اسمی که دوست دارید در آدرس باشد (یا None بگذارید)

    print("=" * 60)
    print("🔍 جستجوگر آدرس‌های رند و اسم‌دار TON (V5R1)")
    print("=" * 60)

    results = search_vanity_wallets(
        total_attempts=TOTAL_ATTEMPTS, target_word=TARGET_WORD
    )

    if results:
        print(
            f"✅ پیدا شد! تعداد {len(results)} آدرس خوشگل/رند از بین {TOTAL_ATTEMPTS:,} تلاش:\n"
        )
        for i, (addr, words, reason) in enumerate(results, 1):
            print(f"📌 ولت رند #{i} ({reason}):")
            print(f"آدرس:    {addr}")
            print(f"۲۴ کلمه: {words}")
            print("-" * 60)
    else:
        print(
            f"❌ در {TOTAL_ATTEMPTS:,} تلاش هیچ آدرس رندی پیدا نشد. تعداد تلاش را بیشتر کنید (مثلاً ۵۰,۰۰۰ بار)."
        )
