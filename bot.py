import os
import sqlite3
import logging
import threading
from datetime import date
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

# ─── FLASK (Koyeb port listener) ──────────────────────────
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Bot is running!"

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

# ─── CONFIG ────────────────────────────────────────────────
BOT_TOKEN    = os.getenv("BOT_TOKEN", "8799884484:AAGnw6GFz05jNHn9cxhEnd9CLPAxtiteEYY")
CHANNEL_ID   = "@apnahub69"
ADMIN_ID     = 7224810102
MIN_WITHDRAW = 10
REFER_REWARD = 1
CHECKIN_AMT  = 0.10

# Milestone: {referral_count: bonus_amount}
MILESTONES = {
    10: 5,
    25: 15,
    50: 30,
}
# ───────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════

def get_conn():
    conn = sqlite3.connect("bot.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id        INTEGER PRIMARY KEY,
            username       TEXT,
            full_name      TEXT,
            balance        REAL    DEFAULT 0,
            referred_by    INTEGER DEFAULT NULL,
            upi_id         TEXT    DEFAULT NULL,
            is_banned      INTEGER DEFAULT 0,
            last_checkin   TEXT    DEFAULT NULL,
            joined_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS withdrawals (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER,
            amount       REAL,
            upi_id       TEXT,
            status       TEXT DEFAULT 'pending',
            requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS milestone_claimed (
            user_id   INTEGER,
            milestone INTEGER,
            PRIMARY KEY (user_id, milestone)
        )
    """)

    # Migration: add new columns if old db exists
    for col, definition in [
        ("is_banned",    "INTEGER DEFAULT 0"),
        ("last_checkin", "TEXT DEFAULT NULL"),
    ]:
        try:
            c.execute(f"ALTER TABLE users ADD COLUMN {col} {definition}")
        except Exception:
            pass

    conn.commit()
    conn.close()


# ═══════════════════════════════════════════════════════════
#  DB HELPERS
# ═══════════════════════════════════════════════════════════

def get_user(user_id: int):
    conn = get_conn()
    row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    conn.close()
    return row


def add_user(user_id: int, username: str, full_name: str, referred_by: int = None):
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO users (user_id, username, full_name, referred_by) VALUES (?,?,?,?)",
        (user_id, username, full_name, referred_by)
    )
    conn.commit()
    conn.close()


def get_referral_count(user_id: int) -> int:
    conn = get_conn()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM users WHERE referred_by = ? AND is_banned = 0",
        (user_id,)
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def credit_balance(user_id: int, amount: float):
    conn = get_conn()
    conn.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()
    conn.close()


def create_withdrawal(user_id: int, amount: float, upi_id: str):
    conn = get_conn()
    conn.execute(
        "INSERT INTO withdrawals (user_id, amount, upi_id) VALUES (?,?,?)",
        (user_id, amount, upi_id)
    )
    conn.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount, user_id))
    conn.commit()
    conn.close()


def get_leaderboard():
    conn = get_conn()
    rows = conn.execute("""
        SELECT u.user_id, u.full_name, u.username,
               COUNT(r.user_id) as ref_count
        FROM users u
        LEFT JOIN users r ON r.referred_by = u.user_id AND r.is_banned = 0
        WHERE u.is_banned = 0
        GROUP BY u.user_id
        ORDER BY ref_count DESC
        LIMIT 10
    """).fetchall()
    conn.close()
    return rows


def get_withdrawal_history(user_id: int):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM withdrawals WHERE user_id = ? ORDER BY requested_at DESC LIMIT 10",
        (user_id,)
    ).fetchall()
    conn.close()
    return rows


def check_and_claim_milestones(user_id: int) -> list:
    ref_count = get_referral_count(user_id)
    conn = get_conn()
    claimed = []
    for milestone, bonus in MILESTONES.items():
        if ref_count >= milestone:
            try:
                conn.execute(
                    "INSERT INTO milestone_claimed (user_id, milestone) VALUES (?,?)",
                    (user_id, milestone)
                )
                conn.execute(
                    "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                    (bonus, user_id)
                )
                claimed.append((milestone, bonus))
            except Exception:
                pass
    conn.commit()
    conn.close()
    return claimed


def do_checkin(user_id: int) -> bool:
    today = str(date.today())
    conn  = get_conn()
    row   = conn.execute("SELECT last_checkin FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if row and row["last_checkin"] == today:
        conn.close()
        return False
    conn.execute(
        "UPDATE users SET last_checkin = ?, balance = balance + ? WHERE user_id = ?",
        (today, CHECKIN_AMT, user_id)
    )
    conn.commit()
    conn.close()
    return True


# ═══════════════════════════════════════════════════════════
#  UI HELPERS
# ═══════════════════════════════════════════════════════════

async def is_member(user_id: int, bot) -> bool:
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception:
        return False


def not_joined_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Channel Join Karo", url="https://t.me/apnahub69")],
        [InlineKeyboardButton("✅ Maine Join Kar Liya", callback_data="verify_join")]
    ])


def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💰 Balance",        callback_data="balance"),
            InlineKeyboardButton("🔗 Refer Link",     callback_data="refer"),
        ],
        [
            InlineKeyboardButton("🏆 Leaderboard",    callback_data="leaderboard"),
            InlineKeyboardButton("🎯 Milestones",     callback_data="milestones"),
        ],
        [
            InlineKeyboardButton("📅 Daily Check-in", callback_data="checkin"),
            InlineKeyboardButton("💸 Withdraw",       callback_data="withdraw"),
        ]
    ])


async def send_main_menu(target, context, user_id: int, edit: bool = False):
    user      = get_user(user_id)
    balance   = user["balance"] if user else 0
    ref_count = get_referral_count(user_id)
    bot_me    = await context.bot.get_me()
    ref_link  = f"https://t.me/{bot_me.username}?start={user_id}"
    today     = str(date.today())
    checkin_done = user and user["last_checkin"] == today

    next_ms  = next((m for m in sorted(MILESTONES) if ref_count < m), None)
    ms_text  = f"🎯 Next Milestone: {next_ms} refers → ₹{MILESTONES[next_ms]}" if next_ms else "🎯 Saare milestones complete! 🏆"

    text = (
        f"🏠 *Main Menu*\n\n"
        f"💰 Balance: ₹{balance:.2f}\n"
        f"👥 Total Referrals: {ref_count}\n"
        f"📅 Check-in: {'✅ Done' if checkin_done else '❌ Available!'}\n\n"
        f"{ms_text}\n\n"
        f"🔗 Tera Referral Link:\n`{ref_link}`\n\n"
        f"━━━━━━━━━━━━━\n"
        f"📌 *Rules:*\n"
        f"• 1 Refer = ₹{REFER_REWARD}\n"
        f"• Daily Check-in = ₹{CHECKIN_AMT}\n"
        f"• Min Withdrawal = ₹{MIN_WITHDRAW}\n"
        f"• UPI se payment hogi"
    )

    if edit:
        await target.edit_message_text(text, reply_markup=main_menu_keyboard(), parse_mode="Markdown")
    else:
        await target.reply_text(text, reply_markup=main_menu_keyboard(), parse_mode="Markdown")


# ═══════════════════════════════════════════════════════════
#  HANDLERS — USER
# ═══════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    user_id = user.id

    referred_by = None
    if context.args:
        try:
            ref_id = int(context.args[0])
            if ref_id != user_id:
                referred_by = ref_id
        except ValueError:
            pass

    existing = get_user(user_id)
    is_new   = existing is None
    add_user(user_id, user.username or "", user.full_name or "", referred_by)

    db_user = get_user(user_id)
    if db_user and db_user["is_banned"]:
        await update.message.reply_text("🚫 Tumhara account ban kar diya gaya hai.")
        return

    if is_new and referred_by:
        referrer = get_user(referred_by)
        if referrer and not referrer["is_banned"]:
            credit_balance(referred_by, REFER_REWARD)
            new_milestones = check_and_claim_milestones(referred_by)
            try:
                ms_text = ""
                if new_milestones:
                    ms_text = "\n\n🎉 *Milestone Bonus!*\n" + "\n".join(
                        f"• {m} refers → ₹{b} bonus!" for m, b in new_milestones
                    )
                await context.bot.send_message(
                    referred_by,
                    f"🎉 *Naya Referral!*\n\n"
                    f"👤 {user.full_name} ne join kiya.\n"
                    f"💰 ₹{REFER_REWARD} add ho gaya!{ms_text}",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

    joined = await is_member(user_id, context.bot)
    if not joined:
        await update.message.reply_text(
            "👋 *Welcome!*\n\n"
            "⚠️ Bot use karne ke liye *pehle channel join karo*:\n\n"
            "👇 Join karo, phir ✅ button dabao.",
            reply_markup=not_joined_keyboard(),
            parse_mode="Markdown"
        )
        return

    await send_main_menu(update.message, context, user_id)


async def cb_verify_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    joined = await is_member(user_id, context.bot)
    if not joined:
        await query.answer("❌ Abhi join nahi kiya! Pehle join karo.", show_alert=True)
        return
    await send_main_menu(query.message, context, user_id, edit=True)


async def cb_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    user = get_user(user_id)
    if user and user["is_banned"]:
        await query.answer("🚫 Account banned.", show_alert=True)
        return

    balance   = user["balance"] if user else 0
    ref_count = get_referral_count(user_id)
    history   = get_withdrawal_history(user_id)

    hist_text = ""
    if history:
        hist_text = "\n\n📋 *Withdrawal History:*\n"
        for h in history[:5]:
            emoji = "✅" if h["status"] == "paid" else "⏳"
            hist_text += f"{emoji} ₹{h['amount']:.2f} → `{h['upi_id']}` ({h['status']})\n"

    await query.message.reply_text(
        f"💰 *Tera Balance*\n\n"
        f"Current Balance: ₹{balance:.2f}\n"
        f"Total Referrals: {ref_count}\n"
        f"Total Earned (refer): ₹{ref_count * REFER_REWARD:.2f}\n\n"
        f"Min Withdrawal: ₹{MIN_WITHDRAW}"
        f"{hist_text}",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Menu", callback_data="menu")]]
        )
    )


async def cb_refer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    bot_me    = await context.bot.get_me()
    ref_link  = f"https://t.me/{bot_me.username}?start={user_id}"
    ref_count = get_referral_count(user_id)

    next_ms  = next((m for m in sorted(MILESTONES) if ref_count < m), None)
    ms_text  = f"\n🎯 Next: {next_ms} refers → ₹{MILESTONES[next_ms]} bonus!" if next_ms else "\n🏆 Saare milestones complete!"

    await query.message.reply_text(
        f"🔗 *Tera Referral Link:*\n\n"
        f"`{ref_link}`\n\n"
        f"👥 Total Referrals: {ref_count}\n"
        f"💰 Earned: ₹{ref_count * REFER_REWARD:.2f}{ms_text}\n\n"
        f"_Yeh link share karo — jab koi join kare toh ₹{REFER_REWARD} milega!_",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Menu", callback_data="menu")]]
        )
    )


async def cb_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    rows   = get_leaderboard()
    medals = ["🥇", "🥈", "🥉"]
    text   = "🏆 *Top Referrers Leaderboard*\n\n"

    for i, row in enumerate(rows):
        medal = medals[i] if i < 3 else f"{i+1}."
        name  = row["full_name"] or "Unknown"
        uname = f"@{row['username']}" if row["username"] else ""
        text += f"{medal} {name} {uname} — *{row['ref_count']} refers*\n"

    if not rows:
        text += "_Abhi koi data nahi hai._"

    my_rank = next((i+1 for i, r in enumerate(rows) if r["user_id"] == user_id), None)
    if my_rank:
        text += f"\n📍 *Teri Rank: #{my_rank}*"

    await query.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Menu", callback_data="menu")]]
        )
    )


async def cb_milestones(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    ref_count = get_referral_count(user_id)
    conn = get_conn()
    claimed_rows = conn.execute(
        "SELECT milestone FROM milestone_claimed WHERE user_id = ?", (user_id,)
    ).fetchall()
    conn.close()
    claimed_set = {r["milestone"] for r in claimed_rows}

    text = f"🎯 *Milestone Rewards*\n\n👥 Tere Referrals: {ref_count}\n\n"
    for m, bonus in sorted(MILESTONES.items()):
        if m in claimed_set:
            status = "✅ Claimed"
        elif ref_count >= m:
            status = "🟡 Claim pending"
        else:
            remaining = m - ref_count
            status = f"🔒 {remaining} aur chahiye"
        text += f"*{m} Refers → ₹{bonus}* — {status}\n"

    await query.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Menu", callback_data="menu")]]
        )
    )


async def cb_checkin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    user = get_user(user_id)
    if user and user["is_banned"]:
        await query.answer("🚫 Account banned.", show_alert=True)
        return

    success = do_checkin(user_id)
    if success:
        updated = get_user(user_id)
        await query.message.reply_text(
            f"📅 *Daily Check-in Complete!*\n\n"
            f"✅ ₹{CHECKIN_AMT} add ho gaya!\n"
            f"💰 New Balance: ₹{updated['balance']:.2f}\n\n"
            f"_Kal dobara aao! 🎉_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Menu", callback_data="menu")]]
            )
        )
    else:
        await query.answer(
            "⏰ Aaj ka check-in ho chuka hai!\nKal dobara aao.",
            show_alert=True
        )


async def cb_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    await query.answer()

    user    = get_user(user_id)
    if user and user["is_banned"]:
        await query.answer("🚫 Account banned.", show_alert=True)
        return

    balance = user["balance"] if user else 0

    if balance < MIN_WITHDRAW:
        needed = MIN_WITHDRAW - balance
        await query.message.reply_text(
            f"❌ *Insufficient Balance*\n\n"
            f"Tera Balance: ₹{balance:.2f}\n"
            f"Minimum: ₹{MIN_WITHDRAW:.2f}\n"
            f"Aur Chahiye: ₹{needed:.2f}\n\n"
            f"_Aur refer karo!_ 💪",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🏠 Menu", callback_data="menu")]]
            )
        )
        return

    context.user_data["awaiting_upi"]    = True
    context.user_data["withdraw_amount"] = balance

    await query.message.reply_text(
        f"💸 *Withdrawal Request*\n\n"
        f"Amount: ₹{balance:.2f}\n\n"
        f"✏️ Apna *UPI ID* type karke bhejo:\n"
        f"_(Example: name@paytm / 9999999999@upi)_\n\n"
        f"Cancel: /cancel",
        parse_mode="Markdown"
    )


async def cb_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    user_id = query.from_user.id
    await query.answer()
    await send_main_menu(query.message, context, user_id, edit=True)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    user_id = user.id
    text    = update.message.text.strip()

    if text == "/cancel":
        context.user_data.pop("awaiting_upi", None)
        context.user_data.pop("withdraw_amount", None)
        await update.message.reply_text("❌ Withdrawal cancel kar di.")
        return

    if not context.user_data.get("awaiting_upi"):
        return

    if len(text) < 5 or " " in text:
        await update.message.reply_text(
            "⚠️ Valid UPI ID dalo.\n_(Example: name@paytm)_",
            parse_mode="Markdown"
        )
        return

    upi_id  = text
    amount  = context.user_data.get("withdraw_amount", 0)
    db_user = get_user(user_id)

    if not db_user or db_user["balance"] < MIN_WITHDRAW:
        await update.message.reply_text("❌ Balance kam ho gaya. Dobara try karo.")
        context.user_data.pop("awaiting_upi", None)
        return

    create_withdrawal(user_id, amount, upi_id)
    context.user_data.pop("awaiting_upi", None)
    context.user_data.pop("withdraw_amount", None)

    await update.message.reply_text(
        f"✅ *Withdrawal Request Submit Ho Gayi!*\n\n"
        f"💰 Amount: ₹{amount:.2f}\n"
        f"📲 UPI ID: `{upi_id}`\n\n"
        f"_Admin 24 hours mein process karega._ 🕐",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🏠 Menu", callback_data="menu")]]
        )
    )

    username_display = f"@{user.username}" if user.username else "_(no username)_"
    try:
        await context.bot.send_message(
            ADMIN_ID,
            f"🚨 *Naya Withdrawal Request!*\n\n"
            f"👤 Name: {user.full_name}\n"
            f"🆔 Username: {username_display}\n"
            f"🔢 User ID: `{user_id}`\n"
            f"💰 Amount: ₹{amount:.2f}\n"
            f"📲 UPI ID: `{upi_id}`\n\n"
            f"Approve: /pending",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Admin notify error: {e}")


# ═══════════════════════════════════════════════════════════
#  ADMIN COMMANDS
# ═══════════════════════════════════════════════════════════

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_ID:
            return
        await func(update, context)
    return wrapper


@admin_only
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_conn()
    total_users   = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    banned_users  = conn.execute("SELECT COUNT(*) as c FROM users WHERE is_banned=1").fetchone()["c"]
    pending_w     = conn.execute(
        "SELECT COUNT(*) as c, COALESCE(SUM(amount),0) as s FROM withdrawals WHERE status='pending'"
    ).fetchone()
    total_paid    = conn.execute(
        "SELECT COALESCE(SUM(amount),0) as s FROM withdrawals WHERE status='paid'"
    ).fetchone()
    checkin_today = conn.execute(
        "SELECT COUNT(*) as c FROM users WHERE last_checkin = ?", (str(date.today()),)
    ).fetchone()["c"]
    conn.close()

    await update.message.reply_text(
        f"📊 *Bot Stats*\n\n"
        f"👥 Total Users: {total_users}\n"
        f"🚫 Banned: {banned_users}\n"
        f"📅 Check-ins Today: {checkin_today}\n\n"
        f"⏳ Pending Withdrawals: {pending_w['c']}\n"
        f"💸 Pending Amount: ₹{pending_w['s']:.2f}\n"
        f"✅ Total Paid Out: ₹{total_paid['s']:.2f}",
        parse_mode="Markdown"
    )


@admin_only
async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_conn()
    rows = conn.execute(
        "SELECT w.id, w.user_id, w.amount, w.upi_id, w.requested_at, "
        "u.full_name, u.username FROM withdrawals w "
        "JOIN users u ON w.user_id = u.user_id "
        "WHERE w.status='pending' ORDER BY w.requested_at DESC LIMIT 10"
    ).fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("✅ Koi pending withdrawal nahi hai!")
        return

    msg = "⏳ *Pending Withdrawals:*\n\n"
    for r in rows:
        uname = f"@{r['username']}" if r['username'] else f"ID:{r['user_id']}"
        msg += (
            f"#{r['id']} | {r['full_name']} ({uname})\n"
            f"   ₹{r['amount']:.2f} → `{r['upi_id']}`\n"
            f"   /paid {r['id']}\n\n"
        )

    await update.message.reply_text(msg, parse_mode="Markdown")


@admin_only
async def cmd_paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /paid <withdrawal_id>")
        return
    try:
        wid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Valid ID dalo.")
        return

    conn = get_conn()
    row  = conn.execute("SELECT * FROM withdrawals WHERE id = ?", (wid,)).fetchone()
    if not row:
        await update.message.reply_text(f"❌ #{wid} nahi mila.")
        conn.close()
        return

    conn.execute("UPDATE withdrawals SET status='paid' WHERE id = ?", (wid,))
    conn.commit()
    conn.close()

    try:
        await context.bot.send_message(
            row["user_id"],
            f"✅ *Tera Withdrawal Process Ho Gaya!*\n\n"
            f"💰 Amount: ₹{row['amount']:.2f}\n"
            f"📲 UPI: `{row['upi_id']}`\n\n"
            f"Payment bhej di gayi! Check karo 🎉",
            parse_mode="Markdown"
        )
    except Exception:
        pass

    await update.message.reply_text(f"✅ Withdrawal #{wid} paid mark kar diya!")


@admin_only
async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage:\n/ban <user_id> — ban karo\n/ban <user_id> unban — hatao"
        )
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Valid user ID dalo.")
        return

    unban  = len(context.args) > 1 and context.args[1].lower() == "unban"
    is_ban = 0 if unban else 1

    conn = get_conn()
    row  = conn.execute("SELECT * FROM users WHERE user_id = ?", (target_id,)).fetchone()
    if not row:
        await update.message.reply_text("❌ User nahi mila.")
        conn.close()
        return

    conn.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (is_ban, target_id))
    conn.commit()
    conn.close()

    await update.message.reply_text(
        f"{'✅ Unban' if unban else '🚫 Ban'} kar diya!\n\n"
        f"👤 {row['full_name']} | 🆔 {target_id}"
    )

    try:
        msg = (
            "✅ *Tera account restore kar diya gaya.*" if unban
            else "🚫 *Tera account ban kar diya gaya hai.*\nAdmin se contact karo."
        )
        await context.bot.send_message(target_id, msg, parse_mode="Markdown")
    except Exception:
        pass


@admin_only
async def cmd_addbalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addbalance <user_id> <amount>")
        return

    try:
        target_id = int(context.args[0])
        amount    = float(context.args[1])
    except ValueError:
        await update.message.reply_text("Valid user_id aur amount dalo.")
        return

    if amount <= 0:
        await update.message.reply_text("Amount positive hona chahiye.")
        return

    row = get_user(target_id)
    if not row:
        await update.message.reply_text("❌ User nahi mila.")
        return

    credit_balance(target_id, amount)
    updated = get_user(target_id)

    await update.message.reply_text(
        f"✅ *Balance Add Ho Gaya!*\n\n"
        f"👤 {row['full_name']}\n"
        f"➕ Added: ₹{amount:.2f}\n"
        f"💰 New Balance: ₹{updated['balance']:.2f}",
        parse_mode="Markdown"
    )

    try:
        await context.bot.send_message(
            target_id,
            f"🎁 *Admin ne tera balance badha diya!*\n\n"
            f"➕ ₹{amount:.2f} add hua\n"
            f"💰 New Balance: ₹{updated['balance']:.2f}",
            parse_mode="Markdown"
        )
    except Exception:
        pass


@admin_only
async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /history <user_id>")
        return

    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Valid user_id dalo.")
        return

    row = get_user(target_id)
    if not row:
        await update.message.reply_text("❌ User nahi mila.")
        return

    history   = get_withdrawal_history(target_id)
    ref_count = get_referral_count(target_id)

    msg = (
        f"📋 *{row['full_name']} ki Detail*\n"
        f"🆔 {target_id}\n"
        f"💰 Balance: ₹{row['balance']:.2f}\n"
        f"👥 Referrals: {ref_count}\n"
        f"🚫 Banned: {'Yes' if row['is_banned'] else 'No'}\n\n"
    )

    if history:
        msg += "*Withdrawal History:*\n"
        for h in history:
            emoji = "✅" if h["status"] == "paid" else "⏳"
            msg += f"{emoji} #{h['id']} ₹{h['amount']:.2f} → `{h['upi_id']}` ({h['status']})\n"
    else:
        msg += "_Koi withdrawal history nahi._"

    await update.message.reply_text(msg, parse_mode="Markdown")


@admin_only
async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage:\n`/broadcast Aapka message yahan`",
            parse_mode="Markdown"
        )
        return

    broadcast_text = " ".join(context.args)
    conn = get_conn()
    all_users = conn.execute("SELECT user_id FROM users WHERE is_banned = 0").fetchall()
    conn.close()

    total   = len(all_users)
    success = 0
    failed  = 0

    status_msg = await update.message.reply_text(f"📤 Bhej raha hoon...\n0/{total}")

    for row in all_users:
        uid = row["user_id"]
        try:
            await context.bot.send_message(
                uid,
                f"📢 *Admin ka Message:*\n\n{broadcast_text}",
                parse_mode="Markdown"
            )
            success += 1
        except Exception:
            failed += 1

        if (success + failed) % 50 == 0:
            try:
                await status_msg.edit_text(
                    f"📤 Chal raha hai...\n{success+failed}/{total}\n✅ {success} | ❌ {failed}"
                )
            except Exception:
                pass

    await status_msg.edit_text(
        f"✅ *Broadcast Complete!*\n\n"
        f"👥 Total: {total}\n✅ Sent: {success}\n❌ Failed: {failed}",
        parse_mode="Markdown"
    )


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════

def main():
    init_db()
    logger.info("Database initialized ✅")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("stats",      cmd_stats))
    app.add_handler(CommandHandler("pending",    cmd_pending))
    app.add_handler(CommandHandler("paid",       cmd_paid))
    app.add_handler(CommandHandler("ban",        cmd_ban))
    app.add_handler(CommandHandler("addbalance", cmd_addbalance))
    app.add_handler(CommandHandler("history",    cmd_history))
    app.add_handler(CommandHandler("broadcast",  cmd_broadcast))

    app.add_handler(CallbackQueryHandler(cb_verify_join, pattern="^verify_join$"))
    app.add_handler(CallbackQueryHandler(cb_balance,     pattern="^balance$"))
    app.add_handler(CallbackQueryHandler(cb_refer,       pattern="^refer$"))
    app.add_handler(CallbackQueryHandler(cb_leaderboard, pattern="^leaderboard$"))
    app.add_handler(CallbackQueryHandler(cb_milestones,  pattern="^milestones$"))
    app.add_handler(CallbackQueryHandler(cb_checkin,     pattern="^checkin$"))
    app.add_handler(CallbackQueryHandler(cb_withdraw,    pattern="^withdraw$"))
    app.add_handler(CallbackQueryHandler(cb_menu,        pattern="^menu$"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("Bot chal raha hai 🚀")
    # Flask thread start karo (Koyeb ke liye)
    threading.Thread(target=run_flask, daemon=True).start()
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
