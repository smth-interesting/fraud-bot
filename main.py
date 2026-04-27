import asyncio
import hashlib
import hmac
import html
import logging
import os
import random
import re
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
import asyncpg
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
try:
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
except ValueError:
    ADMIN_ID = 0
    logger.warning("ADMIN_ID не является числом, отправка отзывов админу отключена.")
_salt_raw = os.getenv("PHONE_SALT", "").strip()
if _salt_raw == "default_salt":
    _salt_raw = ""
PHONE_SALT = _salt_raw.encode() if _salt_raw else None
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", 8080))

_wp = os.getenv("WEBHOOK_PATH", "telegram-webhook").strip().strip("/")
WEBHOOK_PATH = f"/{_wp}" if _wp else "/telegram-webhook"
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip().strip("\"'")
WEBHOOK_SECRET_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)
db_pool = None
active_tasks = {}

class ThrottlingMiddleware:
    def __init__(self, rate_limit=1.0):
        self.rate_limit = rate_limit
        self.last_message = {}
    async def __call__(self, handler, event, data):
        # Throttle only text/media messages. Callback buttons must not be dropped.
        if isinstance(event, types.Message):
            uid = event.from_user.id if event.from_user else None
            if uid:
                now = time.time()
                if uid in self.last_message and now - self.last_message[uid] < self.rate_limit:
                    return
                self.last_message[uid] = now
        return await handler(event, data)

dp.update.middleware(ThrottlingMiddleware())

def hash_phone(phone):
    if not PHONE_SALT:
        raise RuntimeError("PHONE_SALT не задан в окружении")
    return hmac.new(PHONE_SALT, phone.encode(), hashlib.sha256).hexdigest()

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with db_pool.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (tg_id BIGINT PRIMARY KEY, nickname TEXT, phone_hash TEXT, verified INT DEFAULT 0, created_at TEXT)")
        await conn.execute("CREATE TABLE IF NOT EXISTS tasks (id SERIAL PRIMARY KEY, round_num INT, text TEXT, keywords TEXT, mask_words TEXT)")
        await conn.execute("CREATE TABLE IF NOT EXISTS sessions (id SERIAL PRIMARY KEY, tg_id BIGINT, start_time FLOAT, end_time FLOAT, duration FLOAT, tasks_done INT, score FLOAT, status TEXT)")
        await conn.execute("CREATE TABLE IF NOT EXISTS analytics (id SERIAL PRIMARY KEY, event TEXT, payload TEXT, created_at TEXT)")
        row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM tasks")
        if row["cnt"] == 0:
            await conn.executemany("INSERT INTO tasks (round_num, text, keywords, mask_words) VALUES ($1, $2, $3, $4)", [
                (1, "2+2=? Скажи ответ в разговоре", "два, четыре, 4", "кстати, между прочим"),
                (1, "Назови три любых цвета радуги", "красный, оранжевый, жёлтый, синий", "например, вообще"),
                (1, "Назови столицу России", "москва", "кстати, а где"),
                (2, "Напиши фразу только ЗАГЛАВНЫМИ БУКВАМИ", "ВЕРХНИЙ РЕГИСТР", "послушайте, зачем"),
                (2, "Сделай вид, что обиделся на мошенника", "обида, ладно, понял", "ну хорошо, извините"),
                (2, "Ответь максимально коротко (1–3 слова)", "да, нет, ок", "тише, шёпотом"),
                (3, "Крякни 5 раз", "кря, утка, кряк", "ребёнок, фон"),
                (3, "Спроси: «А вы любите ананасы на пицце?»", "ананас, пицца", "кстати, вопрос"),
                (3, "Вставь строчку из любой детской песенки", "чунга, кузнечик", "напеваю, детство")
            ])
    logger.info("✅ DB ready")

DISCLAIMER = (
    "⚠️ <b>ВНИМАНИЕ:</b> Это учебный симулятор. Все диалоги, номера и сценарии вымышлены.\n\n"
    "🎯 <b>Цель проекта:</b> тренировка навыков распознавания мошеннических схем в безопасной среде.\n\n"
    "🚫 <b>НИКОГДА</b> не сообщай реальные данные (номера карт, пароли, коды из СМС) в этом боте или в подозрительных звонках.\n\n"
    "⛔ Не используй полученные знания для обхода реальных систем безопасности или причинения вреда третьим лицам.\n\n"
    "Продолжая игру, ты принимаешь эти условия и Политику конфиденциальности."
)

RULES = "📖 <b>КАК ВЫПОЛНЯТЬ ЗАДАНИЯ:</b>\nТвоя цель — вплетать задания в диалог <b>органично</b>.\n✅ Хорошо: «Кстати, а какой цвет у радуги первый? Красный...»\n❌ Плохо: «Красный оранжевый (выполняю задание)»\n💡 Мошенник реагирует на ключевые слова. ⏱ Минимум 3 мин для рейтинга."
PRIVACY = "🔒 <b>Политика:</b>\n1. Собираем Telegram ID, ник и хэш телефона.\n2. Номер не передаётся третьим лицам.\n3. Удаляйте данные командой /delete_data."
TERMS = "📜 <b>Правила:</b>\n1. 16+\n2. Запрещён спам и мошенничество.\n3. Бот «как есть».\n4. Играя, вы соглашаетесь с правилами."
HELP = "📚 <b>Команды:</b>\n/start - Меню\n/rules - Правила игры\n/privacy - Конфиденциальность\n/terms - Условия\n/leaderboard - Рейтинг\n/delete_data - Удалить данные\n/help - Это сообщение"
CONTACT_HINT = "📱 Нажмите «📤 Поделиться контактом», чтобы подтвердить номер и попасть в рейтинг."

class GameStates(StatesGroup):
    in_game = State()
    waiting_feedback = State()

MAIN_KB = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="🎮 Новая игра"), KeyboardButton(text="🏆 Рейтинг")],
    [KeyboardButton(text="📖 Правила"), KeyboardButton(text="📜 Документы")],
    [KeyboardButton(text="🗑 Удалить данные")]
], resize_keyboard=True)

@router.message(CommandStart())
@router.message(F.text == "🎮 Новая игра")
async def cmd_start(msg: types.Message, state: FSMContext):
    await state.clear()
    await msg.answer(DISCLAIMER, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принимаю условия", callback_data="accept")]]))

@router.callback_query(F.data == "accept")
async def accept(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer(" <b>Как играть:</b>\nТы ведёшь диалог с «мошенником» и получаешь задания. Вплетаи их в разговор органично.\n\n👇 Выбери режим:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 С регистрацией", callback_data="reg")],
        [InlineKeyboardButton(text="👤 Гостевой режим", callback_data="guest")]
    ]))
    await cb.answer()

@router.callback_query(F.data == "reg")
async def req_contact(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("📱 Для рейтинга подтвердите номер:", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📤 Поделиться контактом", request_contact=True)]], resize_keyboard=True, one_time_keyboard=True))
    await state.set_state(GameStates.in_game)
    await cb.answer()

@router.message(GameStates.in_game, F.contact)
async def save_contact(msg: types.Message, state: FSMContext):
    if msg.contact.user_id != msg.from_user.id:
        return await msg.answer("❌ Нельзя чужой номер!", reply_markup=MAIN_KB)
    ph = hash_phone(str(msg.contact.phone_number))
    nick = html.escape(msg.from_user.username or "user")
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO users (tg_id, nickname, phone_hash, verified, created_at) VALUES ($1, $2, $3, 1, $4) ON CONFLICT (tg_id) DO UPDATE SET nickname=$2, verified=1", msg.from_user.id, nick, ph, datetime.now().isoformat())
    except Exception as e:
        logger.error(f"DB: {e}")
        return await msg.answer("❌ Ошибка.", reply_markup=MAIN_KB)
    await msg.answer("✅ Подтверждено!", reply_markup=MAIN_KB)
    await start_game(msg, state, True)

@router.callback_query(F.data == "guest")
async def guest_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("👤 Гостевой режим. Прогресс сохранится, но в рейтинг не попадёт.", reply_markup=MAIN_KB)
    await start_game(cb.message, state, False)
    await cb.answer()

async def start_game(msg, state: FSMContext, verified: bool):
    try:
        async with db_pool.acquire() as conn:
            tasks = await conn.fetch("SELECT * FROM tasks ORDER BY RANDOM() LIMIT 9")
        await state.update_data(verified=verified, tasks=tasks, idx=0, done=0, start=time.time())
        await state.set_state(GameStates.in_game)
        if msg.from_user.id in active_tasks:
            t = active_tasks[msg.from_user.id]
            if t and not t.done(): t.cancel()
            try: await t
            except asyncio.CancelledError: pass
        await msg.answer("📞 <b>ВХОДЯЩИЙ ВЫЗОВ...</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять вызов", callback_data="call")]]))
    except Exception as e:
        logger.error(f"Start: {e}")
        await msg.answer("❌ Ошибка. /start", reply_markup=MAIN_KB)

@router.callback_query(F.data == "call", GameStates.in_game)
async def answer_call(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data or "tasks" not in data:
        return await cb.message.answer("❌ Ошибка. /start")
    t = data["tasks"][0]
    rn = ["Логика", "Актёрство", "Импровизация"][t["round_num"]-1]
    await cb.message.answer(f"🕵️ <b>Мошенник:</b> Здравствуйте, это служба безопасности...\n\n🎯 <b>ЗАДАНИЕ 1 (Раунд: {rn})</b>\n📝 {t['text']}\n💡 Вплети в диалог органично!\n⏱ Таймер запущен.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📞 Завершить", callback_data="end")]]))
    active_tasks[cb.from_user.id] = asyncio.create_task(scammer_bg(cb.from_user.id, state))
    await cb.answer()

async def scammer_bg(uid, state: FSMContext):
    phrases = ["Не отвлекайтесь, нужно подтвердить данные.", "Почему молчите? Карта блокируется!", "Продиктуйте код из СМС, срочно.", "Вы слушаете? Операция отменяется.", "Не перезванивайте, я на линии."]
    try:
        while True:
            await asyncio.sleep(random.randint(25, 40))
            data = await state.get_data()
            if not data or "start" not in data: break
            await bot.send_message(uid, f"🕵️ <b>Мошенник:</b> {random.choice(phrases)}")
    except asyncio.CancelledError: pass
    except Exception as e: logger.error(f"BG: {e}")

@router.message(GameStates.in_game)
async def handle_msg(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data or "tasks" not in data:
        return await msg.answer(CONTACT_HINT)
    if "idx" not in data:
        return
    tasks = data["tasks"]
    idx = data["idx"]
    if idx >= len(tasks): return
    t = tasks[idx]
    kws = [w.strip() for w in t["keywords"].split(",")]
    masks = [w.strip() for w in t["mask_words"].split(",")]
    txt = (msg.text or "").lower()
    kw_ok = any(k in txt for k in kws)
    m_ok = any(m in txt for m in masks)
    if kw_ok and not m_ok:
        await msg.answer("🕵️ <b>Мошенник:</b> Подозрительно... *сброс*")
        await finish_game(msg, state, True)
        return
    if kw_ok:
        await state.update_data(done=data["done"]+1, idx=idx+1)
        if idx+1 < len(tasks):
            rn = ["Логика", "Актёрство", "Импровизация"][tasks[idx+1]["round_num"]-1]
            await msg.answer(f"✅ Засчитано!\n\n🎯 <b>ЗАДАНИЕ {idx+2} ({rn})</b>\n📝 {tasks[idx+1]['text']}\n💡 Вплети органично.")
        else:
            await msg.answer("✅ Все задания! Нажми 📞 Завершить.")
    else:
        await msg.answer("🕵️ <b>Мошенник:</b> Вернёмся к безопасности карты.")

@router.callback_query(F.data == "end", GameStates.in_game)
async def finish_cb(cb: types.CallbackQuery, state: FSMContext):
    await finish_game(cb.message, state, False)
    await cb.answer()

async def finish_game(msg, state: FSMContext, forced: bool):
    data = await state.get_data()
    if not data or "start" not in data:
        return await msg.answer("❌ Ошибка.", reply_markup=MAIN_KB)
    dur = time.time() - data["start"]
    done = data.get("done", 0)
    verified = data.get("verified", False)
    if msg.from_user.id in active_tasks:
        t = active_tasks[msg.from_user.id]
        if t and not t.done(): t.cancel()
        try: await t
        except asyncio.CancelledError: pass
        del active_tasks[msg.from_user.id]
    score = dur * 8 * (1 + done * 0.2) if dur >= 180 and done > 0 else 0
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO sessions (tg_id, start_time, end_time, duration, tasks_done, score, status) VALUES ($1, $2, $3, $4, $5, $6, $7)", msg.from_user.id, data["start"], time.time(), dur, done, score, "ended")
    except Exception as e: logger.error(f"Session: {e}")
    txt = f"📞 <b>ВЫЗОВ ЗАВЕРШЁН</b>\n⏱ {int(dur)} сек\n✅ {done}/9\n💰 {int(score)} ₽\n"
    if score > 0 and verified:
        txt += "🏆 <b>В рейтинг!</b>\n"
    elif score > 0 and not verified:
        txt += "💡 Гостевой режим — в таблицу лидеров очки не идут.\n"
    elif done > 0:
        txt += "⏱ Меньше 3 мин — очки за сессию не начислены.\n"
    else:
        txt += "⏱ Задания не засчитаны или сессия короткая.\n"
    await msg.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📝 Отзыв", callback_data="fb")], [InlineKeyboardButton(text="🔙 Меню", callback_data="menu")]]))
    await state.clear()

@router.callback_query(F.data == "fb")
async def req_fb(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("Оценка 1-5:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=str(i), callback_data=f"r_{i}") for i in range(1,6)]]))
    await state.set_state(GameStates.waiting_feedback)
    await cb.answer()

@router.callback_query(F.data.startswith("r_"), GameStates.waiting_feedback)
async def save_rating(cb: types.CallbackQuery, state: FSMContext):
    await state.update_data(rating=cb.data.split("_")[1])
    await cb.message.answer("Комментарий (/skip):")
    await cb.answer()

@router.message(Command("skip"), GameStates.waiting_feedback)
@router.message(GameStates.waiting_feedback)
async def process_fb(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    raw_text = msg.text or ""
    txt = raw_text if raw_text != "/skip" else "Без комментария"
    if not txt:
        txt = "Без комментария (не текстовый ответ)"
    safe_t = html.escape(txt[:3500])
    safe_n = html.escape(msg.from_user.username or "Anon")
    if ADMIN_ID:
        try:
            await bot.send_message(ADMIN_ID, f"📝 {safe_n}: ⭐{data.get('rating','?')}\n💬{safe_t}")
        except Exception as e:
            logger.warning("Не удалось отправить отзыв админу: %s", e)
    await msg.answer("Спасибо!", reply_markup=MAIN_KB)
    await state.clear()

@router.callback_query(F.data == "menu")
async def back_menu(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("🔙 Главное меню", reply_markup=MAIN_KB)
    await cb.answer()

@router.message(F.text == "📖 Правила")
@router.message(Command("rules"))
async def cmd_rules(m: types.Message): await m.answer(RULES, reply_markup=MAIN_KB)

@router.message(F.text == "📜 Документы")
async def docs_menu(m: types.Message):
    await m.answer("Выбери:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔒 Приватность", callback_data="p")], [InlineKeyboardButton(text="📜 Условия", callback_data="t")]]))

@router.callback_query(F.data == "p")
async def show_p(cb: types.CallbackQuery):
    await cb.message.answer(PRIVACY, reply_markup=MAIN_KB)
    await cb.answer()

@router.callback_query(F.data == "t")
async def show_t(cb: types.CallbackQuery):
    await cb.message.answer(TERMS, reply_markup=MAIN_KB)
    await cb.answer()

@router.message(F.text == "🗑 Удалить данные")
@router.message(Command("delete_data"))
async def cmd_del(m: types.Message, state: FSMContext):
    await state.clear()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM users WHERE tg_id=$1", m.from_user.id)
            await conn.execute("DELETE FROM sessions WHERE tg_id=$1", m.from_user.id)
        await m.answer("🗑️ Удалено.", reply_markup=MAIN_KB)
    except Exception as e:
        logger.error(f"Del: {e}")
        await m.answer("❌ Ошибка.", reply_markup=MAIN_KB)

@router.message(F.text == "🏆 Рейтинг")
@router.message(Command("leaderboard"))
async def cmd_lb(m: types.Message):
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT u.nickname, u.verified, MAX(s.score) as best FROM users u JOIN sessions s ON u.tg_id = s.tg_id WHERE s.duration >= 180 GROUP BY u.tg_id ORDER BY best DESC LIMIT 10")
        if not rows: return await m.answer("🏆 Пусто.", reply_markup=MAIN_KB)
        txt = "🏆 <b>ТОП-10</b>\n"
        for i, r in enumerate(rows, 1):
            txt += f"{i}. {html.escape(r['nickname'] or 'Anon')} {'✅' if r['verified'] else ''} — {int(r['best'])} ₽\n"
        await m.answer(txt, reply_markup=MAIN_KB)
    except Exception as e:
        logger.error(f"LB: {e}")
        await m.answer("❌ Ошибка.", reply_markup=MAIN_KB)

@router.message(Command("help"))
async def cmd_help(m: types.Message): await m.answer(HELP, reply_markup=MAIN_KB)

async def webhook_handler(req: web.Request):
    try:
        if WEBHOOK_SECRET:
            if req.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
                return web.Response(status=403)
        await dp.feed_update(bot, types.Update(**await req.json()))
        return web.Response()
    except Exception as e:
        logger.exception("WH: %s", e)
        return web.Response(status=500)

async def on_startup(app):
    await init_db()
    me = await bot.get_me()
    logger.info("🤖 Bot connected: @%s (id=%s)", me.username, me.id)
    host = os.getenv("RENDER_EXTERNAL_HOSTNAME", "").strip()
    if not host:
        host = "localhost"
        logger.warning("RENDER_EXTERNAL_HOSTNAME не задан — в логе показан условный URL; для продакшена задайте хост Render.")
    url = f"https://{host}{WEBHOOK_PATH}"
    await bot.set_webhook(url, secret_token=WEBHOOK_SECRET or None)
    info = await bot.get_webhook_info()
    logger.info("🌐 Webhook set to: %s (путь без токена; секрет: %s)", url, "да" if WEBHOOK_SECRET else "нет — добавьте WEBHOOK_SECRET в .env")
    logger.info("📡 Telegram webhook info: url=%s pending_updates=%s last_error=%s", info.url, info.pending_update_count, info.last_error_message or "none")

async def on_shutdown(app):
    if db_pool: await db_pool.close()
    await bot.delete_webhook()
    await bot.session.close()
    logger.info("🛑 Stop")

if __name__ == "__main__":
    if not TOKEN or "СЮДА" in TOKEN:
        print("❌ TOKEN!")
        sys.exit(1)
    if not DATABASE_URL:
        print("❌ DATABASE_URL!")
        sys.exit(1)
    if not PHONE_SALT:
        print("❌ Задайте PHONE_SALT в .env (длинная случайная строка, не default_salt).")
        sys.exit(1)
    if WEBHOOK_SECRET and not WEBHOOK_SECRET_RE.fullmatch(WEBHOOK_SECRET):
        print("❌ WEBHOOK_SECRET содержит недопустимые символы.")
        print("   Разрешены только: A-Z, a-z, 0-9, '_' и '-'. Длина: 1..256.")
        sys.exit(1)
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    logger.info("🚀 Start")
    web.run_app(app, host="0.0.0.0", port=PORT)