import asyncio
import hashlib
import hmac
import html
import logging
import os
import random
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
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
PHONE_SALT = os.getenv("PHONE_SALT", "default_salt_change_in_env").encode()
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", 8080))

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

db_pool = None
active_games = {}

class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, rate_limit: float = 1.0):
        self.rate_limit = rate_limit
        self.last_message = {}

    async def __call__(self, handler, event, data):
        user_id = event.from_user.id if hasattr(event, 'from_user') else None
        if user_id:
            now = time.time()
            if user_id in self.last_message and now - self.last_message[user_id] < self.rate_limit:
                logger.info(f"User {user_id} throttled.")
                return
            self.last_message[user_id] = now
        return await handler(event, data)

dp.update.middleware(ThrottlingMiddleware(rate_limit=1.0))

def hash_phone(phone: str) -> str:
    return hmac.new(PHONE_SALT, phone.encode(), hashlib.sha256).hexdigest()

async def init_db(pool):
    async with pool.acquire() as conn:
        await conn.execute('''CREATE TABLE IF NOT EXISTS users
                     (tg_id BIGINT PRIMARY KEY, nickname TEXT, phone_hash TEXT, 
                      consent_longterm INT DEFAULT 0, verified INT DEFAULT 0, 
                      created_at TEXT)''')
        await conn.execute('''CREATE TABLE IF NOT EXISTS tasks
                     (id SERIAL PRIMARY KEY, round_num INT, text TEXT, keywords TEXT, mask_words TEXT)''')
        await conn.execute('''CREATE TABLE IF NOT EXISTS sessions
                     (id SERIAL PRIMARY KEY, tg_id BIGINT, start_time DOUBLE PRECISION, end_time DOUBLE PRECISION, 
                      duration DOUBLE PRECISION, tasks_done INT, score DOUBLE PRECISION, status TEXT)''')
        await conn.execute('''CREATE TABLE IF NOT EXISTS analytics
                     (id SERIAL PRIMARY KEY, event TEXT, payload TEXT, created_at TEXT)''')
        
        cur = await conn.fetchrow("SELECT COUNT(*) as cnt FROM tasks")
        count = cur["cnt"]
        if count == 0:
            tasks = [
                (1, 1, "2+2=? Скажи ответ в разговоре с мошенником", "два, четыре, 4", "кстати, между прочим, а скажи"),
                (2, 1, "Назови три любых цвета радуги", "красный, оранжевый, жёлтый, синий, зелёный", "например, вообще, скажи"),
                (3, 1, "Назови столицу России", "москва", "кстати, а где, столица это"),
                (4, 2, "Напиши фразу только ЗАГЛАВНЫМИ БУКВАМИ, имитируя крик", "ВЕРХНИЙ РЕГИСТР", "послушайте, зачем вы, я просто"),
                (5, 2, "Сделай вид, что обиделся на мошенника", "обида, ладно, понял, всё ясно", "ну хорошо, извините, не хочу"),
                (6, 2, "Ответь максимально коротко (1–3 слова), будто боишься, что рядом кто-то слышит", "да, нет, ок, понял", "тише, шёпотом, аккуратно"),
                (7, 3, "Крякни 5 раз", "кря, утка, кряк", "ребёнок, фон, извините"),
                (8, 3, "Спроси у мошенника: «А вы любите ананасы на пицце?»", "ананас, пицца", "кстати, вопрос, а вы"),
                (9, 3, "Вставь в сообщение строчку из любой детской песенки", "чунга, кузнечик, траве, сидел", "напеваю, детство, песня")
            ]
            await conn.executemany("INSERT INTO tasks VALUES (DEFAULT, $1, $2, $3, $4)", tasks)
    logger.info("Database initialized.")

DISCLAIMER = "⚠️ <b>ВНИМАНИЕ:</b> Это учебный симулятор.\n🎯 <b>Цель:</b> тренировка навыков.\n🚫 <b>НИКОГДА</b> не сообщай реальные данные."
RULES_TEXT = "📖 <b>ПРАВИЛА:</b>\n✅ Вплетаи задания органично\n❌ Не пиши в лоб\n⏱ Минимум 3 минуты"
PRIVACY_TEXT = "🔒 <b>Политика:</b>\n1. Собираем ID и хэш телефона\n2. Не передаём третьим лицам\n3. /delete_data для удаления"
TERMS_TEXT = "📜 <b>Правила:</b>\n1. 16+\n2. Запрещён спам\n3. Бот «как есть»"

class GameStates(StatesGroup):
    waiting_contact = State()
    in_game = State()
    waiting_feedback = State()

async def webhook_handler(request: web.Request):
    try:
        update_data = await request.json()
        update = types.Update(**update_data)
        await dp.feed_update(bot, update)
        return web.Response()
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.Response(status=500)

@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await message.answer(DISCLAIMER, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принимаю", callback_data="accept_rules")]
    ]))

@router.callback_query(F.data == "accept_rules")
async def accept_rules(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("🎯 <b>Как играть:</b>\nВыбери режим:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 С регистрацией", callback_data="reg_contact")],
        [InlineKeyboardButton(text="👤 Гостевой режим", callback_data="guest_mode")]
    ]))

@router.callback_query(F.data == "reg_contact")
async def req_contact(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("📱 Подтвердите номер:", reply_markup=ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📤 Поделиться контактом", request_contact=True)]
    ], resize_keyboard=True))
    await state.set_state(GameStates.waiting_contact)

@router.message(GameStates.waiting_contact, F.contact)
async def save_contact(message: types.Message, state: FSMContext):
    if message.contact.user_id != message.from_user.id:
        await message.answer("❌ Нельзя чужой номер!")
        return
    phone_hash = hash_phone(str(message.contact.phone_number))
    nickname = html.escape(message.from_user.username or "user")
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (tg_id, nickname, phone_hash, verified, created_at) VALUES ($1, $2, $3, 1, $4) ON CONFLICT (tg_id) DO UPDATE SET nickname=$2, phone_hash=$3, verified=1",
                message.from_user.id, nickname, phone_hash, datetime.now().isoformat()
            )
    except Exception as e:
        logger.error(f"DB Error: {e}")
        await message.answer("❌ Ошибка сохранения.")
        return
    await message.answer("✅ Подтверждено!", reply_markup=types.ReplyKeyboardRemove())
    await start_game(message, state, True)

@router.callback_query(F.data == "guest_mode")
async def guest_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("👤 Гостевой режим")
    await start_game(cb.message, state, False)

async def start_game(message, state: FSMContext, verified: bool):
    try:
        async with db_pool.acquire() as conn:
            tasks = await conn.fetch("SELECT * FROM tasks ORDER BY RANDOM() LIMIT 9")
        await state.update_data({
            "verified": verified,
            "tasks": tasks,
            "current_task_idx": 0,
            "tasks_done": 0,
            "start_time": time.time()
        })
        await state.set_state(GameStates.in_game)
        if message.from_user.id in active_games:
            old_task = active_games[message.from_user.id].get("task")
            if old_task and not old_task.done():
                old_task.cancel()
                try:
                    await old_task
                except asyncio.CancelledError:
                    pass
        active_games[message.from_user.id] = {"task": None}
        await message.answer("📞 <b>ВХОДЯЩИЙ ВЫЗОВ...</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Принять", callback_data="answer_call")]
        ]))
    except Exception as e:
        logger.error(f"Error: {e}")
        await message.answer("❌ Ошибка. /start")

@router.callback_query(F.data == "answer_call", GameStates.in_game)
async def answer_call(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data or "tasks" not in 
        await cb.message.answer("❌ Ошибка. /start")
        return
    task_text = data["tasks"][0]["text"]
    await cb.message.answer(
        f"🕵️ <b>Мошенник:</b> Здравствуйте...\n\n🎯 <b>ЗАДАНИЕ 1:</b> {task_text}\n💡 Вплети органично!\n⏱ Таймер запущен.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📞 Завершить", callback_data="end_call")]
        ])
    )
    task = asyncio.create_task(scammer_background(cb.message.from_user.id, state))
    active_games[cb.message.from_user.id]["task"] = task

async def scammer_background(user_id: int, state: FSMContext):
    phrases = ["Не отвлекайтесь...", "Почему молчите?", "Продиктуйте код...", "Вы слушаете?", "Не перезванивайте..."]
    try:
        while True:
            await asyncio.sleep(random.randint(25, 40))
            data = await state.get_data()
            if "start_time" not in data:
                break
            await bot.send_message(user_id, f"🕵️ <b>Мошенник:</b> {random.choice(phrases)}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error: {e}")

@router.message(GameStates.in_game)
async def handle_game_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data or "current_task_idx" not in data:
        return
    tasks = data["tasks"]
    idx = data["current_task_idx"]
    if idx >= len(tasks):
        return
    task_row = tasks[idx]
    task_keywords = [w.strip() for w in task_row["keywords"].split(",")]
    mask_words = [w.strip() for w in task_row["mask_words"].split(",")]
    msg_lower = (message.text or "").lower()
    found_keywords = any(kw in msg_lower for kw in task_keywords)
    found_masks = any(mw in msg_lower for mw in mask_words)
    reaction = "neutral" if (found_keywords and found_masks) else ("suspicion" if found_keywords else "ignore")
    await log_analytics("task_attempt", f"user={message.from_user.id}, task={idx}, reaction={reaction}")
    if reaction == "suspicion":
        await message.answer("🕵️ <b>Мошенник:</b> Всё понятно. *сброс*")
        await finish_game(message, state, True)
        return
    if found_keywords:
        await state.update_data({"tasks_done": data["tasks_done"] + 1, "current_task_idx": idx + 1})
        next_idx = idx + 1
        if next_idx < len(tasks):
            round_name = ["Логика", "Актёрство", "Импровизация"][next_idx // 3]
            await message.answer(f"✅ Засчитано!\n\n🎯 <b>ЗАДАНИЕ {next_idx+1} ({round_name}):</b> {tasks[next_idx]['text']}\n💡 Вплети органично.")
        else:
            await message.answer("✅ Все задания! Нажмите 📞 Завершить.")
    else:
        await message.answer("🕵️ <b>Мошенник:</b> Вернёмся к карте.")

@router.callback_query(F.data == "end_call", GameStates.in_game)
async def finish_game_cb(cb: types.CallbackQuery, state: FSMContext):
    await finish_game(cb.message, state, False)

async def finish_game(message, state: FSMContext, forced_end=False):
    data = await state.get_data()
    if not data or "start_time" not in 
        await message.answer("❌ Ошибка.")
        return
    duration = time.time() - data["start_time"]
    tasks_done = data.get("tasks_done", 0)
    if message.from_user.id in active_games:
        ag = active_games[message.from_user.id]
        if ag["task"] and not ag["task"].done():
            ag["task"].cancel()
            try:
                await ag["task"]
            except asyncio.CancelledError:
                pass
        del active_games[message.from_user.id]
    score = 0.0
    in_leaderboard = duration >= 180 and tasks_done > 0
    if in_leaderboard:
        score = duration * 8 * (1 + tasks_done * 0.2)
    await log_analytics("session_end", f"duration={int(duration)}, tasks={tasks_done}")
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sessions (tg_id, start_time, end_time, duration, tasks_done, score, status) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                message.from_user.id, data["start_time"], time.time(), duration, tasks_done, score, "ended"
            )
    except Exception as e:
        logger.error(f"DB Error: {e}")
    text = f"📞 <b>ВЫЗОВ ЗАВЕРШЁН</b>\n⏱ {int(duration)} сек\n✅ {tasks_done}/9\n💰 {int(score)} ₽\n"
    text += "🏆 <b>В рейтинг!</b>\n" if in_leaderboard else "⏱ <3 мин\n"
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Отзыв", callback_data="feedback_start")]
    ]))
    await state.clear()

@router.callback_query(F.data == "feedback_start")
async def req_feedback(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("Оценка 1-5:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(i), callback_data=f"fb_{i}") for i in range(1,6)]
    ]))
    await state.set_state(GameStates.waiting_feedback)

@router.callback_query(F.data.startswith("fb_"), GameStates.waiting_feedback)
async def save_feedback(cb: types.CallbackQuery, state: FSMContext):
    await state.update_data({"rating": cb.data.split("_")[1]})
    await cb.message.answer("Комментарий (/skip):")

@router.message(Command("skip"), GameStates.waiting_feedback)
@router.message(GameStates.waiting_feedback)
async def process_feedback(message: types.Message, state: FSMContext):
    data = await state.get_data()
    text = message.text if message.text != "/skip" else "Без комментария"
    safe_text = html.escape(text)
    safe_nick = html.escape(message.from_user.username or "Anon")
    await log_analytics("feedback", f"user={message.from_user.id}, rating={data.get('rating','?')}")
    if ADMIN_ID:
        try: 
            await bot.send_message(ADMIN_ID, f"📝 Отзыв от {safe_nick}:\n⭐ {data.get('rating','?')}\n💬 {safe_text}")
        except Exception as e:
            logger.error(f"Error: {e}")
    await message.answer("Спасибо!")
    await state.clear()

@router.message(Command("rules"))
async def cmd_rules(m: types.Message): await m.answer(RULES_TEXT)
@router.message(Command("privacy"))
async def cmd_priv(m: types.Message): await m.answer(PRIVACY_TEXT)
@router.message(Command("terms"))
async def cmd_terms(m: types.Message): await m.answer(TERMS_TEXT)

@router.message(Command("delete_data"))
async def cmd_delete(m: types.Message):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM users WHERE tg_id=$1", m.from_user.id)
            await conn.execute("DELETE FROM sessions WHERE tg_id=$1", m.from_user.id)
        await m.answer("🗑️ Удалено.")
    except Exception as e:
        logger.error(f"Error: {e}")
        await m.answer("❌ Ошибка.")

@router.message(Command("leaderboard"))
async def cmd_leaderboard(m: types.Message):
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT u.nickname, u.verified, MAX(s.score) as best FROM users u JOIN sessions s ON u.tg_id = s.tg_id WHERE s.duration >= 180 GROUP BY u.tg_id ORDER BY best DESC LIMIT 10")
        if not rows: return await m.answer("🏆 Пусто.")
        text = "🏆 <b>ТОП-10</b>\n"
        for i, row in enumerate(rows, 1):
            safe_nick = html.escape(row["nickname"] or "Anon")
            text += f"{i}. {safe_nick} {'✅' if row['verified'] else ''} — {int(row['best'])} ₽\n"
        await m.answer(text)
    except Exception as e:
        logger.error(f"Error: {e}")
        await m.answer("❌ Ошибка.")

async def log_analytics(event, payload):
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("INSERT INTO analytics (event, payload, created_at) VALUES ($1, $2, $3)", event, payload, datetime.now().isoformat())
    except Exception as e:
        logger.error(f"Error: {e}")

async def on_startup(app):
    global db_pool
    db_pool = await asyncpg.create_pool(dsn=DATABASE_URL)
    await init_db(db_pool)
    webhook_url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME', 'localhost')}/bot{TOKEN}"
    await bot.set_webhook(webhook_url)
    logger.info(f"Webhook set: {webhook_url}")

async def on_shutdown(app):
    if db_pool:
        await db_pool.close()
    await bot.delete_webhook()
    await bot.session.close()
    logger.info("Closed.")

if __name__ == "__main__":
    if not TOKEN or "СЮДА" in TOKEN:
        print("❌ Ошибка: токен!")
        exit()
    if not DATABASE_URL:
        print("❌ Ошибка: DATABASE_URL!")
        exit()
    app = web.Application()
    app.router.add_post(f"/bot{TOKEN}", webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    logger.info("Starting...")
    web.run_app(app, host="0.0.0.0", port=PORT)