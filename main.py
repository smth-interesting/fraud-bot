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
from aiohttp import web

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
PHONE_SALT = os.getenv("PHONE_SALT", "default").encode()
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", 8080))

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)
db_pool = None

def hash_phone(phone):
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
                (1, "2+2=? Скажи ответ", "два, четыре, 4", "кстати"),
                (1, "Назови три цвета", "красный, оранжевый, жёлтый, синий", "например"),
                (1, "Назови столицу России", "москва", "кстати"),
                (2, "Пиши ЗАГЛАВНЫМИ", "ВЕРХНИЙ", "послушайте"),
                (2, "Сделай вид что обиделся", "обида, ладно, понял", "хорошо"),
                (2, "Ответь кратко", "да, нет, ок", "тише"),
                (3, "Крякни 5 раз", "кря, утка", "ребёнок"),
                (3, "Спроси про ананасы", "ананас, пицца", "кстати"),
                (3, "Вставь строчку из песни", "чунга, кузнечик", "напеваю")
            ])
    logger.info("DB ready")

DISCLAIMER = "⚠️ ВНИМАНИЕ: Учебный симулятор.\n🎯 Цель: тренировка навыков.\n🚫 НИКОГДА не сообщай реальные данные."

class GameStates(StatesGroup):
    waiting_contact = State()
    in_game = State()

async def webhook_handler(request):
    try:
        update = types.Update(**await request.json())
        await dp.feed_update(bot, update)
        return web.Response()
    except Exception as e:
        logger.error(f"Error: {e}")
        return web.Response(status=500)

@router.message(CommandStart())
async def cmd_start(message: types.Message):
    await message.answer(DISCLAIMER, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принимаю", callback_data="start")]]))

@router.callback_query(F.data == "start")
async def start_menu(cb: types.CallbackQuery):
    await cb.message.answer("Выбери режим:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📱 С регистрацией", callback_data="reg")],
        [InlineKeyboardButton(text="👤 Гость", callback_data="guest")]
    ]))

@router.callback_query(F.data == "reg")
async def req_contact(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("📱 Подтверди номер:", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📤 Поделиться", request_contact=True)]], resize_keyboard=True))
    await state.set_state(GameStates.waiting_contact)

@router.message(GameStates.waiting_contact, F.contact)
async def save_contact(message: types.Message, state: FSMContext):
    if message.contact.user_id != message.from_user.id:
        return await message.answer("❌ Нельзя чужой номер!")
    phone_hash = hash_phone(message.contact.phone_number)
    nick = html.escape(message.from_user.username or "user")
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO users (tg_id, nickname, phone_hash, verified, created_at) VALUES ($1, $2, $3, 1, $4) ON CONFLICT (tg_id) DO UPDATE SET nickname=$2, verified=1", message.from_user.id, nick, phone_hash, datetime.now().isoformat())
    await message.answer("✅ OK", reply_markup=types.ReplyKeyboardRemove())
    await start_game(message, state, True)

@router.callback_query(F.data == "guest")
async def guest_start(cb: types.CallbackQuery, state: FSMContext):
    await start_game(cb.message, state, False)

async def start_game(message, state: FSMContext, verified: bool):
    async with db_pool.acquire() as conn:
        tasks = await conn.fetch("SELECT * FROM tasks ORDER BY RANDOM() LIMIT 9")
    await state.update_data(tasks=tasks, idx=0, done=0, start=time.time(), verified=verified)
    await state.set_state(GameStates.in_game)
    await message.answer("📞 <b>ВЫЗОВ</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять", callback_data="call")]]))

@router.callback_query(F.data == "call", GameStates.in_game)
async def answer_call(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data or "tasks" not in data:
        return await cb.message.answer("❌ Ошибка. /start")
    task = data["tasks"][0]
    await cb.message.answer(f"🕵️ <b>Мошенник:</b> Здравствуйте...\n\n🎯 <b>ЗАДАНИЕ:</b> {task['text']}\n💡 Вплети в диалог!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📞 Завершить", callback_data="end")]]))
    asyncio.create_task(scammer(cb.message.from_user.id, state))

async def scammer(uid, state):
    phrases = ["Не отвлекайтесь", "Почему молчите?", "Продиктуйте код", "Вы слушаете?"]
    try:
        while True:
            await asyncio.sleep(30)
            data = await state.get_data()
            if "start" not in data:
                break
            await bot.send_message(uid, f"🕵️ <b>Мошенник:</b> {random.choice(phrases)}")
    except:
        pass

@router.message(GameStates.in_game)
async def handle_msg(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data or "idx" not in data:
        return
    tasks = data["tasks"]
    idx = data["idx"]
    if idx >= len(tasks):
        return
    task = tasks[idx]
    keywords = [w.strip() for w in task["keywords"].split(",")]
    masks = [w.strip() for w in task["mask_words"].split(",")]
    msg = message.text.lower() if message.text else ""
    found_kw = any(k in msg for k in keywords)
    found_mask = any(m in msg for m in masks)
    if found_kw and not found_mask:
        await message.answer("🕵️ <b>Мошенник:</b> Подозрительно... *сброс*")
        await finish_game(message, state, True)
        return
    if found_kw:
        await state.update_data(done=data["done"]+1, idx=idx+1)
        if idx+1 < len(tasks):
            await message.answer(f"✅ OK\n\n🎯 <b>ЗАДАНИЕ {idx+2}:</b> {tasks[idx+1]['text']}")
        else:
            await message.answer("✅ Все задания! Нажми 📞 Завершить")
    else:
        await message.answer("🕵️ <b>Мошенник:</b> Вернёмся к карте")

@router.callback_query(F.data == "end", GameStates.in_game)
async def finish_cb(cb: types.CallbackQuery, state: FSMContext):
    await finish_game(cb.message, state, False)

async def finish_game(message, state: FSMContext, forced: bool):
    data = await state.get_data()
    if not data or "start" not in data:
        return await message.answer("❌ Ошибка")
    duration = time.time() - data["start"]
    done = data.get("done", 0)
    score = duration * 8 * (1 + done * 0.2) if duration >= 180 and done > 0 else 0
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO sessions (tg_id, start_time, end_time, duration, tasks_done, score, status) VALUES ($1, $2, $3, $4, $5, $6, $7)", message.from_user.id, data["start"], time.time(), duration, done, score, "ended")
    text = f"📞 <b>ГОТОВО</b>\n⏱ {int(duration)} сек\n✅ {done}/9\n💰 {int(score)} ₽\n"
    text += "🏆 В рейтинг!\n" if score > 0 else "⏱ <3 мин\n"
    await message.answer(text)
    await state.clear()

async def on_startup(app):
    await init_db()
    url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME', 'localhost')}/bot{TOKEN}"
    await bot.set_webhook(url)
    logger.info(f"Webhook: {url}")

async def on_shutdown(app):
    if db_pool:
        await db_pool.close()
    await bot.delete_webhook()
    await bot.session.close()

if __name__ == "__main__":
    if not TOKEN or "СЮДА" in TOKEN:
        print("❌ TOKEN!")
        exit()
    if not DATABASE_URL:
        print("❌ DATABASE_URL!")
        exit()
    app = web.Application()
    app.router.add_post(f"/bot{TOKEN}", webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host="0.0.0.0", port=PORT)