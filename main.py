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

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher.middlewares.base import BaseMiddleware

# НАСТРОЙКА ЛОГИРОВАНИЯ (Вместо молчаливого поглощения ошибок)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ЗАГРУЗКА КОНФИГА
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
PHONE_SALT = os.getenv("PHONE_SALT", "default_salt_change_in_env").encode()

# НАСТРОЙКА БОТА
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

DB_PATH = "bot_data.db"
active_games = {}  # tg_id -> {"task": asyncio.Task}

# --- MIDDLEWARE ДЛЯ ЗАЩИТЫ ОТ СПАМА (RATE LIMITING) ---
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
                return  # Игнорируем сообщение, если слишком часто
            self.last_message[user_id] = now
        return await handler(event, data)

# Применяем middleware ко всем обновлениям
dp.update.middleware(ThrottlingMiddleware(rate_limit=1.0))

# 🔐 БЕЗОПАСНОЕ ХЭШИРОВАНИЕ ТЕЛЕФОНОВ
def hash_phone(phone: str) -> str:
    return hmac.new(PHONE_SALT, phone.encode(), hashlib.sha256).hexdigest()

# 🔐 БАЗА ДАННЫХ
async def init_db(db_conn):
    await db_conn.execute('''CREATE TABLE IF NOT EXISTS users
                 (tg_id INTEGER PRIMARY KEY, nickname TEXT, phone_hash TEXT, 
                  consent_longterm INTEGER DEFAULT 0, verified INTEGER DEFAULT 0, 
                  created_at TEXT)''')
    await db_conn.execute('''CREATE TABLE IF NOT EXISTS tasks
                 (id INTEGER PRIMARY KEY, round_num INTEGER, text TEXT, keywords TEXT, mask_words TEXT)''')
    await db_conn.execute('''CREATE TABLE IF NOT EXISTS sessions
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, tg_id INTEGER, start_time REAL, end_time REAL, 
                  duration REAL, tasks_done INTEGER, score REAL, status TEXT)''')
    await db_conn.execute('''CREATE TABLE IF NOT EXISTS analytics
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT, payload TEXT, created_at TEXT)''')
    
    cur = await db_conn.execute("SELECT COUNT(*) FROM tasks")
    count = (await cur.fetchone())[0]
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
        await db_conn.executemany("INSERT INTO tasks VALUES (?,?,?,?,?)", tasks)
    await db_conn.commit()

# Глобальное соединение с БД (оптимизация Этапа 2)
db_pool = None

# 📜 ТЕКСТЫ
DISCLAIMER = (
    "⚠️ <b>ВНИМАНИЕ:</b> Это учебный симулятор. Все диалоги, номера и сценарии вымышлены.\n"
    "🎯 <b>Цель проекта:</b> тренировка навыков распознавания мошеннических схем в безопасной среде.\n"
    "🚫 <b>НИКОГДА</b> не сообщай реальные данные (номера карт, пароли, коды из СМС) в этом боте или в подозрительных звонках.\n"
    "⛔ Не используй полученные знания для обхода реальных систем безопасности или причинения вреда третьим лицам.\n\n"
    "Продолжая игру, ты принимаешь эти условия и Политику конфиденциальности."
)
RULES_TEXT = (
    "📖 <b>КАК ВЫПОЛНЯТЬ ЗАДАНИЯ:</b>\n"
    "Твоя цель — вплетать задания в диалог <b>органично</b>.\n\n"
    "✅ <b>Хорошо:</b> «Кстати, а какой цвет у радуги первый? Красный, оранжевый...»\n"
    "❌ <b>Плохо:</b> «Красный оранжевый жёлтый (выполняю задание)»\n\n"
    "💡 Мошенник реагирует на ключевые слова. Если напишешь «в лоб» — он заподозрит неладное.\n"
    "⏱ Минимум 3 минуты разговора нужно, чтобы очки попали в рейтинг."
)
PRIVACY_TEXT = "🔒 <b>Политика конфиденциальности:</b>\n1. Собираем Telegram ID, ник и хэш телефона для верификации.\n2. Номер не передаётся третьим лицам.\n3. Сообщения хранятся ≤72 часов без согласия.\n4. Удаляйте данные командой /delete_data."
TERMS_TEXT = "📜 <b>Правила:</b>\n1. 16+.\n2. Запрещён спам и мошенничество.\n3. Бот «как есть».\n4. Играя, вы соглашаетесь с правилами."

# 🎮 СОСТОЯНИЯ
class GameStates(StatesGroup):
    waiting_contact = State()
    in_game = State()
    waiting_feedback = State()

# 🛠 ЛОГИКА ИГРЫ
@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await message.answer(DISCLAIMER, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принимаю условия и начинаю", callback_data="accept_rules")]
    ]))

@router.callback_query(F.data == "accept_rules")
async def accept_rules(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "🎯 <b>Как играть:</b>\nТы ведёшь диалог с «мошенником» и получаешь задания. "
        "Вплетаи их в разговор так, чтобы собеседник не заподозрил подвох.\n\n"
        "👇 Выбери режим:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 С регистрацией (доступ к рейтингу)", callback_data="reg_contact")],
            [InlineKeyboardButton(text="👤 Гостевой режим", callback_data="guest_mode")]
        ])
    )

@router.callback_query(F.data == "reg_contact")
async def req_contact(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "📱 Для доступа к лидерборду подтвердите номер телефона.\n"
        "Нажимая кнопку, вы даёте согласие на обработку номера для верификации.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="📤 Поделиться контактом", request_contact=True)]
        ], resize_keyboard=True)
    )
    await state.set_state(GameStates.waiting_contact)

@router.message(GameStates.waiting_contact, F.contact)
async def save_contact(message: types.Message, state: FSMContext):
    # Проверка: контакт должен принадлежать пользователю
    if message.contact.user_id != message.from_user.id:
        await message.answer("❌ Нельзя использовать чужой номер телефона!")
        return

    phone_hash = hash_phone(str(message.contact.phone_number))
    nickname = html.escape(message.from_user.username or "user") # Защита от XSS
    
    try:
        async with db_pool.cursor() as cursor:
            await cursor.execute("INSERT OR REPLACE INTO users VALUES (?,?,?,?,?,?)", 
                      (message.from_user.id, nickname, phone_hash, 0, 1, datetime.now().isoformat()))
        await db_pool.commit()
    except Exception as e:
        logger.error(f"DB Error in save_contact: {e}")
        await message.answer("❌ Произошла ошибка при сохранении данных. Попробуйте позже.")
        return

    await message.answer("✅ Контакт подтверждён! Очки попадут в рейтинг.", reply_markup=types.ReplyKeyboardRemove())
    await start_game(message, state, verified=True)

@router.callback_query(F.data == "guest_mode")
async def guest_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("👤 Вы играете как гость. Прогресс сохранится, но в рейтинг не попадёт.")
    await start_game(cb.message, state, verified=False)

async def start_game(message, state: FSMContext, verified: bool):
    try:
        async with db_pool.cursor() as cursor:
            await cursor.execute("SELECT * FROM tasks ORDER BY RANDOM() LIMIT 9")
            tasks = await cursor.fetchall()
        
        await state.update_data({
            "verified": verified,
            "tasks": tasks,
            "current_task_idx": 0,
            "tasks_done": 0,
            "start_time": time.time()
        })
        await state.set_state(GameStates.in_game)
        
        # Очищаем старую задачу, если она была
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
        logger.error(f"Error starting game for {message.from_user.id}: {e}")
        await message.answer("❌ Ошибка при начале игры. Попробуйте /start заново.")

@router.callback_query(F.data == "answer_call", GameStates.in_game)
async def answer_call(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data or "tasks" not in 
        await cb.message.answer("❌ Ошибка сессии. Начните заново через /start")
        return

    task_text = data["tasks"][0][2]
    
    await cb.message.answer(
        f"🕵️ <b>Мошенник:</b> Здравствуйте, это служба безопасности. Мы зафиксировали подозрительную операцию...\n\n"
        f"🎯 <b>ЗАДАНИЕ 1 (Раунд 1):</b> {task_text}\n"
        f"💡 Вплети это в диалог органично. Если напишешь «в лоб» — мошенник может заподозрить неладное!\n\n"
        "⏱ Таймер запущен. Пишите ответы сюда. Нажмите 📞 Завершить, когда готовы.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📞 Завершить звонок", callback_data="end_call")]
        ])
    )
    
    task = asyncio.create_task(scammer_background(cb.message.from_user.id, state))
    active_games[cb.message.from_user.id]["task"] = task

async def scammer_background(user_id: int, state: FSMContext):
    phrases = [
        "Не отвлекайтесь, нам нужно подтвердить данные карты.",
        "Почему вы молчите? Карта будет заблокирована!",
        "Продиктуйте код из СМС, это срочно.",
        "Вы меня слушаете? Операция отменится через минуту.",
        "Не нужно никуда перезванивать, я сейчас на линии."
    ]
    try:
        while True:
            await asyncio.sleep(random.randint(25, 40))
            data = await state.get_data()
            # ИСПРАВЛЕНИЕ КРИТИЧЕСКОЙ ОШИБКИ: добавлено 'data'
            if "start_time" not in 
                break
            await bot.send_message(user_id, f"🕵️ <b>Мошенник:</b> {random.choice(phrases)}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Error in scammer_background for {user_id}: {e}")

@router.message(GameStates.in_game)
async def handle_game_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data or "current_task_idx" not in 
        return
    
    tasks = data["tasks"]
    idx = data["current_task_idx"]
    if idx >= len(tasks): return
    
    task_row = tasks[idx]
    task_keywords = [w.strip() for w in task_row[3].split(",")]
    mask_words = [w.strip() for w in task_row[4].split(",")]
    
    msg_lower = (message.text or "").lower()
    found_keywords = any(kw in msg_lower for kw in task_keywords)
    found_masks = any(mw in msg_lower for mw in mask_words)
    
    reaction = "neutral" if (found_keywords and found_masks) else ("suspicion" if found_keywords else "ignore")
    await log_analytics("task_attempt", f"user={message.from_user.id}, task={idx}, reaction={reaction}")
    
    if reaction == "suspicion":
        await message.answer("🕵️ <b>Мошенник:</b> Так, с вами всё понятно. Ой, ну всё, с вами дальше бесполезно. *сбрасывает звонок*")
        await finish_game(message, state, forced_end=True)
        return
    
    if found_keywords:
        await state.update_data({"tasks_done": data["tasks_done"] + 1, "current_task_idx": idx + 1})
        next_idx = idx + 1
        if next_idx < len(tasks):
            round_name = ["Логика", "Актёрство", "Импровизация"][next_idx // 3]
            await message.answer(
                f"✅ Задание засчитано!\n\n"
                f"🎯 <b>ЗАДАНИЕ {next_idx+1} (Раунд {round_name}):</b> {tasks[next_idx][2]}\n"
                "💡 Вплети это в диалог органично."
            )
        else:
            await message.answer("✅ Все задания выполнены! Нажмите 📞 Завершить, чтобы увидеть результат.")
    else:
        await message.answer("🕵️ <b>Мошенник:</b> Вернёмся к безопасности вашей карты. Не отвлекайтесь.")

@router.callback_query(F.data == "end_call", GameStates.in_game)
async def finish_game_cb(cb: types.CallbackQuery, state: FSMContext):
    await finish_game(cb.message, state, forced_end=False)

async def finish_game(message, state: FSMContext, forced_end=False):
    data = await state.get_data()
    
    # ВАЛИДАЦИЯ ДАННЫХ (Этап 1)
    if not data or "start_time" not in 
        await message.answer("❌ Ошибка сессии. Данные потеряны.")
        return

    duration = time.time() - data["start_time"]
    tasks_done = data.get("tasks_done", 0)
    
    # ОЧИСТКА ПАМЯТИ (Этап 2)
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
    
    await log_analytics("session_end", f"duration={int(duration)}, tasks={tasks_done}, forced={forced_end}")
    
    try:
        async with db_pool.cursor() as cursor:
            await cursor.execute("INSERT INTO sessions VALUES (NULL,?,?,?,?,?,?)",
                      (message.from_user.id, data["start_time"], time.time(), duration, tasks_done, score, "ended"))
        await db_pool.commit()
    except Exception as e:
        logger.error(f"DB Error saving session for {message.from_user.id}: {e}")
    
    text = f"📞 <b>ВЫЗОВ ЗАВЕРШЁН</b>\n⏱ Длительность: {int(duration)} сек\n✅ Выполнено: {tasks_done}/9\n💰 Условно спасено: {int(score)} ₽\n"
    text += "🏆 <b>Результат попал в лидерборд!</b>\n" if in_leaderboard else "⏱ Разговор <3 мин. Для рейтинга нужно больше времени.\n"
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Оставить отзыв", callback_data="feedback_start")]
    ]))
    await state.clear()

# 📝 ОТЗЫВЫ
@router.callback_query(F.data == "feedback_start")
async def req_feedback(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("Оцените игру от 1 до 5:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(i), callback_data=f"fb_{i}") for i in range(1,6)]
    ]))
    await state.set_state(GameStates.waiting_feedback)

@router.callback_query(F.data.startswith("fb_"), GameStates.waiting_feedback)
async def save_feedback(cb: types.CallbackQuery, state: FSMContext):
    await state.update_data({"rating": cb.data.split("_")[1]})
    await cb.message.answer("Напишите комментарий (или отправьте /skip):")

@router.message(Command("skip"), GameStates.waiting_feedback)
@router.message(GameStates.waiting_feedback)
async def process_feedback(message: types.Message, state: FSMContext):
    data = await state.get_data()
    text = message.text if message.text != "/skip" else "Без комментария"
    
    # ЗАЩИТА ОТ XSS (Этап 2)
    safe_text = html.escape(text)
    safe_nick = html.escape(message.from_user.username or "Anon")
    
    await log_analytics("feedback", f"user={message.from_user.id}, rating={data.get('rating','?')}, comment={safe_text}")
    
    if ADMIN_ID:
        try: 
            await bot.send_message(ADMIN_ID, f"📝 Отзыв от {safe_nick}:\n⭐ {data.get('rating','?')}\n💬 {safe_text}")
        except Exception as e:
            logger.error(f"Failed to send feedback to admin: {e}")
            
    await message.answer("Спасибо за отзыв!")
    await state.clear()

# 🛠 ДОП. КОМАНДЫ
@router.message(Command("rules"))
async def cmd_rules(m: types.Message): await m.answer(RULES_TEXT)
@router.message(Command("privacy"))
async def cmd_priv(m: types.Message): await m.answer(PRIVACY_TEXT)
@router.message(Command("terms"))
async def cmd_terms(m: types.Message): await m.answer(TERMS_TEXT)

@router.message(Command("delete_data"))
async def cmd_delete(m: types.Message):
    try:
        async with db_pool.cursor() as cursor:
            await cursor.execute("DELETE FROM users WHERE tg_id=?", (m.from_user.id,))
            await cursor.execute("DELETE FROM sessions WHERE tg_id=?", (m.from_user.id,))
        await db_pool.commit()
        await m.answer("🗑️ Ваши данные удалены.")
    except Exception as e:
        logger.error(f"Error deleting data for {m.from_user.id}: {e}")
        await m.answer("❌ Ошибка при удалении данных.")

@router.message(Command("leaderboard"))
async def cmd_leaderboard(m: types.Message):
    try:
        async with db_pool.cursor() as cursor:
            await cursor.execute("""SELECT u.nickname, u.verified, MAX(s.score) as best 
                         FROM users u JOIN sessions s ON u.tg_id = s.tg_id 
                         WHERE s.duration >= 180 GROUP BY u.tg_id ORDER BY best DESC LIMIT 10""")
            rows = await cursor.fetchall()
        
        if not rows: return await m.answer("🏆 Рейтинг пока пуст.")
        
        text = "🏆 <b>ТОП-10 ИГРОКОВ</b>\n"
        for i, (nick, ver, sc) in enumerate(rows, 1):
            # ЗАЩИТА ОТ XSS (Этап 2)
            safe_nick = html.escape(nick or "Anon")
            text += f"{i}. {safe_nick} {'✅' if ver else ''} — {int(sc)} ₽\n"
        await m.answer(text)
    except Exception as e:
        logger.error(f"Error fetching leaderboard: {e}")
        await m.answer("❌ Ошибка при загрузке рейтинга.")

async def log_analytics(event, payload):
    try:
        async with db_pool.cursor() as cursor:
            await cursor.execute("INSERT INTO analytics VALUES (NULL, ?, ?, ?)", (event, payload, datetime.now().isoformat()))
        await db_pool.commit()
    except Exception as e:
        logger.error(f"Analytics error: {e}")

# 🚀 ГЛАВНАЯ ФУНКЦИЯ ЗАПУСКА (Этап 1)
async def main():
    global db_pool
    db_pool = await aiosqlite.connect(DB_PATH)
    await init_db(db_pool)
    logger.info("Database initialized and connected.")
    
    try:
        await dp.start_polling(bot)
    finally:
        await db_pool.close()
        logger.info("Bot stopped and DB connection closed.")

if __name__ == "__main__":
    if not TOKEN or "СЮДА" in TOKEN or "ВСТАВЬ" in TOKEN:
        print("❌ Ошибка: вставь токен и ADMIN_ID в .env!")
        exit()
    asyncio.run(main())