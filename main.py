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

# НАСТРОЙКА ЛОГИРОВАНИЯ
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ЗАГРУЗКА КОНФИГУРАЦИИ
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
PHONE_SALT = os.getenv("PHONE_SALT", "default_secret_salt").encode()
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", 8080))

# ИНИЦИАЛИЗАЦИЯ БОТА
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)
db_pool = None
active_tasks = {}  # Для хранения фоновых задач мошенника

# ПОСТОЯННОЕ МЕНЮ
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🎮 Новая игра"), KeyboardButton(text="🏆 Рейтинг")],
        [KeyboardButton(text="📖 Правила"), KeyboardButton(text="📜 Документы")],
        [KeyboardButton(text="🗑 Удалить данные")]
    ],
    resize_keyboard=True
)

# СОСТОЯНИЯ
class GameStates(StatesGroup):
    in_game = State()
    waiting_feedback = State()

# ХЭШИРОВАНИЕ ТЕЛЕФОНА
def hash_phone(phone: str) -> str:
    return hmac.new(PHONE_SALT, phone.encode(), hashlib.sha256).hexdigest()

# ИНИЦИАЛИЗАЦИЯ БД
async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                tg_id BIGINT PRIMARY KEY, nickname TEXT, phone_hash TEXT, 
                verified INT DEFAULT 0, created_at TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY, round_num INT, text TEXT, 
                keywords TEXT, mask_words TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id SERIAL PRIMARY KEY, tg_id BIGINT, start_time FLOAT, 
                end_time FLOAT, duration FLOAT, tasks_done INT, 
                score FLOAT, status TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics (
                id SERIAL PRIMARY KEY, event TEXT, payload TEXT, created_at TEXT
            )
        """)
        
        row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM tasks")
        if row["cnt"] == 0:
            tasks_data = [
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
            await conn.executemany(
                "INSERT INTO tasks (round_num, text, keywords, mask_words) VALUES ($1, $2, $3, $4)",
                tasks_data
            )
    logger.info("✅ База данных инициализирована")

# ТЕКСТЫ
DISCLAIMER = (
    "⚠️ <b>ВНИМАНИЕ:</b> Это учебный симулятор. Все диалоги, номера и сценарии вымышлены.\n\n"
    "🎯 <b>Цель проекта:</b> тренировка навыков распознавания мошеннических схем в безопасной среде.\n\n"
    "🚫 <b>НИКОГДА</b> не сообщай реальные данные (номера карт, пароли, коды из СМС) в этом боте или в подозрительных звонках.\n\n"
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

PRIVACY_TEXT = (
    "🔒 <b>Политика конфиденциальности:</b>\n"
    "1. Собираем Telegram ID, ник и хэш телефона для верификации.\n"
    "2. Номер не передаётся третьим лицам.\n"
    "3. Сообщения хранятся ≤72 часов без согласия.\n"
    "4. Удаляйте данные командой /delete_data или кнопкой в меню."
)

TERMS_TEXT = (
    "📜 <b>Правила использования:</b>\n"
    "1. Доступно лицам от 16 лет.\n"
    "2. Запрещён спам, мошенничество и нарушение работы бота.\n"
    "3. Бот предоставляется «как есть».\n"
    "4. Используя бота, вы соглашаетесь с правилами."
)

HELP_TEXT = (
    "📚 <b>Список команд:</b>\n"
    "/start — Начать игру или главное меню\n"
    "/rules — Как выполнять задания\n"
    "/privacy — Политика конфиденциальности\n"
    "/terms — Правила использования\n"
    "/leaderboard — Топ игроков\n"
    "/delete_data — Удалить мои данные\n"
    "/help — Это сообщение"
)

# ОБРАБОТЧИКИ
@router.message(CommandStart() | F.text == "🎮 Новая игра")
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(DISCLAIMER, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принимаю условия и начинаю", callback_data="accept_rules")]
    ]))

@router.message(F.text == "🏆 Рейтинг")
@router.message(Command("leaderboard"))
async def cmd_leaderboard(message: types.Message):
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT u.nickname, u.verified, MAX(s.score) as best 
                FROM users u JOIN sessions s ON u.tg_id = s.tg_id 
                WHERE s.duration >= 180 GROUP BY u.tg_id ORDER BY best DESC LIMIT 10
            """)
        if not rows:
            return await message.answer("🏆 Рейтинг пока пуст.", reply_markup=MAIN_KEYBOARD)
        
        text = "🏆 <b>ТОП-10 ИГРОКОВ</b>\n"
        for i, row in enumerate(rows, 1):
            safe_nick = html.escape(row["nickname"] or "Anon")
            text += f"{i}. {safe_nick} {'✅' if row['verified'] else ''} — {int(row['best'])} ₽\n"
        await message.answer(text, reply_markup=MAIN_KEYBOARD)
    except Exception as e:
        logger.error(f"Leaderboard error: {e}")
        await message.answer("❌ Ошибка загрузки рейтинга.", reply_markup=MAIN_KEYBOARD)

@router.message(F.text == "📖 Правила")
@router.message(Command("rules"))
async def cmd_rules(message: types.Message):
    await message.answer(RULES_TEXT, reply_markup=MAIN_KEYBOARD)

@router.message(F.text == "📜 Документы")
async def cmd_docs_menu(message: types.Message):
    await message.answer("Выбери документ:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔒 Конфиденциальность", callback_data="show_privacy")],
        [InlineKeyboardButton(text="📜 Правила", callback_data="show_terms")]
    ]))

@router.callback_query(F.data == "show_privacy")
async def show_privacy(cb: types.CallbackQuery):
    await cb.message.answer(PRIVACY_TEXT, reply_markup=MAIN_KEYBOARD)
    await cb.answer()

@router.callback_query(F.data == "show_terms")
async def show_terms(cb: types.CallbackQuery):
    await cb.message.answer(TERMS_TEXT, reply_markup=MAIN_KEYBOARD)
    await cb.answer()

@router.message(F.text == "🗑 Удалить данные")
@router.message(Command("delete_data"))
async def cmd_delete(message: types.Message, state: FSMContext):
    await state.clear()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM users WHERE tg_id=$1", message.from_user.id)
            await conn.execute("DELETE FROM sessions WHERE tg_id=$1", message.from_user.id)
        await message.answer("🗑️ Ваши данные удалены.", reply_markup=MAIN_KEYBOARD)
    except Exception as e:
        logger.error(f"Delete error: {e}")
        await message.answer("❌ Ошибка при удалении.", reply_markup=MAIN_KEYBOARD)

@router.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(HELP_TEXT, reply_markup=MAIN_KEYBOARD)

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
    await cb.answer()

@router.callback_query(F.data == "reg_contact")
async def req_contact(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "📱 Для доступа к лидерборду подтвердите номер телефона.\n"
        "Нажимая кнопку, вы даёте согласие на обработку номера для верификации.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="📤 Поделиться контактом", request_contact=True)]
        ], resize_keyboard=True, one_time_keyboard=True)
    )
    await state.set_state(GameStates.in_game) # Временное состояние для ожидания контакта
    await cb.answer()

@router.message(GameStates.in_game, F.contact)
async def save_contact(message: types.Message, state: FSMContext):
    if message.contact.user_id != message.from_user.id:
        return await message.answer("❌ Нельзя использовать чужой номер телефона!", reply_markup=MAIN_KEYBOARD)
    
    phone_hash = hash_phone(str(message.contact.phone_number))
    nickname = html.escape(message.from_user.username or "user")
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (tg_id, nickname, phone_hash, verified, created_at) VALUES ($1, $2, $3, 1, $4) "
                "ON CONFLICT (tg_id) DO UPDATE SET nickname=$2, phone_hash=$3, verified=1",
                message.from_user.id, nickname, phone_hash, datetime.now().isoformat()
            )
    except Exception as e:
        logger.error(f"DB Error: {e}")
        return await message.answer("❌ Ошибка сохранения.", reply_markup=MAIN_KEYBOARD)
    
    await message.answer("✅ Контакт подтверждён! Очки попадут в рейтинг.", reply_markup=MAIN_KEYBOARD)
    await start_game(message, state, verified=True)

@router.callback_query(F.data == "guest_mode")
async def guest_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("👤 Вы играете как гость. Прогресс сохранится, но в рейтинг не попадёт.", reply_markup=MAIN_KEYBOARD)
    await start_game(cb.message, state, verified=False)
    await cb.answer()

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
        
        # Очистка старой задачи, если была
        if message.from_user.id in active_tasks:
            old_task = active_tasks[message.from_user.id]
            if old_task and not old_task.done():
                old_task.cancel()
                try: await old_task
                except asyncio.CancelledError: pass
        
        await message.answer(
            "📞 <b>ВХОДЯЩИЙ ВЫЗОВ...</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Принять вызов", callback_data="answer_call")]
            ])
        )
    except Exception as e:
        logger.error(f"Start game error: {e}")
        await message.answer("❌ Ошибка при начале игры. Попробуйте /start заново.", reply_markup=MAIN_KEYBOARD)

@router.callback_query(F.data == "answer_call", GameStates.in_game)
async def answer_call(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data or "tasks" not in 
        return await cb.message.answer("❌ Ошибка сессии. Начните заново через /start")
    
    task_text = data["tasks"][0]["text"]
    round_name = ["Логика", "Актёрство", "Импровизация"][data["tasks"][0]["round_num"]-1]
    
    await cb.message.answer(
        f"🕵️ <b>Мошенник:</b> Здравствуйте, это служба безопасности. Мы зафиксировали подозрительную операцию...\n\n"
        f"🎯 <b>ЗАДАНИЕ 1 (Раунд: {round_name})</b>\n"
        f"📝 {task_text}\n"
        f"💡 Вплети это в диалог органично. Если напишешь «в лоб» — мошенник может заподозрить неладное!\n\n"
        "⏱ Таймер запущен. Пишите ответы сюда. Нажмите 📞 Завершить, когда готовы.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📞 Завершить звонок", callback_data="end_call")]
        ])
    )
    
    task = asyncio.create_task(scammer_background(cb.message.from_user.id, state))
    active_tasks[cb.message.from_user.id] = task
    await cb.answer()

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
            if not data or "start_time" not in 
                break
            await bot.send_message(user_id, f"🕵️ <b>Мошенник:</b> {random.choice(phrases)}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Scammer bg error: {e}")

@router.message(GameStates.in_game)
async def handle_game_message(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data or "current_task_idx" not in 
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
    
    if reaction == "suspicion":
        await message.answer("🕵️ <b>Мошенник:</b> Так, с вами всё понятно. Ой, ну всё, с вами дальше бесполезно. *сбрасывает звонок*")
        await finish_game(message, state, forced_end=True)
        return
    
    if found_keywords:
        await state.update_data({"tasks_done": data["tasks_done"] + 1, "current_task_idx": idx + 1})
        next_idx = idx + 1
        if next_idx < len(tasks):
            round_name = ["Логика", "Актёрство", "Импровизация"][tasks[next_idx]["round_num"]-1]
            await message.answer(
                f"✅ Задание засчитано!\n\n"
                f"🎯 <b>ЗАДАНИЕ {next_idx+1} (Раунд: {round_name})</b>\n"
                f"📝 {tasks[next_idx]['text']}\n"
                "💡 Вплети это в диалог органично."
            )
        else:
            await message.answer("✅ Все задания выполнены! Нажмите 📞 Завершить, чтобы увидеть результат.")
    else:
        await message.answer("🕵️ <b>Мошенник:</b> Вернёмся к безопасности вашей карты. Не отвлекайтесь.")

@router.callback_query(F.data == "end_call", GameStates.in_game)
async def finish_game_cb(cb: types.CallbackQuery, state: FSMContext):
    await finish_game(cb.message, state, forced_end=False)
    await cb.answer()

async def finish_game(message, state: FSMContext, forced_end=False):
    data = await state.get_data()
    if not data or "start_time" not in 
        await message.answer("❌ Ошибка сессии.", reply_markup=MAIN_KEYBOARD)
        return

    duration = time.time() - data["start_time"]
    tasks_done = data.get("tasks_done", 0)
    
    # Отмена фоновой задачи
    if message.from_user.id in active_tasks:
        ag = active_tasks[message.from_user.id]
        if ag and not ag.done():
            ag.cancel()
            try: await ag
            except asyncio.CancelledError: pass
        del active_tasks[message.from_user.id]
    
    score = 0.0
    in_leaderboard = duration >= 180 and tasks_done > 0
    if in_leaderboard:
        score = duration * 8 * (1 + tasks_done * 0.2)
    
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sessions (tg_id, start_time, end_time, duration, tasks_done, score, status) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                message.from_user.id, data["start_time"], time.time(), duration, tasks_done, score, "ended"
            )
    except Exception as e:
        logger.error(f"Session save error: {e}")
    
    text = f"📞 <b>ВЫЗОВ ЗАВЕРШЁН</b>\n⏱ Длительность: {int(duration)} сек\n✅ Выполнено: {tasks_done}/9\n💰 Условно спасено: {int(score)} ₽\n"
    text += "🏆 <b>Результат попал в лидерборд!</b>\n" if in_leaderboard else "⏱ Разговор <3 мин. Для рейтинга нужно больше времени.\n"
    
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Оставить отзыв", callback_data="feedback_start")],
        [InlineKeyboardButton(text="🔙 В главное меню", callback_data="back_menu")]
    ]))
    await state.clear()

@router.callback_query(F.data == "feedback_start")
async def req_feedback(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("Оцените игру от 1 до 5:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(i), callback_data=f"fb_{i}") for i in range(1,6)]
    ]))
    await state.set_state(GameStates.waiting_feedback)
    await cb.answer()

@router.callback_query(F.data == "back_menu")
async def back_to_menu(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("🔙 Возвращено в главное меню.", reply_markup=MAIN_KEYBOARD)
    await cb.answer()

@router.callback_query(F.data.startswith("fb_"), GameStates.waiting_feedback)
async def save_feedback(cb: types.CallbackQuery, state: FSMContext):
    await state.update_data({"rating": cb.data.split("_")[1]})
    await cb.message.answer("Напишите комментарий (или отправьте /skip):")
    await cb.answer()

@router.message(Command("skip"), GameStates.waiting_feedback)
@router.message(GameStates.waiting_feedback)
async def process_feedback(message: types.Message, state: FSMContext):
    data = await state.get_data()
    text = message.text if message.text != "/skip" else "Без комментария"
    
    safe_text = html.escape(text)
    safe_nick = html.escape(message.from_user.username or "Anon")
    
    if ADMIN_ID:
        try: 
            await bot.send_message(ADMIN_ID, f"📝 Отзыв от {safe_nick}:\n⭐ {data.get('rating','?')}\n💬 {safe_text}")
        except Exception as e:
            logger.error(f"Feedback send error: {e}")
            
    await message.answer("Спасибо за отзыв!", reply_markup=MAIN_KEYBOARD)
    await state.clear()

# ВЕБХУК И ЗАПУСК
async def webhook_handler(request: web.Request):
    try:
        update_data = await request.json()
        update = types.Update(**update_data)
        await dp.feed_update(bot, update)
        return web.Response()
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return web.Response(status=500)

async def on_startup(app):
    await init_db()
    hostname = os.getenv('RENDER_EXTERNAL_HOSTNAME', 'localhost')
    webhook_url = f"https://{hostname}/bot{TOKEN}"
    await bot.set_webhook(webhook_url)
    logger.info(f"🌐 Webhook set: {webhook_url}")

async def on_shutdown(app):
    if db_pool:
        await db_pool.close()
    await bot.delete_webhook()
    await bot.session.close()
    logger.info("🛑 Bot stopped")

if __name__ == "__main__":
    if not TOKEN or "СЮДА" in TOKEN:
        print("❌ Ошибка: вставь токен в .env или переменные Render!")
        exit()
    if not DATABASE_URL:
        print("❌ Ошибка: DATABASE_URL не найден!")
        exit()
        
    app = web.Application()
    app.router.add_post(f"/bot{TOKEN}", webhook_handler)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    
    logger.info("🚀 Запуск бота...")
    web.run_app(app, host="0.0.0.0", port=PORT)