import asyncio
import json
import os
import random
import re
import copy
from typing import Optional, Dict, Any

import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ------------------ Конфигурация ------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")               # Токен бота
API_BASE_URL = os.getenv("API_BASE_URL")         # Базовый URL API (например, "http://localhost:8188")
TEMPLATE_PATH = "template.json"                  # Путь к шаблону
# --------------------------------------------------

def parse_command(text: str) -> Optional[Dict[str, Any]]:
    """Разбирает строку команды /gen и извлекает параметры."""
    prompt_match = re.search(r'prompt:"(.*?)"', text)
    neg_match = re.search(r'neg:"(.*?)"', text)
    width_match = re.search(r'width:(\d+)', text)
    height_match = re.search(r'height:(\d+)', text)
    cfg_match = re.search(r'cfg:(\d+(?:\.\d+)?)', text)
    seed_match = re.search(r'seed:(\d+)', text)
    steps_match = re.search(r'steps:(\d+)', text)

    if not prompt_match or not width_match or not height_match:
        return None

    prompt = prompt_match.group(1)
    neg = neg_match.group(1) if neg_match else "low quality, worst quality, low detail, bad drawing, bad quality, oldest, (score_3, score_2, score_1:0.25), jpeg artifacts, watermark, signature, artist name, low detail, missing head, missing limb, text, bad anatomy, bad proportions, bad hands, missing fingers, "
    width = int(width_match.group(1))
    height = int(height_match.group(1))
    cfg = float(cfg_match.group(1)) if cfg_match else float(7.0)
    seed = int(seed_match.group(1)) if seed_match else random.randint(100_000_000_000_000, 999_999_999_999_999)
    steps = int(steps_match.group(1)) if steps_match else 30 

    return {"prompt": prompt, "neg": neg, "width": width, "height": height, "cfg": cfg, "seed": seed, "steps": steps}

async def gen_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /gen."""
    if not update.message:
        return

    session: aiohttp.ClientSession = context.bot_data["session"]
    api_base: str = context.bot_data["api_base"]
    base_template: dict = context.bot_data["template"]

    args = parse_command(update.message.text)
    if args is None:
        await update.message.reply_text(
            "❌ Неверный формат команды.\n"
            "Используйте: /gen prompt:\"текст\" [neg:\"текст\"] width:ЧИСЛО height:ЧИСЛО [cfg:ЧИСЛО] [seed:ЧИСЛО] [steps:ЧИСЛО]"
        )
        return

    prompt_text = args["prompt"]
    print(f'DEBUG: prompt text: {prompt_text}')
    neg_text = args["neg"]
    print(f'DEBUG: negative text: {neg_text}')
    width = args["width"]
    print(f'DEBUG: width: {width}')
    height = args["height"]
    print(f'DEBUG: height: {height}')
    cfg = args["cfg"]
    print(f'DEBUG: cfg: {cfg}')
    seed = args["seed"]
    print(f'DEBUG: seed: {seed}')
    steps = args["steps"]
    print(f'DEBUG: steps: {steps}')

    payload = copy.deepcopy(base_template)
    try:
        payload["prompt"]["6"]["inputs"]["text"] = prompt_text
        payload["prompt"]["7"]["inputs"]["text"] = neg_text
        payload["prompt"]["5"]["inputs"]["width"] = width
        payload["prompt"]["5"]["inputs"]["height"] = height
        payload["prompt"]["3"]["inputs"]["cfg"] = cfg 
        payload["prompt"]["3"]["inputs"]["seed"] = seed
        payload["prompt"]["3"]["inputs"]["steps"] = steps
    except KeyError:
        await update.message.reply_text("❌ Ошибка структуры template.json. Проверьте ключи.")
        return

    # 1. POST /prompt
    try:
        async with session.post(f"{api_base}/prompt", json=payload) as resp:
            if resp.status != 200:
                await update.message.reply_text(f"❌ Ошибка API: {resp.status}")
                return
            data = await resp.json()
            prompt_id = data.get("prompt_id")
            if not prompt_id:
                await update.message.reply_text("❌ Не получен prompt_id от API.")
                return
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка соединения с API: {e}")
        return

    # 2. WebSocket – ждём queue_remaining == 0
    if "://" in api_base:
        ws_host = api_base.split("://", 1)[1]
    else:
        ws_host = api_base
    ws_url = f"ws://{ws_host}/ws"

    status_received = False
    max_retries = 1000

    for attempt in range(max_retries):
        try:
            async with session.ws_connect(ws_url) as ws:
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.receive_json(), timeout=1.0)
                    except asyncio.TimeoutError:
                        break  # переподключаемся

                    try:
                        exec_info = msg["data"]["status"]["exec_info"]
                        if exec_info.get("queue_remaining") == 0:
                            status_received = True
                            break
                    except (KeyError, TypeError):
                        continue

                if status_received:
                    break
        except Exception:
            await asyncio.sleep(0.5)

    if not status_received:
        await update.message.reply_text("❌ Не удалось дождаться завершения генерации.")
        return

    # 3. Получаем имя файла
    try:
        async with session.get(f"{api_base}/history/{prompt_id}") as resp:
            if resp.status != 200:
                await update.message.reply_text(f"❌ Ошибка получения истории: {resp.status}")
                return
            history = await resp.json()
            filename = history[str(prompt_id)]["outputs"]["9"]["images"][0]["filename"]
    except (KeyError, IndexError, Exception) as e:
        await update.message.reply_text(f"❌ Не удалось извлечь имя файла: {e}")
        return

    # 4. Скачиваем и отправляем изображение
    try:
        async with session.get(f"{api_base}/view", params={"filename": filename, "type": "output"}) as resp:
            if resp.status != 200:
                await update.message.reply_text(f"❌ Ошибка загрузки изображения: {resp.status}")
                return
            image_bytes = await resp.read()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при скачивании изображения: {e}")
        return

    await update.message.reply_photo(
        photo=image_bytes,
        caption=f"✅ Готово!\nseed: {seed}",
        reply_to_message_id=update.message.message_id
    )

async def post_init(application: Application) -> None:
    """Создаём aiohttp-сессию после запуска event loop."""
    application.bot_data["session"] = aiohttp.ClientSession()

async def post_shutdown(application: Application) -> None:
    """Закрываем сессию при остановке."""
    session: aiohttp.ClientSession = application.bot_data.get("session")
    if session:
        await session.close()

def main() -> None:
    """Запуск бота."""
    if not BOT_TOKEN or not API_BASE_URL:
        raise ValueError("Переменные BOT_TOKEN и API_BASE_URL должны быть заданы")
    if not os.path.exists(TEMPLATE_PATH):
        raise FileNotFoundError(f"Файл шаблона {TEMPLATE_PATH} не найден")

    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        template = json.load(f)

    application = Application.builder().token(BOT_TOKEN).build()
    application.bot_data["api_base"] = API_BASE_URL.rstrip("/")
    application.bot_data["template"] = template

    # Регистрируем асинхронные инициализацию и завершение
    application.post_init = post_init
    application.post_shutdown = post_shutdown

    application.add_handler(CommandHandler("gen", gen_command))

    print("Бот запущен...")
    application.run_polling()

if __name__ == "__main__":
    main()