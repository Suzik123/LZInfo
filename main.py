
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler
import re
import logging
from query import *
import pandas as pd

# Инициализация логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                     level=logging.INFO)
def len_condition(text, lenght, size):
    difference = lenght - len(text)
    while difference!=0:
        text = text + " "*size
        difference = difference - 1
    return text
async def start(update, context):
    hello = '''
    Hello, this bot designed to help you get information about your LZ activity. To start paste into bot your wallet's addresses'''
    await context.bot.send_message(chat_id=update.effective_chat.id, text=hello)

async def get_statistic(update, context):
    user = update.effective_user.username
    wallets = re.findall('0x[a-z0-9A-Z]{40}', update.message.text)
    if len(wallets)==0:
        await context.bot.send_message(chat_id=update.effective_chat.id, text='No address')
        return 0
    version = 2
    if version==2:
        wallets_v2 = ', '.join(wallets)
        table_name = get_stats_v2(wallets_v2, user)
    else:
        table_name = get_all_wallets_stats(wallets, user)
    conn = sqlite3.connect('user_stats_db.sqlite')
    cur = conn.cursor()
    first_row = ("```"
                 " Num | Address | Volume | S-rce_chains | Des-tion chains | Months"
                 "```")

    await context.bot.send_message(chat_id=update.effective_chat.id, text=first_row, parse_mode="Markdown")
    for wallet in wallets:
        query = "SELECT * FROM {table_name} WHERE address = ?".format(table_name=table_name)
        cur.execute(query, (wallet,))
        result = cur.fetchall()[0]  # Извлекает все строки
        print(result)
    #   num, address, volume = str(result[0]), result[1][:8], str(int(result[2]))
       #source_chains, destination_chains, months = str(result[3]), str(result[4]), str(result[5])

       # wal_info = f"{num:3} | {address:9} | {volume:8} | {source_chains:22} | {destination_chains:27} | {months:7}"
        wal_info ="``` "+len_condition(str(result[0]), 3, 1)+" | " + result[1][:7] + " |" + len_condition(str(int(result[2]))+"$", 8, 1 ) + "| "
        wal_info = wal_info + len_condition(str(result[4]), 13, 1) + "| " + len_condition(str(result[5]), 16, 1) + "| "
        wal_info = wal_info + len_condition(str(result[3]), 10, 1) + " ```"
        await context.bot.send_message(chat_id=update.effective_chat.id, text=wal_info, parse_mode="Markdown")


class TgBot():
    def __init__(self):
        TOKEN = '<your-token>'
        self.application = ApplicationBuilder().token(TOKEN).build()

    def add_handler(self, handler):
        self.application.add_handler(handler)

    def start_bot(self):
        self.application.run_polling()



Bot = TgBot()
Bot.add_handler(CommandHandler('start', start))
Bot.add_handler(MessageHandler(None, get_statistic))
Bot.start_bot()
