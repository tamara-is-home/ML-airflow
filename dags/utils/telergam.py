from telegram.ext import CommandHandler
from telegram.ext import Updater

# you will be able to interact with the bot - https://t.me/btc_ucu_bot

def send_message(prediction):
    
    updater = Updater(token='1749301024:AAHu2q_RkDMzLlesnQl8ysxeggP-BjGTPik', use_context=True)
    dispatcher = updater.dispatcher

    import logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                         level=logging.INFO)

    def start(update, context):
        if prediction == 0:
            context.bot.send_message(chat_id=update.effective_chat.id, text="Tomorrow BTC price will go DOWN, SELL it now!")
        else:
            context.bot.send_message(chat_id=update.effective_chat.id, text="Tomorrow BTC price will go UP, BUY it now!")
            
    start_handler = CommandHandler('start', start)
    dispatcher.add_handler(start_handler)

    updater.start_polling()
    
# now by running \START command i the bot you will get relevant price prediction for tomorrow
