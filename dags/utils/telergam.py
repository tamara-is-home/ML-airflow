from some_telegram_API_package import telegram


def send_message(prediction):
    if prediction == 0:
        telegram.send('SELL')
    else:
        telegram.send('BUY')