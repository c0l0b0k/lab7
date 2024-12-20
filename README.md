# Лабораторная работа 7: Очередь сообщений - RabbitMQ

## Описание

В этой лабораторной работе разрабатываются два консольных приложения с использованием RabbitMQ для обмена сообщениями между компонентами.

**Producer**: Принимает HTTP(S) URL из аргументов командной строки, извлекает все внутренние ссылки на HTML-странице и помещает их в очередь RabbitMQ.

**Consumer**: Извлекает ссылки из очереди RabbitMQ, находит внутренние ссылки на этих страницах и помещает их обратно в очередь RabbitMQ.

### Зависимости

pip install -r requirements.txt

### Запуск

1. **Запуск RabbitMQ**

2. **Запуск Producer**:
   Пример:

   ```bash
   python producer.py http://www.mathprofi.ru/
   ```

3. **Запуск Consumer**:

   ```bash
   python consumer.py
   ```



- **TIMEOUT** - максимальное время ожидания сообщений в очереди (в секундах). Если очередь пуста в течение этого времени, consumer завершит свою работу.

