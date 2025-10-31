# CrptApi: Тестовое задание для API "Честный ЗНАК"

## Описание
Thread-safe Java-клиент для создания документов ввода в оборот (Java 11).

## Использование
```java
CrptApi api = new CrptApi(TimeUnit.SECONDS, 3);
String id = api.createDocument(doc, signature, "milk", token);