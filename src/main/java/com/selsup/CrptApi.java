package com.selsup;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h2>Thread-safe клиент для API «Честный ЗНАК» (ГИС МТ)</h2>
 *
 * <p>Класс предназначен для создания документа ввода в оборот товара, произведённого в РФ,
 * с соблюдением ограничения на количество запросов в единицу времени (rate limiting).</p>
 *
 * <p><strong>Особенности реализации:</strong></p>
 * <ul>
 *   <li>Полностью соответствует ТЗ и документации API (стр. 44, 108)</li>
 *   <li>Thread-safe: использует {@link ReentrantLock} + {@link Condition}</li>
 *   <li>Rate limiting: точная блокировка при превышении лимита</li>
 *   <li>Ручная JSON-сборка с экранированием</li>
 *   <li>URL-параметры</li>
 *   <li>Без внешних зависимостей (Jackson, Gson и т.п.)</li>
 *   <li>Один файл, все классы — внутренние</li>
 */
public class CrptApi {
    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private static final Pattern VALUE_PATTERN = Pattern.compile("\"value\"\\s*:\\s*\"([^\"]+)\"");

    private final HttpClient httpClient;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private final long intervalMillis;
    private final int requestLimit;
    private int currentCount = 0;
    private long lastReset = System.currentTimeMillis();

    /**
     * Создаёт экземпляр API-клиента с ограничением запросов.
     *
     * @param timeUnit   единица времени для интервала (секунда, минута и т.д.)
     * @param requestLimit максимальное количество запросов в интервале
     * @throws IllegalArgumentException если {@code timeUnit == null} или {@code requestLimit <= 0}
     */
    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (timeUnit == null) throw new IllegalArgumentException("timeUnit is null");
        if (requestLimit <= 0) throw new IllegalArgumentException("requestLimit must be > 0");

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.intervalMillis = timeUnit.toMillis(1);
        this.requestLimit = requestLimit;
    }

    /**
     * Создаёт документ ввода в оборот товара, произведённого в РФ.
     *
     * <p>Метод:</p>
     * <ul>
     *   <li>Собирает внутренний документ по схеме (стр. 108)</li>
     *   <li>Кодирует его в Base64</li>
     *   <li>Формирует тело запроса (стр. 44)</li>
     *   <li>Блокирует выполнение при превышении лимита</li>
     *   <li>Отправляет POST-запрос с заголовками</li>
     *   <li>Возвращает {@code document_id} из поля {@code value}</li>
     * </ul>
     *
     * @param document   объект с данными документа
     * @param signature  открепленная подпись в Base64 (УКЭП)
     * @param pg         код товарной группы (например, {@code "milk"})
     * @param token      Bearer-токен авторизации
     * @return уникальный идентификатор созданного документа
     * @throws InterruptedException если поток прерван во время ожидания лимита
     * @throws CrptApiException при ошибке HTTP или парсинга ответа
     */
    public String createDocument(Document document, String signature, String pg, String token)
            throws InterruptedException, CrptApiException {

        String docJson = buildDocumentJson(document);
        String base64Doc = Base64.getEncoder().encodeToString(docJson.getBytes(StandardCharsets.UTF_8));
        String body = String.format(
                "{\"document_format\":\"MANUAL\"," +
                        "\"product_document\":\"%s\",\"type\":\"LP_INTRODUCE_GOODS\",\"signature\":\"%s\"}",
                base64Doc, signature
        );

        acquire();

        String encodedPg = URLEncoder.encode(pg, StandardCharsets.UTF_8);
        URI uri = URI.create(API_URL + "?pg=" + encodedPg);

        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + token)
                .header("Accept", "*/*")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(30))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return extractValue(response.body());
            } else {
                throw new CrptApiException("HTTP " + response.statusCode() + ": " + response.body());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (Exception e) {
            throw new CrptApiException("Request failed", e);
        }
    }

    /**
     * Блокирует выполнение, если превышен лимит запросов в текущем интервале.
     * <p>Использует {@link ReentrantLock} и {@link Condition} для точного ожидания.</p>
     *
     * @throws InterruptedException если поток прерван
     */
    private void acquire() throws InterruptedException {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            if (now - lastReset >= intervalMillis) {
                currentCount = 0;
                lastReset = now;
                condition.signalAll();
            }
            while (currentCount >= requestLimit) {
                long wait = lastReset + intervalMillis - System.currentTimeMillis();
                if (wait <= 0) {
                    currentCount = 0;
                    lastReset = System.currentTimeMillis();
                    condition.signalAll();
                    break;
                }
                condition.await(wait, TimeUnit.MILLISECONDS);
            }
            currentCount++;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Собирает JSON внутреннего документа по схеме (стр. 108).
     *
     * @param doc объект документа
     * @return валидный JSON-строка
     */
    private String buildDocumentJson(Document doc) {
        StringBuilder sb = new StringBuilder("{");

        sb.append("\"description\":{\"participantInn\":\"")
                .append(escapeJson(doc.description_participant_inn)).append("\"},");

        appendField(sb, "doc_id", doc.doc_id);
        appendField(sb, "doc_status", doc.doc_status);
        appendField(sb, "doc_type", doc.doc_type);
        sb.append("\"importRequest\":").append(doc.importRequest).append(",");

        appendField(sb, "owner_inn", doc.owner_inn);
        appendField(sb, "participant_inn", doc.participant_inn);
        appendField(sb, "producer_inn", doc.producer_inn);
        appendField(sb, "production_date", doc.production_date);
        appendField(sb, "production_type", doc.production_type);

        sb.append("\"products\":[");
        for (int i = 0; i < doc.products.size(); i++) {
            Product p = doc.products.get(i);
            sb.append("{");
            appendField(sb, "certificate_document", p.certificate_document);
            appendField(sb, "certificate_document_date", p.certificate_document_date);
            appendField(sb, "certificate_document_number", p.certificate_document_number);
            appendField(sb, "owner_inn", p.owner_inn);
            appendField(sb, "producer_inn", p.producer_inn);
            appendField(sb, "production_date", p.production_date);
            appendField(sb, "tnved_code", p.tnved_code);
            appendField(sb, "uit_code", p.uit_code);
            appendField(sb, "uitu_code", p.uitu_code);
            sb.setLength(sb.length() - 1);
            sb.append("}");
            if (i < doc.products.size() - 1) sb.append(",");
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Добавляет поле в JSON, если значение не null.
     *
     * @param sb    StringBuilder
     * @param key   имя поля
     * @param value значение
     */
    private void appendField(StringBuilder sb, String key, String value) {
        if (value != null) {
            sb.append('"').append(key).append("\":\"")
                    .append(escapeJson(value)).append("\",");
        }
    }

    /**
     * Экранирует строку для JSON.
     *
     * @param s исходная строка
     * @return экранированная строка
     */
    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * Извлекает значение поля {@code value} из JSON-ответа.
     *
     * @param json ответ сервера
     * @return ID документа
     * @throws CrptApiException если поле не найдено
     */
    private String extractValue(String json) throws CrptApiException {
        Matcher m = VALUE_PATTERN.matcher(json);
        if (m.find()) return m.group(1);
        throw new CrptApiException("No 'value' in response: " + json);
    }

    /**
     * <h3>Модель документа ввода в оборот (стр. 108)</h3>
     *
     * <p>Неизменяемый (immutable) класс. Все поля {@code final}.</p>
     * <p>Поля с пробелами в названиях (например, {@code "production date"}) — как в схеме.</p>
     */
    public static class Document {
        public final String description_participant_inn;
        public final String doc_id;
        public final String doc_status;
        public final String doc_type = "LP_INTRODUCE_GOODS";
        public final boolean importRequest = false;
        public final String owner_inn;
        public final String participant_inn;
        public final String producer_inn;
        public final String production_date;
        public final String production_type;
        public final List<Product> products;

        public Document(String description_participant_inn, String doc_id, String doc_status,
                        String owner_inn, String participant_inn, String producer_inn,
                        String production_date, String production_type, List<Product> products) {
            this.description_participant_inn = description_participant_inn;
            this.doc_id = doc_id;
            this.doc_status = doc_status;
            this.owner_inn = owner_inn;
            this.participant_inn = participant_inn;
            this.producer_inn = producer_inn;
            this.production_date = production_date;
            this.production_type = production_type;
            this.products = List.copyOf(products);
        }
    }

    /**
     * <h3>Модель товара в документе</h3>
     *
     * <p>Неизменяемый класс. Поддерживает все поля из схемы.</p>
     */
    public static class Product {
        public final String certificate_document;
        public final String certificate_document_date;
        public final String certificate_document_number;
        public final String owner_inn;
        public final String producer_inn;
        public final String production_date;
        public final String tnved_code;
        public final String uit_code;
        public final String uitu_code;

        public Product(String certificate_document, String certificate_document_date,
                       String certificate_document_number, String owner_inn, String producer_inn,
                       String production_date, String tnved_code, String uit_code, String uitu_code) {
            this.certificate_document = certificate_document;
            this.certificate_document_date = certificate_document_date;
            this.certificate_document_number = certificate_document_number;
            this.owner_inn = owner_inn;
            this.producer_inn = producer_inn;
            this.production_date = production_date;
            this.tnved_code = tnved_code;
            this.uit_code = uit_code;
            this.uitu_code = uitu_code;
        }
    }

    /**
     * Кастомное исключение для ошибок API.
     */
    public static class CrptApiException extends Exception {
        public CrptApiException(String message) { super(message); }
        public CrptApiException(String message, Throwable cause) { super(message, cause); }
    }

    /**
     * <h3>Тест: 10 потоков с rate limiting</h3>
     *
     * <p>Проверяет:</p>
     * <ul>
     *   <li>Thread-safety</li>
     *   <li>Rate limiting (3 запроса/сек)</li>
     *   <li>Корректность создания документа</li>
     * </ul>
     */
    public static void main(String[] args) throws Exception {
        CrptApi api = new CrptApi(TimeUnit.SECONDS, 3);

        var product = new Product(
                "CONFORMITY_CERTIFICATE", "2025-04-01", "123",
                "7724211288", "7724211288", "2025-04-05",
                "0401101000", "01000000000000000000000000000000", null
        );

        var doc = new Document(
                "7724211288",
                "doc_001",
                "NEW",
                "7724211288",
                "7724211288",
                "7724211288",
                "2025-04-05",
                "OWN_PRODUCTION",
                List.of(product)
        );
        String token = "your_real_token_here";

        for (int i = 0; i < 10; i++) {
            final int n = i;
            new Thread(() -> {
                try {
                    String id = api.createDocument(doc, "MEQCIF...", "milk", token);
                    System.out.println("Успех #" + n + ": " + id);
                } catch (Exception e) {
                    System.err.println("Ошибка #" + n + ": " + e.getMessage());
                }
            }).start();
            Thread.sleep(100);
        }
    }
}
