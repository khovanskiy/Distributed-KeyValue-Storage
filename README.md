# Алгоритм Viewstamped Replication Revisited

## 3 Обзор
Граф состояний репликаций требует, чтобы репликации запускались в одинаковом начальном состоянии, и чтобы операции были детерминированы. С этими допущениями, легко увидеть, что реплики в конечном итоге будут в одинаковом состоянии, если они выполнили одинаковую последовательность операций. Проблема для протокола репликаций в гарантии того, что операции выполняются в одинаковом порядке на всех репликах несмотря на одновременный запросы от клиентов и несмотря на отказы в работе.

VR использует первичную реплику (лидера) для упорядочения запросов от клиентов; другие реплики - это **резервные копии** (англ. *backups*), которые просто принимают порядок, выбранный лидером. Использование лидера предоставляет простое решение для требования порядка запросов, но это добавляет проблему: что случится, если лидер откажет? VR имеет решает эту проблему, разрешая различным репликам брать на себя роль лидера с течением времени. Система проходит через последовательность представлений (англ. view). В каждой картине одна из реплик выбрана в качестве лидера. Резервные копии проверяют лидера, и если он, по-видимому, неисправен, они выполняют протокол **смены представления** (англ. *view change*) для выбора нового лидера.

Для корректной работы по изменению вида в течение смены представления состояние системы в следующем представлении должно отражать все клиентские операции, которые были выполнены в предыдущих представлениях в предыдущем выбранном порядке. Мы поддерживаем это требование, имея ожидание лидера до тех пор, пока по крайней мере f + 1 реплика (включая самого лидера) не будут знать о запросе клиента до его выполнения, и инициализируя состояние нового представления через опрашивание f + 1 реплики. Таким образом, каждый запрос, известен как, кворум (англ. quorum), и новое представление начинается с кворума.

VR также предоставляет путь для узлов, которые не удалось восстановить, а затем продолжить обработку. Это важно, поскольку в противном случае количество отказов узлов могло бы в конечном счете превысить порог. Корректное восстановление требует, чтобы восстанавливаемая реплика присоединялась к протоколу только после того, как он знает состояние по крайней мере не старее того, когда реплика отказала. Тогда он может правильно реагировать, если это необходимо для кворума.
Очевидно, это требование могло бы быть удовлетворено, если у нас каждая реплика записывала, что она знает на диск перед каждым сообщением. Но мы не требуем использование диск для этой цели.

Итак, VR использует 3 суб-протокола, которые работает вместе и гарантируют корректность:
* **Обычный случай**, обрабатывающий пользовательские сообщения.
* **Изменение представлений** для выбора нового лидера.
* **Восстановление отказавшей реплики**, чтобы она могла заново присоединиться к группе.

Эти суб-протоколы детально описаны в следующем разделе.

## 4 The VR Protocol
Этот раздел описывает как VR работает при допущении, что группа из реплик фиксирована. Мы опишем несколько путей для улучшения производительности протоколов в разделе 5 и оптимизаций в разделе 6. Реконфигурации протокола, который позволяет группе реплик изменяться описана в разделе 7.

Фигура 2 показывает состояние VR слоя реплики. Подлинность лидера не записана в состоянии, но скорее вычисляется из **номера представления** (англ. *view-number*) и конфигурации (англ. configuration). Реплики нумеруются на основе их IP-адресов: реплика с наименьшим IP-адресом - реплика под номером 1. Лидер выбирается циклически, начиная с реплики 1, когда система переходит в новые представления. Статус (англ. status) показывает каком суб-протоколом занимается реплика.

Клиентская сторона также имеет состояние. Оно записывает конфигурацию и текущий номер представления, по его мнению, что позволяет знать какая реплика является в данный момент первичной. Каждое сообщение, посланное клиенту, сообщение ему о текущей номере представления, это позволяет клиенту следить за лидером.

Кроме того клиент записывает собственный номер (англ. client-id) и текущий номер запроса (англ. request-number). Клиенту позволено иметь только один outstanding запрос за раз. Каждому запросу дан номер клиентом и последующие запросы должны иметь больший номер, чем предыдущие; мы опишем как клиенты гарантируют это, если они отказывают и восстанавливаются в разделе 4.5. Номер запроса используется репликами для избежания выполнения запроса больше 1 раза; он также используется клиентом для отбрасывания дублирующихся ответов на его запросы. 

### 4.1 Normal Operation
Этот раздел описывает, как VR работает, когда лидер не отказал. Реплики принимают участие в обработке клиентский запросов только когда их статус *нормальный*. Это ограничение критично для корректности, описанной в разделе 8.



### 4.2 View Changes
### 4.3 Recovery
