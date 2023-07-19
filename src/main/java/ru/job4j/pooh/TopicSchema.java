package ru.job4j.pooh;

import java.util.concurrent.*;

public class TopicSchema implements Schema {
    private final CopyOnWriteArrayList<Receiver> receivers = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<String>> data = new ConcurrentHashMap<>();
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new CopyOnWriteArrayList<String>());
        data.get(message.name()).add(message.text());
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            do {
                for (var receiver : receivers) {
                    var topic = data.get(receiver.name());
                    for (int i = 0; i < topic.size(); i++) {
                        var message = topic.get(i);
                        receiver.receive(message);
                    }
                    condition.off();
                }
            }
            while (condition.check());
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}