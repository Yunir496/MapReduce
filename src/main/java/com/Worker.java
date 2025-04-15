package com;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class Worker {
    private final Coordinator coordinator;
    private final long timeoutMillis = 5000;

    public Worker(Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    public void run() {
        try {
            while (!coordinator.isDone()) {
                Task task = coordinator.getTask(timeoutMillis);
                if (task == null) {
                    if (coordinator.isDone()) {
                        break;
                    }
                    continue;
                }

                if (task instanceof MapTask) {
                    processMapTask((MapTask) task);
                } else if (task instanceof ReduceTask) {
                    processReduceTask((ReduceTask) task);
                }
            }
        } catch (Exception e) {
            System.err.println("Error: Worker failed to process task: " + e.getMessage());
        }
    }

    private void processMapTask(MapTask task) throws IOException {
        // Читаем содержимое файла
        String content = Files.readString(new File(task.getFileName()).toPath());

        // Выполняем map функцию
        List<KeyValue> keyValues = map(task.getFileName(), content);

        // Разделяем по бакетам (reduce задачам)
        List<List<KeyValue>> buckets = new ArrayList<>();
        for (int i = 0; i < task.getNumReduceTasks(); i++) {
            buckets.add(new ArrayList<>());
        }

        for (KeyValue kv : keyValues) {
            int bucket = Math.abs(kv.getKey().hashCode() % task.getNumReduceTasks());
            buckets.get(bucket).add(kv);
        }

        // Записываем промежуточные файлы
        List<String> intermediateFiles = new ArrayList<>();
        for (int i = 0; i < task.getNumReduceTasks(); i++) {
            String fileName = "mr-" + task.getTaskId() + "-" + i;
            intermediateFiles.add(fileName);
            try (BufferedWriter writer = Files.newBufferedWriter(new File(fileName).toPath())) {
                for (KeyValue kv : buckets.get(i)) {
                    writer.write(kv.getKey() + "\t" + kv.getValue() + "\n");
                }
            }
        }

        // Сообщаем координатору о завершении
        coordinator.completeMapTask(task.getTaskId(), intermediateFiles);
    }

    private void processReduceTask(ReduceTask task) throws IOException {
        // Читаем все промежуточные файлы
        List<KeyValue> keyValues = new ArrayList<>();
        for (String fileName : task.getIntermediateFiles()) {
            try (BufferedReader reader = Files.newBufferedReader(new File(fileName).toPath())) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        keyValues.add(new KeyValue(parts[0], parts[1]));
                    }
                }
            }
        }

        // Сортируем по ключу
        keyValues.sort(Comparator.comparing(KeyValue::getKey));

        // Группируем по ключу и выполняем reduce
        String outputFile = "output-" + task.getTaskId();
        try (BufferedWriter writer = Files.newBufferedWriter(new File(outputFile).toPath())) {
            String currentKey = null;
            List<String> values = new ArrayList<>();

            for (KeyValue kv : keyValues) {
                if (!kv.getKey().equals(currentKey)) {
                    if (currentKey != null) {
                        String result = reduce(currentKey, values);
                        writer.write(currentKey + "\t" + result + "\n");
                    }
                    currentKey = kv.getKey();
                    values.clear();
                }
                values.add(kv.getValue());
            }

            // Последняя группа
            if (currentKey != null) {
                String result = reduce(currentKey, values);
                writer.write(currentKey + "\t" + result + "\n");
            }
        }

        // Сообщаем координатору о завершении
        coordinator.completeReduceTask(task.getTaskId());
    }

    private List<KeyValue> map(String fileName, String content) {
        // Разделяем на слова и создаём KeyValue
        String[] words = content.toLowerCase().split("\\W+");
        return Arrays.stream(words)
                .filter(word -> !word.isEmpty())
                .map(word -> new KeyValue(word, "1"))
                .collect(Collectors.toList());
    }

    private String reduce(String key, List<String> values) {
        // Суммируем значения
        int sum = values.stream().mapToInt(Integer::parseInt).sum();
        return String.valueOf(sum);
    }
}
