package com;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Coordinator {
    private final List<String> inputFiles;
    private final int numReduceTasks;
    private final ConcurrentLinkedQueue<MapTask> mapTasks;
    private final ConcurrentLinkedQueue<ReduceTask> reduceTasks;
    private final ConcurrentMap<Integer, List<String>> intermediateFiles;
    private final AtomicInteger completedMapTasks;
    private final AtomicInteger completedReduceTasks;
    private volatile boolean isDone;

    public Coordinator(List<String> inputFiles, int numReduceTasks) {
        this.inputFiles = inputFiles;
        this.numReduceTasks = numReduceTasks;
        this.mapTasks = new ConcurrentLinkedQueue<>();
        this.reduceTasks = new ConcurrentLinkedQueue<>();
        this.intermediateFiles = new ConcurrentHashMap<>();
        this.completedMapTasks = new AtomicInteger(0);
        this.completedReduceTasks = new AtomicInteger(0);
        this.isDone = false;

        // Инициализация map задач
        for (int i = 0; i < inputFiles.size(); i++) {
            mapTasks.add(new MapTask(i, inputFiles.get(i), numReduceTasks));
        }
    }

    // Воркер запрашивает задачу
    public synchronized Task getTask(long timeoutMillis) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            // Проверяем, есть ли map задачи
            MapTask mapTask = mapTasks.poll();
            if (mapTask != null) {
                return mapTask;
            }

            // Если map задачи закончились, проверяем reduce задачи
            if (completedMapTasks.get() == inputFiles.size() && !reduceTasks.isEmpty()) {
                ReduceTask reduceTask = reduceTasks.poll();
                if (reduceTask != null) {
                    return reduceTask;
                }
            }

            // Если всё выполнено
            if (completedMapTasks.get() == inputFiles.size() && completedReduceTasks.get() == numReduceTasks) {
                isDone = true;
                return null;
            }

            // Ждём немного перед следующей попыткой
            Thread.sleep(100);
        }

        return null; // Timeout
    }

    // Воркер сообщает о завершении map задачи
    public synchronized void completeMapTask(int taskId, List<String> intermediate) {
        intermediateFiles.put(taskId, intermediate);
        if (completedMapTasks.incrementAndGet() == inputFiles.size()) {
            // Все map задачи завершены, создаём reduce задачи
            for (int i = 0; i < numReduceTasks; i++) {
                List<String> files = new ArrayList<>();
                for (int j = 0; j < inputFiles.size(); j++) {
                    files.add("mr-" + j + "-" + i);
                }
                reduceTasks.add(new ReduceTask(i, files));
            }
        }
    }

    // Воркер сообщает о завершении reduce задачи
    public synchronized void completeReduceTask(int taskId) {
        completedReduceTasks.incrementAndGet();
    }

    public boolean isDone() {
        return isDone;
    }
}

// Класс для map задачи
class MapTask extends Task {
    private final String fileName;
    private final int numReduceTasks;

    public MapTask(int taskId, String fileName, int numReduceTasks) {
        super(taskId);
        this.fileName = fileName;
        this.numReduceTasks = numReduceTasks;
    }

    public String getFileName() {
        return fileName;
    }

    public int getNumReduceTasks() {
        return numReduceTasks;
    }
}

// Класс для reduce задачи
class ReduceTask extends Task {
    private final List<String> intermediateFiles;

    public ReduceTask(int taskId, List<String> intermediateFiles) {
        super(taskId);
        this.intermediateFiles = intermediateFiles;
    }

    public List<String> getIntermediateFiles() {
        return intermediateFiles;
    }
}

// Абстрактный класс задачи
abstract class Task {
    private final int taskId;

    public Task(int taskId) {
        this.taskId = taskId;
    }

    public int getTaskId() {
        return taskId;
    }
}