package com;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        // Параметры
        String inputDir = "C:\\Users\\User\\IdeaProjects\\MapReduce\\input";

        int numWorkers = 4;
        int numReduceTasks = 3;

        // Собираем входные файлы
        File dir = new File(inputDir);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Error: Directory " + inputDir + " does not exist or is not a directory.");
            System.exit(1);
        }

        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            System.err.println("Error: No files found in directory " + inputDir);
            System.exit(1);
        }

        List<String> inputFiles = Arrays.stream(files)
                .filter(File::isFile)
                .map(File::getPath)
                .collect(Collectors.toList());

        // Создаём координатор
        Coordinator coordinator = new Coordinator(inputFiles, numReduceTasks);

        // Запускаем воркеров
        Thread[] workers = new Thread[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            workers[i] = new Thread(new Worker(coordinator)::run);
            workers[i].start();
        }

        // Ждём завершения всех воркеров
        for (Thread worker : workers) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                System.err.println("Error: Worker thread was interrupted while waiting for completion.");
            }
        }

        System.out.println("MapReduce completed.");
    }
}