package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	// Точка "." означает текущую директорию, где запущен скрипт
	root := "." 
	totalLines := 0
	fileCount := 0

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Проверяем, что это файл и он имеет расширение .go
		if !d.IsDir() && strings.HasSuffix(strings.ToLower(d.Name()), ".go") {
			lines, err := countLines(path)
			if err != nil {
				fmt.Printf("Ошибка чтения файла %s: %v\n", path, err)
				return nil // Продолжаем обход остальных файлов
			}
			totalLines += lines
			fileCount++
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Ошибка при обходе директорий: %v\n", err)
		return
	}

	fmt.Printf("Найдено файлов: %d\n", fileCount)
	fmt.Printf("Общее количество строк: %d\n", totalLines)
}

// Функция для подсчета строк в конкретном файле
func countLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lines := 0
	for scanner.Scan() {
		lines++
	}

	return lines, scanner.Err()
}
