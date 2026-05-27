#!/bin/bash

set -e

OS_TYPE="unknown"
BUILD_FLAGS=""

if [ -d "/data/data/com.termux" ] || [[ "$(uname -o)" == *"Android"* ]]; then
    OS_TYPE="termux"
    # Флаг необходим только для Android/Termux при использовании Go 1.23+
    BUILD_FLAGS="-ldflags=-checklinkname=0"
    echo "[INFO] Обнаружена среда Android."

elif [ "$(uname -s)" == "Linux" ]; then
    OS_TYPE="linux"
    echo "[INFO] Обнаружена система Linux."

# 3. Проверка на macOS
elif [ "$(uname -s)" == "Darwin" ]; then
    OS_TYPE="macos"
    echo "[INFO] Обнаружена система macOS."
fi

case "$OS_TYPE" in
    "termux")
        echo "Установка зависимостей через pkg..."
        pkg update -y
        pkg install git golang -y
        ;;
    "linux")
        echo "Установка зависимостей через apt..."
        sudo apt update
        sudo apt install -y git golang
        ;;
    "macos")
        echo "Установка зависимостей через Homebrew..."
        if ! command -v brew &> /dev/null; then
            echo "[ERROR] Homebrew не установлен. Пожалуйста, установите его с https://brew.sh"
            exit 1
        fi
        brew install git go
        ;;
    *)
        echo "[WARNING] Не удалось определить ОС для автоматической установки пакетов."
        echo "Убедитесь, что Git и Go установлены вручную."
        ;;
esac

if [ ! -f "go.mod" ]; then
    echo "Клонирование репозитория..."
    git clone https://github.com/tivorku/concord.git
    cd concord/client
fi

echo "Компиляция проекта..."
if [ -n "$BUILD_FLAGS" ]; then
    echo "Запуск go build с флагами: $BUILD_FLAGS"
    go build "$BUILD_FLAGS" .
else
    echo "Запуск стандартной сборки go build"
    go build .
fi

echo "Сборка завершена."