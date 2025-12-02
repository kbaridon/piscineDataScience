#!/bin/bash

DEST="/home/kbaridon/goinfre"
DB_DIR="$DEST/dbeaver"

URL="https://dbeaver.io/files/dbeaver-ce-latest-linux.gtk.x86_64.tar.gz"

echo "📥 Téléchargement de DBeaver..."
mkdir -p "$DEST"

curl -L "$URL" -o "$DEST/dbeaver.tar.gz"

echo "📦 Décompression..."
rm -rf "$DB_DIR"
tar -xzf "$DEST/dbeaver.tar.gz" -C "$DEST"

if [ ! -d "$DB_DIR" ]; then
    EXTRACTED=$(tar -tzf "$DEST/dbeaver.tar.gz" | head -1 | cut -f1 -d"/")
    mv "$DEST/$EXTRACTED" "$DB_DIR"
fi

rm "$DEST/dbeaver.tar.gz"

echo "🚀 Lancement de DBeaver..."
"$DB_DIR/dbeaver" &

