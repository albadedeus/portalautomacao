#!/bin/bash
# =============================================================
# SETUP AUTOMÁTICO - Automator Portal na GCP (e2-micro)
# Execute como: sudo bash setup_gcp.sh
# =============================================================

set -e

APP_DIR="/home/$SUDO_USER/automator_portal"
APP_USER="$SUDO_USER"

echo "=========================================="
echo " Instalando dependências do sistema..."
echo "=========================================="
apt update
apt install -y python3 python3-pip python3-venv nginx

echo "=========================================="
echo " Configurando ambiente Python..."
echo "=========================================="
cd "$APP_DIR"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install gunicorn
deactivate

echo "=========================================="
echo " Criando pastas necessárias..."
echo "=========================================="
mkdir -p "$APP_DIR/data"
mkdir -p "$APP_DIR/uploads"
mkdir -p "$APP_DIR/output_rv"
mkdir -p "$APP_DIR/output_royalties"
mkdir -p "$APP_DIR/output_conciliacao"
chown -R "$APP_USER:$APP_USER" "$APP_DIR"

echo "=========================================="
echo " Configurando serviço systemd..."
echo "=========================================="
cat > /etc/systemd/system/automator.service << EOF
[Unit]
Description=Automator Portal Flask App
After=network.target

[Service]
User=$APP_USER
WorkingDirectory=$APP_DIR
Environment="PATH=$APP_DIR/venv/bin"
ExecStart=$APP_DIR/venv/bin/gunicorn --workers 2 --bind 127.0.0.1:8080 --timeout 120 app:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable automator
systemctl start automator

echo "=========================================="
echo " Configurando Nginx (proxy reverso)..."
echo "=========================================="
cat > /etc/nginx/sites-available/automator << 'EOF'
server {
    listen 80;
    server_name _;

    client_max_body_size 50M;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 120s;
    }

    location /static/ {
        alias $APP_DIR_PLACEHOLDER/static/;
        expires 1d;
    }
}
EOF

# Substituir placeholder pelo caminho real
sed -i "s|\$APP_DIR_PLACEHOLDER|$APP_DIR|g" /etc/nginx/sites-available/automator

ln -sf /etc/nginx/sites-available/automator /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl restart nginx

echo ""
echo "=========================================="
echo " DEPLOY CONCLUIDO!"
echo "=========================================="
echo " App rodando em: http://$(curl -s ifconfig.me)"
echo " Status: sudo systemctl status automator"
echo " Logs:   sudo journalctl -u automator -f"
echo "=========================================="
