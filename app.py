#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PROCESSADOR RV E DSR - BACKEND WEB
Sistema com autenticação e gerenciamento de usuários/acionistas
"""

from flask import Flask, render_template, request, jsonify, send_file, send_from_directory, redirect, url_for, session
from functools import wraps
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
import pandas as pd
import os
import json
import hashlib
import zipfile
import shutil
from datetime import datetime
from pathlib import Path
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'output_rv'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max
app.secret_key = 'totvs-nordeste-conciliacao-2024-secret-key'

# Cria pastas se não existirem
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

# ==================================================================================
# ARQUIVOS DE DADOS
# ==================================================================================

USUARIOS_FILE = 'data/usuarios.json'
ACIONISTAS_FILE = 'data/acionistas.json'
ROYALTIES_CONFIG_FILE = 'data/royalties_config.json'
ROYALTIES_OUTPUT_FOLDER = 'output_royalties'
CONCILIACAO_OUTPUT_FOLDER = 'output_conciliacao'

os.makedirs('data', exist_ok=True)
os.makedirs(ROYALTIES_OUTPUT_FOLDER, exist_ok=True)
os.makedirs(CONCILIACAO_OUTPUT_FOLDER, exist_ok=True)

# Import do motor de conciliação bancária x contábil
try:
    from conciliacao_bancaria_contabil import processar_conciliacao
except ImportError:
    processar_conciliacao = None

# Import do motor de conciliação cliente (NFs x Recebimentos)
try:
    from conciliacao_bancaria_cliente import processar_conciliacao_cliente
except ImportError:
    processar_conciliacao_cliente = None

def hash_password(password):
    """Hash simples para senhas"""
    return hashlib.sha256(password.encode()).hexdigest()

def load_usuarios():
    """Carrega usuários do arquivo JSON"""
    if os.path.exists(USUARIOS_FILE):
        with open(USUARIOS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    # Usuário admin padrão
    default_users = {
        'admin': {
            'name': 'Administrador',
            'password': hash_password('admin123'),
            'is_admin': True,
            'active': True
        }
    }
    save_usuarios(default_users)
    return default_users

def save_usuarios(usuarios):
    """Salva usuários no arquivo JSON"""
    with open(USUARIOS_FILE, 'w', encoding='utf-8') as f:
        json.dump(usuarios, f, ensure_ascii=False, indent=2)

def load_acionistas():
    """Carrega acionistas do arquivo JSON"""
    if os.path.exists(ACIONISTAS_FILE):
        with open(ACIONISTAS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    # Acionistas padrão
    default_acionistas = {
        "Francisco Ferreira": {"active": True},
        "Marcos Saraiva": {"active": True},
        "Julio Bernadotte": {"active": True},
        "Luciana Colares": {"active": True},
        "Marcos Guimaraes": {"active": True},
        "Maury Neto": {"active": True},
        "Paulo Morais": {"active": True},
        "Romulo Barroso": {"active": True},
        "Simone Guimaraes": {"active": True},
    }
    save_acionistas(default_acionistas)
    return default_acionistas

def save_acionistas(acionistas):
    """Salva acionistas no arquivo JSON"""
    with open(ACIONISTAS_FILE, 'w', encoding='utf-8') as f:
        json.dump(acionistas, f, ensure_ascii=False, indent=2)


def load_royalties_config():
    """Carrega configuração de royalties do arquivo JSON"""
    default_config = {
        "produtos_nao_royalties": [
            "PS01009", "PS01010", "PS02001", "PS02002", "PS02003",
            "PS02004", "PS02010", "PS02020", "PS02021", "PS02022",
            "PS02023", "PS02030", "PS02031", "PS03001"
        ],
        "clientes_nao_royalties": [
            "DEBITO", "A00001", "A00002", "A000AL", "A00158", "A84063",
            "AAA002", "AAA003", "AAA004", "AAA005", "AAA006", "AAA007",
            "AAA008", "T82665", "TEZBY4", "TFDBWH", "TFCNN8", "TFCPGR",
            "TFEBT1", "TFEFYX", "X000LI", "X00111", "X00112", "X0004I",
            "X0010W", "X0012G", "X000LA", "X000EE", "TFDVD4", "TFDHQX",
            "TEZGHH", "A000BY"
        ]
    }

    if os.path.exists(ROYALTIES_CONFIG_FILE):
        try:
            with open(ROYALTIES_CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            save_royalties_config(default_config)
            return default_config
    else:
        save_royalties_config(default_config)
        return default_config


def save_royalties_config(config):
    """Salva configuração de royalties no arquivo JSON"""
    with open(ROYALTIES_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=4)

# ==================================================================================
# DECORADORES DE AUTENTICAÇÃO
# ==================================================================================

def login_required(f):
    """Decorator para rotas que requerem login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """Decorator para rotas que requerem admin"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        if not session.get('is_admin'):
            return jsonify({'error': 'Acesso negado'}), 403
        return f(*args, **kwargs)
    return decorated_function

# ==================================================================================
# MAPEAMENTO DE ACIONISTAS (agora dinâmico via JSON)
# ==================================================================================

def get_acionistas_set():
    """Retorna set de acionistas ativos"""
    acionistas = load_acionistas()
    return {nome for nome, data in acionistas.items() if data.get('active', True)}

# ==================================================================================
# FUNÇÕES DE PROCESSAMENTO
# ==================================================================================

def eh_acionista(nome):
    """Verifica se o funcionário é ACIONISTA"""
    acionistas = get_acionistas_set()
    if nome in acionistas:
        return True

    nome_limpo = nome.upper().strip()
    for acionista in acionistas:
        if acionista.upper() == nome_limpo:
            return True

    return False

def pegar_valor(valor):
    """Converte valor para float"""
    if not valor or valor == '-':
        return 0.0
    
    if isinstance(valor, (int, float)):
        return float(valor)
    
    valor = str(valor).replace('R$', '').replace(' ', '').strip()
    
    if ',' in valor and '.' in valor:
        valor = valor.replace('.', '').replace(',', '.')
    elif ',' in valor:
        valor = valor.replace(',', '.')
    
    try:
        return float(valor)
    except:
        return 0.0

def formatar(valor):
    """Formata valor com 18 caracteres: 15 dígitos + ponto + 2 centavos"""
    valor_float = pegar_valor(valor)
    centavos = int(round(valor_float * 100))
    parte_inteira = centavos // 100
    parte_decimal = centavos % 100
    return f"{parte_inteira:015d}.{parte_decimal:02d}"

def processar_arquivo_rv_dsr(arquivo_path):
    """Processa o arquivo XLSX e retorna estatísticas"""
    
    # Abre arquivo com data_only=True para obter valores calculados
    wb = openpyxl.load_workbook(arquivo_path, data_only=True)

    # Tenta abrir a aba "MODELO PARA AUTOMAÇÃO"
    aba_nome = "MODELO PARA AUTOMAÇÃO"
    if aba_nome not in wb.sheetnames:
        for nome in wb.sheetnames:
            if "MODELO" in nome.upper() and "AUTOMA" in nome.upper():
                aba_nome = nome
                break
        else:
            raise Exception(f"Aba 'MODELO PARA AUTOMAÇÃO' não encontrada!\nAbas disponíveis: {', '.join(wb.sheetnames)}")

    ws = wb[aba_nome]

    # Processa linhas
    dados_por_filial = {}
    total = 0
    acionistas_count = 0
    clt_count = 0
    ignorados_filial = 0
    ignorados_valor = 0

    log_processados = []
    log_ignorados = []

    for i in range(2, ws.max_row + 1):
        executivo = ws.cell(i, 3).value  # Coluna C
        rv_raw = ws.cell(i, 8).value     # Coluna H
        dsr_raw = ws.cell(i, 9).value    # Coluna I
        filial = ws.cell(i, 12).value    # Coluna L
        matricula = ws.cell(i, 13).value # Coluna M

        if not executivo:
            continue

        executivo = str(executivo).strip()

        # Valida filial/matrícula
        if not filial or not matricula or str(filial).upper() == "VAZIO" or str(matricula).upper() == "VAZIO":
            log_ignorados.append({
                "executivo": executivo,
                "motivo": "Sem filial/matrícula",
                "rv": rv_raw,
                "dsr": dsr_raw
            })
            ignorados_filial += 1
            continue

        filial = str(filial).strip().zfill(6)         # filial sempre 6
        matricula = str(matricula).strip().zfill(6)   # matrícula sempre 6 (padrão layout 2)

        rv_valor = pegar_valor(rv_raw)
        dsr_valor = pegar_valor(dsr_raw)

        # Valida valores
        if rv_valor == 0 and dsr_valor == 0:
            log_ignorados.append({
                "executivo": executivo,
                "motivo": "RV e DSR zerados/vazios",
                "rv": rv_raw,
                "dsr": dsr_raw
            })
            ignorados_valor += 1
            continue

        is_acionista = eh_acionista(executivo)

        if is_acionista:
            cod_rv = "390"
            cod_dsr = "391"
            tipo = "ACIONISTA"
            acionistas_count += 1
        else:
            cod_rv = "392"
            cod_dsr = "393"
            tipo = "CLT"
            clt_count += 1

        rv_fmt = formatar(rv_valor)
        dsr_fmt = formatar(dsr_valor)
        identificador = filial + matricula            # identificador sempre 12 chars
        espacos = 6                                   # espaçamento fixo (padrão layout 2)

        linha_rv = f"{identificador}{' ' * espacos}{cod_rv}{rv_fmt}"
        linha_dsr = f"{identificador}{' ' * espacos}{cod_dsr}{dsr_fmt}"

        if filial not in dados_por_filial:
            dados_por_filial[filial] = []

        dados_por_filial[filial].append(linha_rv)
        dados_por_filial[filial].append(linha_dsr)

        log_processados.append({
            "executivo": executivo,
            "tipo": tipo,
            "rv": rv_valor,
            "dsr": dsr_valor,
            "filial": filial,
            "matricula": matricula
        })

        total += 1

    # Gera arquivos TXT
    pasta = app.config['OUTPUT_FOLDER']
    arquivos_gerados = []

    for filial, linhas in dados_por_filial.items():
        arquivo = os.path.join(pasta, f"filial_{filial}.txt")
        with open(arquivo, 'w', encoding='utf-8') as f:
            for linha in linhas:
                f.write(linha + '\n')
        arquivos_gerados.append(f"filial_{filial}.txt")

    # Gera log detalhado
    data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo_log = os.path.join(pasta, f"log_processamento_{data_hora}.txt")

    with open(arquivo_log, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("LOG DE PROCESSAMENTO RV/DSR\n")
        f.write(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
        f.write("="*80 + "\n\n")

        f.write("-"*80 + "\n")
        f.write(f"FUNCIONÁRIOS PROCESSADOS ({total})\n")
        f.write("-"*80 + "\n")
        for item in log_processados:
            f.write(f"  {item['executivo']:30} | {item['tipo']:10} | "
                   f"Filial: {item['filial']} | Mat: {item['matricula']} | "
                   f"RV: R$ {item['rv']:>10.2f} | DSR: R$ {item['dsr']:>10.2f}\n")
        f.write("\n")

        total_ignorados = ignorados_filial + ignorados_valor
        f.write("-"*80 + "\n")
        f.write(f"FUNCIONÁRIOS IGNORADOS ({total_ignorados})\n")
        f.write("-"*80 + "\n")
        if log_ignorados:
            for item in log_ignorados:
                f.write(f"  {item['executivo']:30} | Motivo: {item['motivo']}\n")
        else:
            f.write("  Nenhum funcionário ignorado.\n")
        f.write("\n")

        f.write("="*80 + "\n")
        f.write("RESUMO\n")
        f.write("="*80 + "\n")
        f.write(f"  Total processados: {total}\n")
        f.write(f"  - ACIONISTAS (390/391): {acionistas_count}\n")
        f.write(f"  - CLT (392/393): {clt_count}\n")
        f.write(f"  Total ignorados: {total_ignorados}\n")
        f.write(f"  Arquivos gerados: {len(dados_por_filial)}\n")
        f.write("="*80 + "\n")

    arquivos_gerados.append(f"log_processamento_{data_hora}.txt")

    return {
        'total': total,
        'acionistas': acionistas_count,
        'clt': clt_count,
        'ignorados': ignorados_filial + ignorados_valor,
        'ignorados_filial': ignorados_filial,
        'ignorados_valor': ignorados_valor,
        'filiais': len(dados_por_filial),
        'arquivos': arquivos_gerados,
        'log_arquivo': f"log_processamento_{data_hora}.txt"
    }

# ==================================================================================
# ROTAS DE AUTENTICAÇÃO
# ==================================================================================

@app.route('/')
def index():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/assets/<path:filename>')
def serve_assets(filename):
    return send_from_directory('assets', filename)

@app.route('/login')
def login():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return render_template('login.html')

@app.route('/auth/login', methods=['POST'])
def auth_login():
    data = request.get_json()
    username = data.get('username', '').strip()
    password = data.get('password', '')

    usuarios = load_usuarios()

    if username not in usuarios:
        return jsonify({'success': False, 'error': 'Usuário não encontrado'})

    user = usuarios[username]

    if not user.get('active', True):
        return jsonify({'success': False, 'error': 'Usuário inativo'})

    if user['password'] != hash_password(password):
        return jsonify({'success': False, 'error': 'Senha incorreta'})

    session['user'] = username
    session['name'] = user.get('name', username)
    session['is_admin'] = user.get('is_admin', False)

    return jsonify({'success': True, 'redirect': '/dashboard'})

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html', user={
        'username': session.get('user'),
        'name': session.get('name'),
        'is_admin': session.get('is_admin')
    })

@app.route('/rv-dsr')
@login_required
def rv_dsr():
    return render_template('rv_dsr.html', user={
        'username': session.get('user'),
        'name': session.get('name'),
        'is_admin': session.get('is_admin')
    })


@app.route('/royalties')
@login_required
def royalties():
    return render_template('royalties.html', user={
        'username': session.get('user'),
        'name': session.get('name'),
        'is_admin': session.get('is_admin')
    })

# ==================================================================================
# ROTAS DE ADMIN - USUÁRIOS
# ==================================================================================

@app.route('/admin/usuarios')
@admin_required
def admin_usuarios():
    return render_template('usuarios.html', user={
        'username': session.get('user'),
        'name': session.get('name'),
        'is_admin': session.get('is_admin')
    })

@app.route('/api/usuarios', methods=['GET'])
@admin_required
def api_get_usuarios():
    usuarios = load_usuarios()
    # Remove senhas da resposta
    safe_usuarios = {}
    for username, data in usuarios.items():
        safe_usuarios[username] = {k: v for k, v in data.items() if k != 'password'}
    return jsonify(safe_usuarios)

@app.route('/api/usuarios', methods=['POST'])
@admin_required
def api_create_usuario():
    data = request.get_json()
    username = data.get('username', '').strip().lower()
    name = data.get('name', '').strip()
    password = data.get('password', '') or 'mudar123'
    is_admin = data.get('is_admin', False)
    active = data.get('active', True)

    if not username:
        return jsonify({'success': False, 'error': 'Username é obrigatório'})

    usuarios = load_usuarios()

    if username in usuarios:
        return jsonify({'success': False, 'error': 'Usuário já existe'})

    usuarios[username] = {
        'name': name,
        'password': hash_password(password),
        'is_admin': is_admin,
        'active': active
    }

    save_usuarios(usuarios)
    return jsonify({'success': True, 'message': 'Usuário criado com sucesso!'})

@app.route('/api/usuarios/<username>', methods=['PUT'])
@admin_required
def api_update_usuario(username):
    data = request.get_json()
    usuarios = load_usuarios()

    if username not in usuarios:
        return jsonify({'success': False, 'error': 'Usuário não encontrado'})

    if 'name' in data:
        usuarios[username]['name'] = data['name']
    if 'is_admin' in data:
        usuarios[username]['is_admin'] = data['is_admin']
    if 'active' in data:
        usuarios[username]['active'] = data['active']
    if data.get('password'):
        usuarios[username]['password'] = hash_password(data['password'])

    save_usuarios(usuarios)
    return jsonify({'success': True, 'message': 'Usuário atualizado!'})

@app.route('/api/usuarios/<username>', methods=['DELETE'])
@admin_required
def api_delete_usuario(username):
    if username == 'admin':
        return jsonify({'success': False, 'error': 'Não é possível excluir o admin'})

    usuarios = load_usuarios()

    if username not in usuarios:
        return jsonify({'success': False, 'error': 'Usuário não encontrado'})

    del usuarios[username]
    save_usuarios(usuarios)
    return jsonify({'success': True, 'message': 'Usuário excluído!'})

# ==================================================================================
# ROTAS DE ADMIN - ACIONISTAS
# ==================================================================================

@app.route('/admin/acionistas')
@admin_required
def admin_acionistas():
    return render_template('acionistas.html', user={
        'username': session.get('user'),
        'name': session.get('name'),
        'is_admin': session.get('is_admin')
    })

@app.route('/api/acionistas', methods=['GET'])
@login_required
def api_get_acionistas():
    acionistas = load_acionistas()
    return jsonify(acionistas)

@app.route('/api/acionistas', methods=['POST'])
@admin_required
def api_create_acionista():
    data = request.get_json()
    nome = data.get('nome', '').strip()
    active = data.get('active', True)

    if not nome:
        return jsonify({'success': False, 'error': 'Nome é obrigatório'})

    acionistas = load_acionistas()

    if nome in acionistas:
        return jsonify({'success': False, 'error': 'Acionista já existe'})

    acionistas[nome] = {'active': active}
    save_acionistas(acionistas)
    return jsonify({'success': True, 'message': 'Acionista cadastrado!'})

@app.route('/api/acionistas/<nome>', methods=['PUT'])
@admin_required
def api_update_acionista(nome):
    data = request.get_json()
    acionistas = load_acionistas()

    if nome not in acionistas:
        return jsonify({'success': False, 'error': 'Acionista não encontrado'})

    new_nome = data.get('nome', nome).strip()
    active = data.get('active', True)

    if new_nome != nome:
        # Renomear acionista
        del acionistas[nome]
        acionistas[new_nome] = {'active': active}
    else:
        acionistas[nome]['active'] = active

    save_acionistas(acionistas)
    return jsonify({'success': True, 'message': 'Acionista atualizado!'})

@app.route('/api/acionistas/<nome>', methods=['DELETE'])
@admin_required
def api_delete_acionista(nome):
    acionistas = load_acionistas()

    if nome not in acionistas:
        return jsonify({'success': False, 'error': 'Acionista não encontrado'})

    del acionistas[nome]
    save_acionistas(acionistas)
    return jsonify({'success': True, 'message': 'Acionista excluído!'})

# ==================================================================================
# ROTAS DE PROCESSAMENTO
# ==================================================================================

@app.route('/processar', methods=['POST'])
@login_required
def processar():
    try:
        if 'arquivo' not in request.files:
            return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'}), 400

        arquivo = request.files['arquivo']
        
        if arquivo.filename == '':
            return jsonify({'success': False, 'error': 'Nenhum arquivo selecionado'}), 400

        if not arquivo.filename.endswith('.xlsx'):
            return jsonify({'success': False, 'error': 'Apenas arquivos .xlsx são permitidos'}), 400

        # Salva arquivo
        filename = secure_filename(arquivo.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        arquivo.save(filepath)

        # Processa
        resultado = processar_arquivo_rv_dsr(filepath)

        # Remove arquivo temporário
        os.remove(filepath)

        return jsonify({
            'success': True,
            **resultado
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/download/<filename>')
@login_required
def download(filename):
    try:
        filepath = os.path.join(app.config['OUTPUT_FOLDER'], filename)
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 404

@app.route('/download-zip', methods=['POST'])
@login_required
def download_zip():
    """Cria e baixa um ZIP com todos os arquivos gerados"""
    try:
        data = request.get_json()
        arquivos = data.get('arquivos', [])

        if not arquivos:
            return jsonify({'error': 'Nenhum arquivo para compactar'}), 400

        # Nome da pasta: data_rv_dsr (ex: 20260120_rv_dsr)
        data_hoje = datetime.now().strftime("%Y%m%d")
        nome_pasta = f"{data_hoje}_rv_dsr"
        nome_zip = f"{nome_pasta}.zip"

        # Caminho do ZIP
        zip_path = os.path.join(app.config['OUTPUT_FOLDER'], nome_zip)

        # Cria o ZIP
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for arquivo in arquivos:
                arquivo_path = os.path.join(app.config['OUTPUT_FOLDER'], arquivo)
                if os.path.exists(arquivo_path):
                    # Coloca o arquivo dentro de uma pasta no ZIP
                    zf.write(arquivo_path, os.path.join(nome_pasta, arquivo))

        return send_file(zip_path, as_attachment=True, download_name=nome_zip)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ==================================================================================
# ROTAS DE ROYALTIES
# ==================================================================================

@app.route('/api/royalties/config', methods=['GET'])
@login_required
def api_get_royalties_config():
    config = load_royalties_config()
    return jsonify(config)


@app.route('/api/royalties/produtos', methods=['POST'])
@login_required
def api_add_royalties_produto():
    data = request.get_json()
    produto = data.get('produto', '').strip().upper()

    if not produto:
        return jsonify({'success': False, 'error': 'Produto é obrigatório'})

    config = load_royalties_config()

    if produto in config['produtos_nao_royalties']:
        return jsonify({'success': False, 'error': 'Produto já existe na lista'})

    config['produtos_nao_royalties'].append(produto)
    save_royalties_config(config)
    return jsonify({'success': True, 'message': 'Produto adicionado!'})


@app.route('/api/royalties/produtos/<produto>', methods=['DELETE'])
@login_required
def api_delete_royalties_produto(produto):
    config = load_royalties_config()

    if produto not in config['produtos_nao_royalties']:
        return jsonify({'success': False, 'error': 'Produto não encontrado'})

    config['produtos_nao_royalties'].remove(produto)
    save_royalties_config(config)
    return jsonify({'success': True, 'message': 'Produto removido!'})


@app.route('/api/royalties/clientes', methods=['POST'])
@login_required
def api_add_royalties_cliente():
    data = request.get_json()
    cliente = data.get('cliente', '').strip().upper()

    if not cliente:
        return jsonify({'success': False, 'error': 'Cliente é obrigatório'})

    config = load_royalties_config()

    if cliente in config['clientes_nao_royalties']:
        return jsonify({'success': False, 'error': 'Cliente já existe na lista'})

    config['clientes_nao_royalties'].append(cliente)
    save_royalties_config(config)
    return jsonify({'success': True, 'message': 'Cliente adicionado!'})


@app.route('/api/royalties/clientes/<cliente>', methods=['DELETE'])
@login_required
def api_delete_royalties_cliente(cliente):
    config = load_royalties_config()

    if cliente not in config['clientes_nao_royalties']:
        return jsonify({'success': False, 'error': 'Cliente não encontrado'})

    config['clientes_nao_royalties'].remove(cliente)
    save_royalties_config(config)
    return jsonify({'success': True, 'message': 'Cliente removido!'})


def to_number_ptbr(value):
    """Converte valor brasileiro para float"""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    s = s.replace('\u00a0', '').replace(' ', '')
    s = s.replace('.', '').replace(',', '.')

    try:
        return float(s)
    except:
        return 0.0


def to_number_ptbr_series(series):
    """Converte série pandas com valores brasileiros para numérico"""
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0)
    s = series.astype(str).str.strip()
    s = s.str.replace('\u00a0', '', regex=False)
    s = s.str.replace(' ', '', regex=False)
    s = s.str.replace('.', '', regex=False)
    s = s.str.replace(',', '.', regex=False)
    return pd.to_numeric(s, errors='coerce').fillna(0)


def escolher_coluna_valor(df, palavras_chave):
    """Escolhe a melhor coluna de valor no DataFrame"""
    candidatos = []
    for col in df.columns:
        low = str(col).lower()
        if any(p in low for p in palavras_chave):
            candidatos.append(col)

    for col in candidatos:
        nums = to_number_ptbr_series(df[col])
        if nums.abs().sum() > 0:
            return col

    melhor = None
    melhor_score = -1.0
    for col in df.columns:
        nums = to_number_ptbr_series(df[col])
        score = float(nums.abs().sum())
        if score > melhor_score:
            melhor_score = score
            melhor = col

    return melhor if melhor_score > 0 else None


def escolher_campos_linhas(df):
    """Escolhe as melhores colunas para agrupar"""
    cols = df.columns.tolist()
    escolhidos = []

    for col in cols:
        low = str(col).lower()
        if ('cliente' in low) or ('cod' in low) or ('cód' in low) or ('codigo' in low):
            escolhidos.append(col)

    for col in cols:
        low = str(col).lower()
        if ('nome' in low) and ('cliente' not in low):
            escolhidos.append(col)

    seen = set()
    escolhidos = [c for c in escolhidos if not (c in seen or seen.add(c))]

    if len(escolhidos) >= 1:
        return escolhidos

    return cols[:2] if len(cols) >= 2 else cols[:1]


def criar_validacao_sim_nao(df, campos_linhas, campo_valor):
    """Cria tabela de validação agrupando por SIM/NÃO"""
    temp = df.copy()

    if 'Royalties' not in temp.columns:
        raise ValueError("Coluna 'Royalties' não existe no DataFrame.")
    if campo_valor is None or campo_valor not in temp.columns:
        raise ValueError("Não foi possível identificar a coluna de valor.")
    for c in campos_linhas:
        if c not in temp.columns:
            raise ValueError(f"Coluna '{c}' não existe no DataFrame.")

    temp['Royalties'] = temp['Royalties'].astype(str).str.strip().str.upper()
    temp['Royalties'] = temp['Royalties'].replace({'NAO': 'NÃO'})
    temp.loc[~temp['Royalties'].isin(['SIM', 'NÃO']), 'Royalties'] = 'SIM'

    temp[campo_valor] = to_number_ptbr_series(temp[campo_valor])

    agg = (
        temp.groupby(campos_linhas + ['Royalties'], dropna=False)[campo_valor]
        .sum()
        .unstack('Royalties', fill_value=0)
        .reset_index()
    )

    if 'SIM' not in agg.columns:
        agg['SIM'] = 0
    if 'NÃO' not in agg.columns:
        agg['NÃO'] = 0

    agg = agg[campos_linhas + ['NÃO', 'SIM']]
    agg['Total'] = agg['NÃO'] + agg['SIM']

    total_row = {c: 'Total Geral' for c in campos_linhas}
    total_row['NÃO'] = float(agg['NÃO'].sum())
    total_row['SIM'] = float(agg['SIM'].sum())
    total_row['Total'] = float(agg['Total'].sum())

    agg = pd.concat([agg, pd.DataFrame([total_row])], ignore_index=True)
    return agg


def processar_royalties(arquivo_path):
    """Processa arquivo Excel de royalties"""
    config = load_royalties_config()
    produtos_nao = set(p.strip().upper() for p in config['produtos_nao_royalties'])
    clientes_nao = set(c.strip().upper() for c in config['clientes_nao_royalties'])

    # Carrega arquivo
    wb_original = openpyxl.load_workbook(arquivo_path)
    xls = pd.ExcelFile(arquivo_path)
    todas_abas = {aba: pd.read_excel(arquivo_path, sheet_name=aba) for aba in xls.sheet_names}

    resultado = {
        'fat_sim': 0,
        'fat_nao': 0,
        'fat_total': 0,
        'baixas_sim': 0,
        'baixas_nao': 0,
        'baixas_total': 0
    }

    # Processa Detalhado NF
    if "Detalhado NF" in todas_abas:
        df_nf = todas_abas["Detalhado NF"].copy()

        # Adiciona coluna Royalties baseado na coluna H (índice 7)
        df_nf["Royalties"] = df_nf.iloc[:, 7].apply(
            lambda x: "NÃO" if str(x).strip().upper() in produtos_nao else "SIM"
        )
        todas_abas["Detalhado NF"] = df_nf

        # Calcula totais para validação
        valor_col = None
        for col in df_nf.columns:
            if any(p in str(col).lower() for p in ['total', 'valor']):
                valor_col = col
                break

        if valor_col:
            df_nf['_valor_num'] = df_nf[valor_col].apply(to_number_ptbr)
            resultado['fat_sim'] = float(df_nf[df_nf['Royalties'] == 'SIM']['_valor_num'].sum())
            resultado['fat_nao'] = float(df_nf[df_nf['Royalties'] == 'NÃO']['_valor_num'].sum())
            resultado['fat_total'] = resultado['fat_sim'] + resultado['fat_nao']

        # Gera VALIDAÇÃO FAT
        try:
            campos = escolher_campos_linhas(df_nf)
            valor = escolher_coluna_valor(df_nf, ['total', 'valor'])
            todas_abas['VALIDAÇÃO FAT'] = criar_validacao_sim_nao(df_nf, campos, valor)
        except Exception as e:
            todas_abas['VALIDAÇÃO FAT'] = pd.DataFrame([{'Erro': 'Falha ao criar VALIDAÇÃO FAT', 'Motivo': str(e)}])

    # Processa Detalhado Baixas
    if "Detalhado Baixas" in todas_abas:
        df_baixas = todas_abas["Detalhado Baixas"].copy()

        # Adiciona coluna Royalties baseado na coluna B (índice 1)
        df_baixas["Royalties"] = df_baixas.iloc[:, 1].apply(
            lambda x: "NÃO" if str(x).strip().upper() in clientes_nao else "SIM"
        )
        todas_abas["Detalhado Baixas"] = df_baixas

        # Calcula totais para validação
        valor_col = None
        for col in df_baixas.columns:
            if any(p in str(col).lower() for p in ['total', 'valor', 'baixa']):
                valor_col = col
                break

        if valor_col:
            df_baixas['_valor_num'] = df_baixas[valor_col].apply(to_number_ptbr)
            resultado['baixas_sim'] = float(df_baixas[df_baixas['Royalties'] == 'SIM']['_valor_num'].sum())
            resultado['baixas_nao'] = float(df_baixas[df_baixas['Royalties'] == 'NÃO']['_valor_num'].sum())
            resultado['baixas_total'] = resultado['baixas_sim'] + resultado['baixas_nao']

        # Gera VALIDAÇÃO BAIXAS
        try:
            campos = escolher_campos_linhas(df_baixas)
            valor = escolher_coluna_valor(df_baixas, ['total', 'valor', 'baixa'])
            todas_abas['VALIDAÇÃO BAIXAS'] = criar_validacao_sim_nao(df_baixas, campos, valor)
        except Exception as e:
            todas_abas['VALIDAÇÃO BAIXAS'] = pd.DataFrame([{'Erro': 'Falha ao criar VALIDAÇÃO BAIXAS', 'Motivo': str(e)}])

    # Gera arquivo de saída
    caminho = Path(arquivo_path)
    data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_saida = f"{caminho.stem}_processado_{data_hora}{caminho.suffix}"
    arquivo_saida = os.path.join(ROYALTIES_OUTPUT_FOLDER, nome_saida)

    # Salva
    wb_original.save(arquivo_saida)
    wb = openpyxl.load_workbook(arquivo_saida)

    # Formatação
    header_fill = PatternFill(start_color="002233", end_color="002233", fill_type="solid")
    header_font = Font(bold=True, color="00DBFF", size=11)
    border_style = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    for nome_aba, df in todas_abas.items():
        # Remove coluna temporária se existir
        if '_valor_num' in df.columns:
            df = df.drop(columns=['_valor_num'])

        if nome_aba in wb.sheetnames:
            ws = wb[nome_aba]
            ws.delete_rows(1, ws.max_row)
        else:
            ws = wb.create_sheet(nome_aba)

        for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
            for c_idx, value in enumerate(row, 1):
                cell = ws.cell(row=r_idx, column=c_idx, value=value)
                if isinstance(value, (datetime, pd.Timestamp)):
                    cell.number_format = "DD/MM/YYYY"

        # Aplica formatação
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = border_style

        for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
            for cell in row:
                cell.border = border_style
                cell.alignment = Alignment(vertical="center")
                if isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
                    cell.number_format = "#,##0.00"

        for column in ws.columns:
            max_length = 0
            col_letter = column[0].column_letter
            for cell in column:
                try:
                    if cell.value is not None and cell.value != "":
                        max_length = max(max_length, len(str(cell.value)))
                except:
                    pass
            ws.column_dimensions[col_letter].width = min(max_length + 2, 50)

        ws.freeze_panes = ws["A2"]

    wb.save(arquivo_saida)

    resultado['arquivo_saida'] = nome_saida
    return resultado


@app.route('/api/royalties/processar', methods=['POST'])
@login_required
def api_processar_royalties():
    try:
        if 'arquivo' not in request.files:
            return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'}), 400

        arquivo = request.files['arquivo']

        if arquivo.filename == '':
            return jsonify({'success': False, 'error': 'Nenhum arquivo selecionado'}), 400

        if not arquivo.filename.endswith(('.xlsx', '.xls')):
            return jsonify({'success': False, 'error': 'Apenas arquivos .xlsx ou .xls são permitidos'}), 400

        # Salva arquivo temporário
        filename = secure_filename(arquivo.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        arquivo.save(filepath)

        # Processa
        resultado = processar_royalties(filepath)

        # Remove arquivo temporário
        os.remove(filepath)

        return jsonify({
            'success': True,
            **resultado
        })

    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': f"{str(e)}\n\n{traceback.format_exc()}"
        }), 500


@app.route('/api/royalties/download/<filename>')
@login_required
def api_download_royalties(filename):
    try:
        filepath = os.path.join(ROYALTIES_OUTPUT_FOLDER, filename)
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 404



# ==================================================================================
# ROTAS DE CONCILIACAO BANCARIA
# ==================================================================================

@app.route('/conciliacao')
@login_required
def conciliacao_menu():
    """Menu de opções de conciliação"""
    return render_template('conciliacao_menu.html', user={
        'username': session.get('user'),
        'name': session.get('name'),
        'is_admin': session.get('is_admin')
    })


@app.route('/conciliacao/bancaria-contabil')
@login_required
def conciliacao_bancaria_contabil():
    """Conciliação Bancária x Contábil"""
    return render_template('conciliacao_bancaria_contabil.html', user={
        'username': session.get('user'),
        'name': session.get('name'),
        'is_admin': session.get('is_admin')
    })


@app.route('/api/conciliacao/processar', methods=['POST'])
@login_required
def api_conciliacao_processar():
    """Processa conciliacao bancaria"""
    try:
        if processar_conciliacao is None:
            return jsonify({'success': False, 'error': 'Modulo de conciliacao nao encontrado'}), 500

        if 'arquivo_fin' not in request.files or 'arquivo_contabil' not in request.files:
            return jsonify({'success': False, 'error': 'Envie os dois arquivos (Financeiro e Contabil)'}), 400

        fin_file = request.files['arquivo_fin']
        cont_file = request.files['arquivo_contabil']

        if fin_file.filename == '' or cont_file.filename == '':
            return jsonify({'success': False, 'error': 'Selecione os 2 arquivos'}), 400

        # Salva arquivos temporarios
        fin_filename = secure_filename(fin_file.filename)
        cont_filename = secure_filename(cont_file.filename)

        fin_path = os.path.join(app.config['UPLOAD_FOLDER'], f"conc_fin_{fin_filename}")
        cont_path = os.path.join(app.config['UPLOAD_FOLDER'], f"conc_cont_{cont_filename}")

        fin_file.save(fin_path)
        cont_file.save(cont_path)

        # Gera arquivo de saida
        data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"conciliacao_{data_hora}.xlsx"
        output_path = os.path.join(CONCILIACAO_OUTPUT_FOLDER, output_filename)

        # Processa conciliacao
        stats = processar_conciliacao(
            financeiro_path=fin_path,
            contabil_path=cont_path,
            output_xlsx=output_path,
            tolerancia=0.01,
            min_len=3
        )

        # Remove arquivos temporarios
        try:
            os.remove(fin_path)
            os.remove(cont_path)
        except:
            pass

        return jsonify({
            'success': True,
            'arquivo': output_filename,
            **stats
        })

    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': f"{str(e)}\n\n{traceback.format_exc()}"
        }), 500


@app.route('/api/conciliacao/download/<filename>')
@login_required
def api_conciliacao_download(filename):
    """Download do arquivo de conciliacao"""
    try:
        filepath = os.path.join(CONCILIACAO_OUTPUT_FOLDER, filename)
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 404


# ==================================================================================
# ROTAS DE CONCILIACAO CLIENTE (NFs x Recebimentos)
# ==================================================================================

@app.route('/conciliacao/cliente')
@login_required
def conciliacao_cliente():
    """Conciliação Cliente - NFs x Recebimentos"""
    return render_template('conciliacao_bancaria_cliente.html', user={
        'username': session.get('user'),
        'name': session.get('name'),
        'is_admin': session.get('is_admin')
    })


@app.route('/api/conciliacao-cliente/processar', methods=['POST'])
@login_required
def api_conciliacao_cliente_processar():
    """Processa conciliacao cliente"""
    try:
        if processar_conciliacao_cliente is None:
            return jsonify({'success': False, 'error': 'Modulo de conciliacao cliente nao encontrado'}), 500

        if 'arquivo' not in request.files:
            return jsonify({'success': False, 'error': 'Envie o arquivo de Razão Contábil'}), 400

        arquivo = request.files['arquivo']
        if arquivo.filename == '':
            return jsonify({'success': False, 'error': 'Selecione o arquivo'}), 400

        saldo_inicial = request.form.get('saldo_inicial', '0')
        data_inicio = request.form.get('data_inicio', '')
        data_fim = request.form.get('data_fim', '')

        # Salva arquivo temporario
        filename = secure_filename(arquivo.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"conc_cli_{filename}")
        arquivo.save(filepath)

        # Arquivo financeiro (opcional)
        filepath_fin = None
        if 'arquivo_financeiro' in request.files:
            arquivo_fin = request.files['arquivo_financeiro']
            if arquivo_fin.filename != '':
                fin_filename = secure_filename(arquivo_fin.filename)
                filepath_fin = os.path.join(app.config['UPLOAD_FOLDER'], f"conc_cli_fin_{fin_filename}")
                arquivo_fin.save(filepath_fin)

        # Gera arquivo de saida
        data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"conciliacao_cliente_{data_hora}.xlsx"
        output_path = os.path.join(CONCILIACAO_OUTPUT_FOLDER, output_filename)

        # Processa conciliacao
        resultado = processar_conciliacao_cliente(
            arquivo_path=filepath,
            saldo_inicial=saldo_inicial,
            data_inicio=data_inicio,
            data_fim=data_fim,
            output_path=output_path,
            arquivo_financeiro_path=filepath_fin
        )

        # Remove arquivos temporarios
        try:
            os.remove(filepath)
        except:
            pass
        if filepath_fin:
            try:
                os.remove(filepath_fin)
            except:
                pass

        return jsonify({
            'success': True,
            'arquivo': output_filename,
            **resultado
        })

    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': f"{str(e)}\n\n{traceback.format_exc()}"
        }), 500


@app.route('/api/conciliacao-cliente/download/<filename>')
@login_required
def api_conciliacao_cliente_download(filename):
    """Download do arquivo de conciliacao cliente"""
    try:
        filepath = os.path.join(CONCILIACAO_OUTPUT_FOLDER, filename)
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 404


if __name__ == '__main__':
    app.run(debug=True, port=5001, use_reloader=False)
