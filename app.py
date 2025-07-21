import os
import paramiko
from flask import Flask, render_template, request, jsonify
from pathlib import Path
import subprocess
import threading
import re
import time

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'

transfer_in_progress = False
transfer_status = {
    'progress': 0,
    'current_file': '',
    'total_files': 0,
    'transferred_files': 0,
    'speed': '',
    'time_remaining': '',
    'error': None,
    'output': []
}

def get_remote_info(host, username='root'):
    """Получаем информацию о сервере через SSH"""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, username=username, key_filename='/root/.ssh/id_rsa')
        
        stdin, stdout, stderr = ssh.exec_command('hostname')
        hostname = stdout.read().decode().strip()
        
        stdin, stdout, stderr = ssh.exec_command('du -sh /d2/contSrc')
        dir_size = stdout.read().decode().split()[0]
        
        return {
            'hostname': hostname,
            'ip': host,
            'dir_size': dir_size,
            'error': None
        }
    except Exception as e:
        return {
            'error': str(e)
        }
    finally:
        ssh.close()

def create_symlinks():
    """Создаем симлинки в /d2/vod"""
    vod_dir = Path('/d2/vod')
    cont_src_dir = Path('/d2/contSrc')
    
    if not vod_dir.exists():
        vod_dir.mkdir(parents=True, exist_ok=True)
    
    for item in cont_src_dir.iterdir():
        if item.is_dir():
            link_path = vod_dir / item.name
            if not link_path.exists():
                link_path.symlink_to(item)

def transfer_content(host, username='root', threads=8):
    """Перенос с поддержкой многопоточности"""
    global transfer_in_progress, transfer_status
    
    transfer_in_progress = True
    transfer_status.update({
        'progress': 0,
        'current_file': 'Starting parallel transfer...',
        'total_files': 0,
        'transferred_files': 0,
        'speed': '',
        'time_remaining': '',
        'error': None,
        'output': []
    })
    
    try:
        os.makedirs('/d2/contSrc', exist_ok=True)
        
        # Генерируем список файлов для параллельной обработки
        transfer_status['output'].append("Generating file list...")
        
        # Получаем список файлов через ssh
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=username, key_filename='/root/.ssh/id_rsa')
        
        stdin, stdout, stderr = ssh.exec_command('find /d2/contSrc -type f')
        all_files = stdout.read().decode().splitlines()
        ssh.close()
        
        total_files = len(all_files)
        transfer_status['total_files'] = total_files
        transfer_status['output'].append(f"Found {total_files} files to transfer")
        
        # Функция для копирования одного файла
        def copy_file(file_path):
            try:
                rel_path = os.path.relpath(file_path, '/d2/contSrc')
                dest_dir = os.path.dirname(f'/d2/contSrc/{rel_path}')
                os.makedirs(dest_dir, exist_ok=True)
                
                subprocess.run([
                    'rsync',
                    '-az',
                    '-e', 'ssh -i /root/.ssh/id_rsa -o StrictHostKeyChecking=no',
                    f'{username}@{host}:{file_path}',
                    f'/d2/contSrc/{rel_path}'
                ], check=True)
                
                # Обновляем статус
                with threading.Lock():
                    transfer_status['transferred_files'] += 1
                    progress = int((transfer_status['transferred_files'] / total_files) * 100)
                    transfer_status['progress'] = progress
                    transfer_status['current_file'] = rel_path
                    transfer_status['output'].append(f"Copied: {rel_path}")
                    
            except Exception as e:
                with threading.Lock():
                    transfer_status['error'] = f"Failed to copy {file_path}: {str(e)}"
                    transfer_status['output'].append(f"ERROR: {file_path} - {str(e)}")

        # Запускаем в пуле потоков
        from concurrent.futures import ThreadPoolExecutor
        
        transfer_status['output'].append(f"Starting transfer with {threads} threads...")
        with ThreadPoolExecutor(max_workers=threads) as executor:
            executor.map(copy_file, all_files)
        
        if not transfer_status['error']:
            transfer_status['output'].append("Transfer completed successfully")
            transfer_status['progress'] = 100
            create_symlinks()
            
    except Exception as e:
        transfer_status['error'] = str(e)
        transfer_status['output'].append(f"EXCEPTION: {str(e)}")
    finally:
        transfer_in_progress = False

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_server_info', methods=['POST'])
def get_server_info():
    host = request.form.get('host')
    info = get_remote_info(host)
    if info.get('error'):
        return jsonify({'success': False, 'error': info['error']})
    return jsonify({'success': True, 'info': info})

@app.route('/start_transfer', methods=['POST'])
def start_transfer():
    if transfer_in_progress:
        return jsonify({'success': False, 'error': 'Transfer already in progress'})
    
    host = request.form.get('host')
    thread = threading.Thread(target=transfer_content, args=(host,))
    thread.daemon = True
    thread.start()
    
    return jsonify({'success': True})

@app.route('/transfer_status')
def get_transfer_status():
    return jsonify(transfer_status)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9088, threaded=True)