from flask import Flask, render_template, request, redirect, url_for, session, jsonify, flash
import os
import subprocess
import threading
from datetime import datetime
import sqlite3
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = 'your_secret_key_here_akiru_team_2023'  # Change this to a strong secret key
app.config['UPLOAD_FOLDER'] = 'user_bots'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Database setup
def init_db():
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 username TEXT UNIQUE,
                 password TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS files
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 user_id INTEGER,
                 filename TEXT,
                 filetype TEXT,
                 upload_date TEXT,
                 FOREIGN KEY(user_id) REFERENCES users(id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS processes
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 file_id INTEGER,
                 pid INTEGER,
                 start_time TEXT,
                 FOREIGN KEY(file_id) REFERENCES files(id))''')
    conn.commit()
    conn.close()

init_db()

# Process management
running_processes = {}
process_logs = {}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in {'py', 'js', 'zip'}

@app.route('/')
def index():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('SELECT id, filename, filetype FROM files WHERE user_id = ?', (session['user_id'],))
    files = c.fetchall()
    conn.close()
    
    # Check which files are running
    file_status = []
    for file in files:
        file_id, filename, filetype = file
        is_running = file_id in running_processes and running_processes[file_id].poll() is None
        file_status.append({
            'id': file_id,
            'name': filename,
            'type': filetype,
            'running': is_running
        })
    
    return render_template('index.html', files=file_status, username=session.get('username'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        conn = sqlite3.connect('bot_data.db')
        c = conn.cursor()
        c.execute('SELECT id FROM users WHERE username = ? AND password = ?', (username, password))
        user = c.fetchone()
        conn.close()
        
        if user:
            session['user_id'] = user[0]
            session['username'] = username
            return redirect(url_for('index'))
        else:
            flash('Invalid credentials', 'error')
    
    return render_template('login.html')

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        conn = sqlite3.connect('bot_data.db')
        c = conn.cursor()
        try:
            c.execute('INSERT INTO users (username, password) VALUES (?, ?)', (username, password))
            conn.commit()
            flash('Registration successful! Please login.', 'success')
            return redirect(url_for('login'))
        except sqlite3.IntegrityError:
            flash('Username already exists', 'error')
        finally:
            conn.close()
    
    return render_template('register.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    # Check file limit
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM files WHERE user_id = ?', (session['user_id'],))
    file_count = c.fetchone()[0]
    
    if file_count >= 5:
        conn.close()
        flash('You can only upload up to 5 files', 'error')
        return redirect(url_for('index'))
    
    if 'file' not in request.files:
        conn.close()
        flash('No file selected', 'error')
        return redirect(url_for('index'))
    
    file = request.files['file']
    if file.filename == '':
        conn.close()
        flash('No file selected', 'error')
        return redirect(url_for('index'))
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filetype = filename.rsplit('.', 1)[1].lower()
        
        # Create user folder if not exists
        user_folder = os.path.join(app.config['UPLOAD_FOLDER'], str(session['user_id']))
        os.makedirs(user_folder, exist_ok=True)
        
        # Save file
        filepath = os.path.join(user_folder, filename)
        file.save(filepath)
        
        # Add to database
        c.execute('INSERT INTO files (user_id, filename, filetype, upload_date) VALUES (?, ?, ?, ?)',
                  (session['user_id'], filename, filetype, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        flash('File uploaded successfully', 'success')
        return redirect(url_for('index'))
    
    conn.close()
    flash('Invalid file type. Only .py, .js, and .zip files are allowed', 'error')
    return redirect(url_for('index'))

@app.route('/control/<int:file_id>/<action>')
def control_file(file_id, action):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('SELECT filename, filetype FROM files WHERE id = ? AND user_id = ?', 
              (file_id, session['user_id']))
    file = c.fetchone()
    
    if not file:
        conn.close()
        flash('File not found', 'error')
        return redirect(url_for('index'))
    
    filename, filetype = file
    user_folder = os.path.join(app.config['UPLOAD_FOLDER'], str(session['user_id']))
    filepath = os.path.join(user_folder, filename)
    
    if action == 'start':
        if file_id in running_processes and running_processes[file_id].poll() is None:
            conn.close()
            flash('This file is already running', 'error')
            return redirect(url_for('index'))
        
        try:
            if filetype == 'py':
                process = subprocess.Popen(['python', filepath], 
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT,
                                         text=True)
            elif filetype == 'js':
                process = subprocess.Popen(['node', filepath],
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT,
                                         text=True)
            else:
                conn.close()
                flash('Unsupported file type', 'error')
                return redirect(url_for('index'))
            
            running_processes[file_id] = process
            process_logs[file_id] = []
            
            # Start log collection thread
            threading.Thread(target=collect_logs, args=(file_id, process)).start()
            
            c.execute('INSERT INTO processes (file_id, pid, start_time) VALUES (?, ?, ?)',
                      (file_id, process.pid, datetime.now().isoformat()))
            conn.commit()
            conn.close()
            
            flash('Script started successfully', 'success')
            return redirect(url_for('index'))
        
        except Exception as e:
            conn.close()
            flash(f'Error starting script: {str(e)}', 'error')
            return redirect(url_for('index'))
    
    elif action == 'stop':
        if file_id not in running_processes:
            conn.close()
            flash('Script is not running', 'error')
            return redirect(url_for('index'))
        
        process = running_processes[file_id]
        process.terminate()
        del running_processes[file_id]
        if file_id in process_logs:
            del process_logs[file_id]
        
        c.execute('DELETE FROM processes WHERE file_id = ?', (file_id,))
        conn.commit()
        conn.close()
        
        flash('Script stopped successfully', 'success')
        return redirect(url_for('index'))
    
    conn.close()
    flash('Invalid action', 'error')
    return redirect(url_for('index'))

def collect_logs(file_id, process):
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            process_logs[file_id].append(output.strip())

@app.route('/logs/<int:file_id>')
def get_logs(file_id):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    # Verify user owns this file
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('SELECT 1 FROM files WHERE id = ? AND user_id = ?', 
              (file_id, session['user_id']))
    if not c.fetchone():
        conn.close()
        flash('Access denied', 'error')
        return redirect(url_for('index'))
    
    conn.close()
    
    if file_id not in process_logs:
        return jsonify([])
    
    return jsonify(process_logs[file_id][-100:])  # Return last 100 lines

@app.route('/delete/<int:file_id>', methods=['POST'])
def delete_file(file_id):
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    
    # Get file info
    c.execute('SELECT filename FROM files WHERE id = ? AND user_id = ?', 
              (file_id, session['user_id']))
    file = c.fetchone()
    
    if not file:
        conn.close()
        flash('File not found', 'error')
        return redirect(url_for('index'))
    
    filename = file[0]
    
    # Stop if running
    if file_id in running_processes:
        running_processes[file_id].terminate()
        del running_processes[file_id]
        if file_id in process_logs:
            del process_logs[file_id]
    
    # Delete from filesystem
    user_folder = os.path.join(app.config['UPLOAD_FOLDER'], str(session['user_id']))
    filepath = os.path.join(user_folder, filename)
    if os.path.exists(filepath):
        os.remove(filepath)
    
    # Delete from database
    c.execute('DELETE FROM files WHERE id = ?', (file_id,))
    c.execute('DELETE FROM processes WHERE file_id = ?', (file_id,))
    conn.commit()
    conn.close()
    
    flash('File deleted successfully', 'success')
    return redirect(url_for('index'))

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(host='0.0.0.0', port=5000)