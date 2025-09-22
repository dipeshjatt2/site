from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import os
import zipfile
import time
import uuid
import logging
from datetime import datetime
import threading # <-- 1. IMPORT THREADING

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
BATCH_SIZE = 3
QUESTION_WORKERS = 5
MAX_WORKERS = 10
TIMEOUT = 30

class ScraperAPI:
    def __init__(self):
        self.active_tasks = {}
        self.base_url = "https://testnookapp-f602da876a9b.herokuapp.com"
        
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-GB',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36',
        }

    def sanitize_filename(self, name):
        """Removes invalid characters from filenames."""
        return re.sub(r'[\\/*?:"<>|]', "", name)

    async def scrape_quiz_ids(self, creator_id, page_num):
        """Scrapes quiz IDs from creator pages."""
        quiz_ids = []
        urls = []
        
        for page in range(1, page_num + 1):
            if page == 1:
                urls.append(f"{self.base_url}/creator/{creator_id}")
            else:
                urls.append(f"{self.base_url}/creator/{creator_id}?page={page}")
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = []
            for url in urls:
                task = asyncio.create_task(self.scrape_single_page(session, url))
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, list):
                    quiz_ids.extend(result)
        
        return quiz_ids

    async def scrape_single_page(self, session, url):
        """Scrapes a single page for quiz data."""
        try:
            async with session.get(url, timeout=TIMEOUT) as response:
                if response.status != 200:
                    return []
                html = await response.text()
            
            soup = BeautifulSoup(html, 'html.parser')
            quiz_cards = soup.find_all('div', class_='quiz-card')
            
            if not quiz_cards:
                return []
            
            quizzes = []
            for card in quiz_cards:
                name_tag = card.find('h3')
                quiz_name = name_tag.get_text(strip=True) if name_tag else "Unknown Quiz Name"

                onclick_attr = card.get('onclick', '')
                match = re.search(r"/quiz/([a-zA-Z0-9]+)", onclick_attr)
                quiz_id = match.group(1) if match else None

                if quiz_id:
                    quizzes.append({
                        'quiz_name': quiz_name,
                        'quiz_id': quiz_id
                    })
            
            return quizzes
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return []

    async def scrape_quiz_batch(self, quiz_ids, task_id):
        """Scrapes a batch of quizzes."""
        if task_id not in self.active_tasks:
            return []
        
        scraped_files = []
        
        for i, quiz_info in enumerate(quiz_ids):
            if self.active_tasks[task_id].get('cancelled'):
                break
                
            try:
                file_path = await self.scrape_single_quiz(quiz_info)
                if file_path and os.path.exists(file_path):
                    scraped_files.append(file_path)
                    
                # Update progress
                self.active_tasks[task_id]['processed'] = i + 1
                self.active_tasks[task_id]['last_update'] = time.time()
                
            except Exception as e:
                logger.error(f"Error processing quiz {quiz_info['quiz_id']}: {e}")
                continue
        
        return scraped_files

    async def scrape_single_quiz(self, quiz_info):
        """Scrapes a single quiz."""
        quiz_name = quiz_info['quiz_name']
        quiz_id = quiz_info['quiz_id']
        output_filename = f"temp_{uuid.uuid4().hex}_{self.sanitize_filename(quiz_name)}.txt"
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                first_q_url = f"{self.base_url}/quiz/{quiz_id}/question/0"
                async with session.get(first_q_url, timeout=TIMEOUT) as response:
                    if response.status != 200:
                        raise Exception(f"HTTP {response.status}")
                    
                    response_text = await response.text()
                    
                    if "Quiz Complete" in response_text:
                        with open(output_filename, 'w', encoding='utf-8') as f:
                            f.write("Quiz is empty or already complete.")
                        return output_filename
                    
                    soup = BeautifulSoup(response_text, 'html.parser')
                    progress_div = soup.find('div', class_='question-progress')
                    total_questions = 1
                    if progress_div:
                        progress_span = progress_div.find('span')
                        if progress_span:
                            progress_text = progress_span.get_text(strip=True)
                            if '/' in progress_text:
                                total_questions = int(progress_text.split('/')[1])
                
                question_tasks = []
                for q_num in range(total_questions):
                    question_tasks.append(self.fetch_and_solve_question(session, quiz_id, q_num))
                
                question_results = await asyncio.gather(*question_tasks, return_exceptions=True)
                
                with open(output_filename, 'w', encoding='utf-8') as f:
                    for result in sorted(question_results, key=lambda x: x.get('q_num', float('inf'))):
                        q_num = result.get('q_num', -1)
                        
                        if isinstance(result, Exception) or "error" in result:
                            f.write(f"{q_num + 1}. FAILED TO FETCH QUESTION: {result.get('error', 'Unknown Error')}\n\n")
                            continue
                        
                        f.write(f"{q_num + 1}. {result['text']}\n")
                        for i, option_text in enumerate(result['options']):
                            cleaned_option = re.sub(r'^[A-Z]\s*', '', option_text)
                            marker = "✅" if i == result['correct_index'] else ""
                            f.write(f"({chr(97 + i)}) {cleaned_option} {marker}\n")
                        f.write("\n")
                
                return output_filename
                
        except Exception as e:
            logger.error(f"Error scraping quiz {quiz_id}: {e}")
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(f"Error scraping quiz: {str(e)}")
            return output_filename

    async def fetch_and_solve_question(self, session, quiz_id, q_num):
        """Fetches and processes a single question."""
        try:
            q_url = f"{self.base_url}/quiz/{quiz_id}/question/{q_num}"
            async with session.get(q_url, timeout=TIMEOUT) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")
                html = await response.text()
            
            soup = BeautifulSoup(html, 'html.parser')
            
            question_text_elem = soup.find('div', class_='question-text')
            question_text = question_text_elem.get_text(strip=True) if question_text_elem else f"Question {q_num + 1}"
            
            options = []
            option_elements = soup.find_all('div', class_='option')
            for opt in option_elements:
                options.append(opt.get_text(strip=True))
            
            return {
                "q_num": q_num,
                "text": question_text,
                "options": options,
                "correct_index": 0
            }
            
        except Exception as e:
            return {"q_num": q_num, "error": str(e)}

    def create_zip_file(self, creator_id, scraped_files, task_id):
        """Creates a zip file with all scraped quizzes."""
        if not scraped_files:
            return None
            
        zip_filename = f"temp_{task_id}_{creator_id}_quizzes.zip"
        
        try:
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file in scraped_files:
                    if os.path.exists(file):
                        zipf.write(file, os.path.basename(file).split('_', 2)[-1]) # Clean up temp filename
            
            return zip_filename
        except Exception as e:
            logger.error(f"Error creating zip file: {e}")
            return None

    def cleanup_task(self, task_id):
        """Clean up files for a task."""
        try:
            task = self.active_tasks.get(task_id, {})
            # Clean up individual text files from the task if they exist
            if 'result_files' in task:
                for file_path in task['result_files']:
                    if file_path and os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except OSError as e:
                            logger.error(f"Error removing file {file_path}: {e}")

            # Clean up the zip file
            zip_file = task.get('zip_file')
            if zip_file and os.path.exists(zip_file):
                try:
                    os.remove(zip_file)
                except OSError as e:
                    logger.error(f"Error removing zip file {zip_file}: {e}")
            
            # Remove from active tasks
            if task_id in self.active_tasks:
                del self.active_tasks[task_id]
                logger.info(f"Task {task_id} cleaned up and removed.")
        except Exception as e:
            logger.error(f"Error during generic cleanup for task {task_id}: {e}")

# Initialize the scraper
scraper = ScraperAPI()

# ---- ASYNC TASK EXECUTION (FIX) ----
# 2. CREATE A WRAPPER FUNCTION TO RUN THE ASYNC CODE
def run_async_task(task_id):
    """
    Sets up and runs the asyncio event loop in a new thread.
    """
    asyncio.run(execute_scraping_task(task_id))

async def execute_scraping_task(task_id):
    """Execute the scraping task."""
    if task_id not in scraper.active_tasks:
        return
    
    task = scraper.active_tasks[task_id]
    
    try:
        task['status'] = 'scraping_ids'
        quiz_ids = await scraper.scrape_quiz_ids(task['creator_id'], task['page_num'])
        
        if task.get('cancelled'):
            task['status'] = 'cancelled'
            return
        
        if not quiz_ids:
            task['status'] = 'completed'
            task['error'] = "No quizzes found for this creator."
            return
        
        task['quiz_ids'] = quiz_ids
        task['total'] = len(quiz_ids)
        task['status'] = 'scraping_quizzes'
        
        scraped_files = await scraper.scrape_quiz_batch(quiz_ids, task_id)
        task['result_files'] = scraped_files
        
        status_after_scrape = 'cancelled' if task.get('cancelled') else 'completed'

        if scraped_files:
            zip_file = scraper.create_zip_file(task['creator_id'], scraped_files, task_id)
            task['zip_file'] = zip_file
        
        task['status'] = status_after_scrape
        if status_after_scrape == 'completed':
            task['processed'] = task['total']

    except Exception as e:
        logger.error(f"Error in task {task_id}: {e}")
        task['status'] = 'error'
        task['error'] = str(e)

# --- API Routes ---
@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_tasks": len(scraper.active_tasks)
    })

@app.route('/api/scrape/start', methods=['POST'])
def start_scraping():
    """Start a new scraping task."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
            
        creator_id = data.get('creator_id')
        page_num = data.get('page_num', 1)
        
        if not creator_id:
            return jsonify({"error": "creator_id is required"}), 400
        
        try:
            page_num = int(page_num)
        except (ValueError, TypeError):
            return jsonify({"error": "page_num must be an integer"}), 400
        
        if not (1 <= page_num <= 1000):
            return jsonify({"error": "Page number must be between 1 and 1000"}), 400
        
        task_id = uuid.uuid4().hex[:8]
        
        scraper.active_tasks[task_id] = {
            'creator_id': creator_id,
            'page_num': page_num,
            'status': 'starting',
            'processed': 0, 'total': 0,
            'start_time': time.time(),
            'last_update': time.time(),
            'cancelled': False,
            'quiz_ids': [], 'result_files': [], 'zip_file': None
        }
        
        # 3. START THE BACKGROUND THREAD INSTEAD OF CALLING ASYNCIO DIRECTLY
        thread = threading.Thread(target=run_async_task, args=(task_id,))
        thread.daemon = True
        thread.start()
        
        return jsonify({
            "task_id": task_id,
            "status": "started",
            "message": f"Scraping task started for creator {creator_id}",
            "details": {"pages": page_num}
        })
        
    except Exception as e:
        logger.error(f"Error starting task: {e}")
        return jsonify({"error": "An unexpected server error occurred."}), 500

@app.route('/api/scrape/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    if task_id not in scraper.active_tasks:
        return jsonify({"error": "Task not found"}), 404
    
    task = scraper.active_tasks[task_id]
    
    progress_percentage = 0
    if task['total'] > 0:
        progress_percentage = round((task['processed'] / task['total']) * 100, 1)
    
    response = {
        "task_id": task_id,
        "status": task['status'],
        "progress": task['processed'],
        "total": task['total'],
        "progress_percentage": progress_percentage,
        "elapsed_time": round(time.time() - task['start_time'], 1),
        "creator_id": task['creator_id']
    }
    
    if task.get('zip_file') and os.path.exists(task['zip_file']):
        response['download_ready'] = True
        response['file_size'] = os.path.getsize(task['zip_file'])
    
    if 'error' in task:
        response['error'] = task['error']
    
    return jsonify(response)

@app.route('/api/scrape/download/<task_id>', methods=['GET'])
def download_results(task_id):
    if task_id not in scraper.active_tasks:
        return jsonify({"error": "Task not found"}), 404
    
    task = scraper.active_tasks[task_id]
    zip_file = task.get('zip_file')
    
    if not zip_file:
        return jsonify({"error": "Results not ready or no files were generated"}), 400
    
    if not os.path.exists(zip_file):
        return jsonify({"error": "File not found on server, it may have been cleaned up."}), 404
    
    try:
        return send_file(
            zip_file,
            as_attachment=True,
            download_name=f"creator_{task['creator_id']}_quizzes.zip",
            mimetype='application/zip'
        )
    except Exception as e:
        logger.error(f"Error sending file for task {task_id}: {e}")
        return jsonify({"error": "Could not send file."}), 500

@app.route('/api/scrape/cancel/<task_id>', methods=['POST'])
def cancel_task(task_id):
    if task_id not in scraper.active_tasks:
        return jsonify({"error": "Task not found"}), 404
    
    task = scraper.active_tasks[task_id]
    if task['status'] not in ['starting', 'scraping_ids', 'scraping_quizzes']:
        return jsonify({"message": "Task is already completed or cancelled."}), 400

    task['cancelled'] = True
    task['status'] = 'cancelling'
    
    return jsonify({"task_id": task_id, "message": "Task cancellation requested."})

@app.route('/api/scrape/cleanup/<task_id>', methods=['POST'])
def cleanup_task_route(task_id):
    if task_id not in scraper.active_tasks:
        return jsonify({"error": "Task not found or already cleaned up"}), 404
    scraper.cleanup_task(task_id)
    return jsonify({"message": f"Task {task_id} cleaned up."})

@app.route('/api/scrape/list', methods=['GET'])
def list_tasks():
    active_tasks_summary = {}
    for task_id, task in scraper.active_tasks.items():
        active_tasks_summary[task_id] = {
            "status": task.get('status'),
            "creator_id": task.get('creator_id'),
            "progress": task.get('processed'),
            "total": task.get('total'),
            "elapsed_time": round(time.time() - task.get('start_time', 0), 1)
        }
    return jsonify({"active_tasks": active_tasks_summary, "total_tasks": len(active_tasks_summary)})

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "message": "Scraper API is operational.  by   @andr0idpie9 akka  Choudhary sahab      ",
        "endpoints": {
            "GET /api/health": "Health check",
            "POST /api/scrape/start": "Body: {'creator_id': str, 'page_num': int}",
            "GET /api/scrape/status/<task_id>": "Check task status",
            "GET /api/scrape/download/<task_id>": "Download results zip",
            "POST /api/scrape/cancel/<task_id>": "Cancel running task",
            "POST /api/scrape/cleanup/<task_id>": "Manually clean up a task's files",
            "GET /api/scrape/list": "List all active tasks"
        }
    })

# --- Cleanup Scheduler ---
def cleanup_old_tasks():
    # Schedule next run before executing current one
    threading.Timer(600, cleanup_old_tasks).start()
    
    current_time = time.time()
    # Use a copy of keys to avoid runtime errors during dict modification
    tasks_to_remove = [
        task_id for task_id, task in scraper.active_tasks.items()
        if current_time - task.get('start_time', 0) > 360000  # 1 hour
    ]
    
    if tasks_to_remove:
        logger.info(f"Auto-cleaning up {len(tasks_to_remove)} old tasks.")
        for task_id in tasks_to_remove:
            scraper.cleanup_task(task_id)

if __name__ == '__main__':
    # Start the periodic cleanup thread
    cleanup_thread = threading.Thread(target=cleanup_old_tasks, daemon=True)
    cleanup_thread.start()
    
    port = int(os.environ.get('PORT', 5000))
    # Use a production-ready WSGI server like gunicorn instead of app.run for deployment
    app.run(host='0.0.0.0', port=port, debug=False)
