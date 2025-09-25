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
import threading
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
TIMEOUT = 30
BATCH_SIZE = 5
QUESTION_WORKERS = 10

class ScraperAPI:
    def __init__(self):
        self.active_tasks = {}
        self.base_url = "https://testnookapp-f602da876a9b.herokuapp.com"
        
        # Updated headers to match the working script for better compatibility
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-GB',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36',
            'sec-ch-ua': '"Chromium";v="127", "Not)A;Brand";v="99", "Microsoft Edge Simulate";v="127", "Lemur";v="127"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
        }
        
        # POST headers will automatically inherit the updated base headers
        self.post_headers = {
            **self.headers,
            'Accept': '*/*',
            'Content-Type': 'application/json',
            'Origin': self.base_url,
            'Sec-Fetch-Mode': 'cors',
        }

    def sanitize_filename(self, name):
        """
        Cleans and sanitizes a string to be used as a valid filename.
        Handles special characters, newlines, and length issues.
        """
        # Replace newline characters with a space
        name = name.replace('\n', ' ').replace('\r', '')
        # Remove characters that are invalid in filenames across most OS
        name = re.sub(r'[\\/*?:"<>|]', "", name)
        # Trim leading/trailing whitespace
        name = name.strip()
        # Truncate the name to a safe length (e.g., 150 characters) to avoid filesystem errors
        if len(name) > 150:
            name = name[:150]
        # If the name is empty after sanitization, provide a default name
        if not name:
            name = "untitled_quiz"
        return name

    async def scrape_quiz_ids(self, creator_id, page_num):
        """Scrapes quiz IDs from creator pages."""
        urls = []
        for i in range(1, page_num + 1):
            if i == 1:
                urls.append(f"{self.base_url}/creator/{creator_id}")
            else:
                urls.append(f"{self.base_url}/creator/{creator_id}?page={i}")

        quiz_ids = []
        async with aiohttp.ClientSession(headers=self.headers) as session:
            tasks = [self.scrape_single_page(session, url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, list):
                    quiz_ids.extend(result)
        
        return quiz_ids

    async def scrape_single_page(self, session, url):
        """Scrapes a single page for quiz data."""
        try:
            async with session.get(url, timeout=TIMEOUT) as response:
                response.raise_for_status()
                html = await response.text()
            
            soup = BeautifulSoup(html, 'html.parser')
            quiz_cards = soup.find_all('div', class_='quiz-card')
            if not quiz_cards: return []
            
            quizzes = []
            for card in quiz_cards:
                name_tag = card.find('h3')
                quiz_name = name_tag.get_text(strip=True) if name_tag else "Unknown Quiz"
                match = re.search(r"/quiz/([a-zA-Z0-9]+)", card.get('onclick', ''))
                if match:
                    quizzes.append({'quiz_name': quiz_name, 'quiz_id': match.group(1)})
            return quizzes
            
        except Exception as e:
            logger.error(f"Error scraping page {url}: {e}")
            return []

    async def scrape_quiz_batch(self, quiz_ids, task_id):
        """Scrapes a batch of quizzes."""
        if task_id not in self.active_tasks: return []
        
        scraped_files = []
        total_quizzes = len(quiz_ids)
        
        # Process quizzes in batches
        for batch_start in range(0, total_quizzes, BATCH_SIZE):
            if self.active_tasks.get(task_id, {}).get('cancelled'): 
                break
                
            batch_end = min(batch_start + BATCH_SIZE, total_quizzes)
            current_batch = quiz_ids[batch_start:batch_end]
            
            # Process current batch with error handling
            batch_tasks = []
            for quiz_info in current_batch:
                task = asyncio.create_task(
                    self.scrape_single_quiz(quiz_info)
                )
                batch_tasks.append(task)
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Process results
            for i, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logger.error(f"Error processing quiz {current_batch[i]['quiz_id']}: {result}")
                    # Create error file for failed quizzes
                    quiz_info = current_batch[i]
                    error_filename = f"error_{quiz_info['quiz_id']}.txt"
                    with open(error_filename, 'w', encoding='utf-8-sig') as f:
                        f.write(f"Failed to scrape quiz: {quiz_info['quiz_name']}\n")
                        f.write(f"Quiz ID: {quiz_info['quiz_id']}\n")
                        f.write(f"Error: {str(result)}")
                    scraped_files.append(error_filename)
                    self.active_tasks[task_id]['processed'] += 1
                elif result:  # Successful result
                    scraped_files.append(result)
                    self.active_tasks[task_id]['processed'] += 1
                
                self.active_tasks[task_id]['last_update'] = time.time()
        
        return scraped_files

    async def scrape_single_quiz(self, quiz_info):
        """Scrapes a single quiz and saves it using a sanitized version of its name."""
        quiz_name = quiz_info['quiz_name']
        quiz_id = quiz_info['quiz_id']
        
        # Sanitize the quiz name to create a valid filename.
        sanitized_name = self.sanitize_filename(quiz_name)
        
        # Append the unique quiz_id to prevent files with the same name from overwriting each other.
        output_filename = f"{sanitized_name}_{quiz_id}.txt"
        
        try:
            async with aiohttp.ClientSession() as session:
                # Check if quiz is accessible
                first_q_url = f"{self.base_url}/quiz/{quiz_id}/question/0"
                async with session.get(first_q_url, headers=self.headers, timeout=TIMEOUT) as response:
                    response_text = await response.text()
                    
                    if "Quiz Complete" in response_text:
                        # Quiz is empty or already complete
                        with open(output_filename, 'w', encoding='utf-8-sig') as f:
                            f.write(f"Quiz: {quiz_name}\n")
                            f.write(f"Quiz ID: {quiz_id}\n")
                            f.write("Status: Quiz is empty or already complete.\n")
                        return output_filename
                    
                    # Get total questions
                    soup = BeautifulSoup(response_text, 'html.parser')
                    progress_span = soup.find('div', class_='question-progress').find('span')
                    if progress_span:
                        total_questions = int(progress_span.get_text(strip=True).split('/')[1])
                    else:
                        total_questions = 1
                
                # Fetch all questions in parallel
                question_tasks = [self.fetch_and_solve_question(session, quiz_id, q_num) for q_num in range(total_questions)]
                question_results = await asyncio.gather(*question_tasks, return_exceptions=True)
                
                # Sort results by question number and write to file with UTF-8-SIG encoding
                sorted_results = sorted(
                    [r for r in question_results if isinstance(r, dict)], 
                    key=lambda x: x.get('q_num', float('inf'))
                )
                
                with open(output_filename, 'w', encoding='utf-8-sig') as f:
                    # Write quiz header with original name
                    f.write(f"Quiz: {quiz_name}\n")
                    f.write(f"Quiz ID: {quiz_id}\n")
                    f.write(f"Total Questions: {total_questions}\n")
                    f.write("=" * 50 + "\n\n")
                    
                    for result in sorted_results:
                        if "error" in result:
                            f.write(f"{result.get('q_num', -1) + 1}. FAILED TO FETCH QUESTION: {result['error']}\n\n")
                            continue
                        
                        f.write(f"{result['q_num'] + 1}. {result['text']}\n")
                        for i, option_text in enumerate(result['options']):
                            cleaned_option = re.sub(r'^[A-Z]\s*', '', option_text)
                            marker = "✅" if i == result['correct_index'] else ""
                            f.write(f"({chr(97 + i)}) {cleaned_option} {marker}\n")
                        f.write("\n")
                
                return output_filename
                
        except Exception as e:
            logger.error(f"Error scraping quiz {quiz_id}: {e}")
            # Create error file
            error_filename = f"error_{quiz_id}.txt"
            with open(error_filename, 'w', encoding='utf-8-sig') as f:
                f.write(f"Quiz: {quiz_name}\n")
                f.write(f"Quiz ID: {quiz_id}\n")
                f.write(f"Error: {str(e)}\n")
            return error_filename

    async def fetch_and_solve_question(self, session, quiz_id, q_num):
        """Fetches a question, submits a dummy answer to get the correct option, and returns the parsed data."""
        try:
            q_url = f"{self.base_url}/quiz/{quiz_id}/question/{q_num}"
            async with session.get(q_url, headers=self.headers, timeout=TIMEOUT) as response:
                response.raise_for_status()
                html = await response.text()
            
            soup = BeautifulSoup(html, 'html.parser')
            question_text_elem = soup.find('div', class_='question-text')
            question_text = question_text_elem.get_text(strip=True) if question_text_elem else f"Question {q_num + 1}"
            
            options = [opt.get_text(strip=True) for opt in soup.find_all('div', class_='option')]
            
            answer_url = f"{self.base_url}/quiz/{quiz_id}/answer"
            payload = {"question_num": q_num, "selected_option": 0}
            
            current_post_headers = {**self.post_headers, 'Referer': q_url}
            
            async with session.post(answer_url, json=payload, headers=current_post_headers, timeout=TIMEOUT) as answer_res:
                answer_res.raise_for_status()
                answer_data = await answer_res.json()
            
            if not answer_data.get('success'):
                raise Exception("Failed to get correct answer from API.")

            correct_option_index = answer_data['correct_option']
            
            return {
                "q_num": q_num,
                "text": question_text,
                "options": options,
                "correct_index": correct_option_index
            }
            
        except Exception as e:
            return {"q_num": q_num, "error": str(e)}

    def create_quiz_ids_json(self, creator_id, quiz_ids, task_id):
        """Creates a JSON file with quiz IDs information."""
        json_filename = f"temp_{task_id}_creator_{creator_id}_quiz_ids.json"
        try:
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(quiz_ids, f, ensure_ascii=False, indent=2)
            return json_filename
        except Exception as e:
            logger.error(f"Error creating JSON file: {e}")
            return None

    def create_zip_file(self, creator_id, scraped_files, task_id):
        """Creates a zip file with all scraped quizzes."""
        if not scraped_files: return None
        zip_filename = f"temp_{task_id}_{creator_id}_quizzes.zip"
        try:
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file in scraped_files:
                    if os.path.exists(file):
                        # The file already has the new sanitized name on disk,
                        # so os.path.basename(file) will use that name in the zip archive.
                        zipf.write(file, os.path.basename(file))
            return zip_filename
        except Exception as e:
            logger.error(f"Error creating zip file: {e}")
            return None

    def cleanup_task(self, task_id):
        """Cleans up all temporary files associated with a task."""
        task = self.active_tasks.get(task_id, {})
        files_to_clean = task.get('result_files', [])
        if task.get('quiz_ids_json'):
            files_to_clean.append(task.get('quiz_ids_json'))
        if task.get('zip_file'):
            files_to_clean.append(task.get('zip_file'))
        
        for file_path in files_to_clean:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except OSError as e:
                    logger.error(f"Error removing file {file_path}: {e}")
        
        if task_id in self.active_tasks:
            del self.active_tasks[task_id]
            logger.info(f"Task {task_id} cleaned up and removed.")

# Initialize the scraper
scraper = ScraperAPI()

# ---- ASYNC TASK EXECUTION IN A THREAD ----
def run_async_task(task_id):
    """Sets up and runs the asyncio event loop for the scraping task."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(execute_scraping_task(task_id))
    loop.close()

async def execute_scraping_task(task_id):
    """The main async function that orchestrates the scraping process."""
    if task_id not in scraper.active_tasks: return
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
        
        # Create quiz IDs JSON file
        task['status'] = 'creating_json'
        quiz_ids_json = scraper.create_quiz_ids_json(task['creator_id'], quiz_ids, task_id)
        task['quiz_ids_json'] = quiz_ids_json
        
        task['status'] = 'scraping_quizzes'
        
        scraped_files = await scraper.scrape_quiz_batch(quiz_ids, task_id)
        task['result_files'] = scraped_files
        
        status_after_scrape = 'cancelled' if task.get('cancelled') else 'completed'

        if scraped_files and not task.get('cancelled'):
            task['status'] = 'zipping'
            zip_file = scraper.create_zip_file(task['creator_id'], scraped_files, task_id)
            task['zip_file'] = zip_file
        
        task['status'] = status_after_scrape
        if status_after_scrape == 'completed' and not task.get('error'):
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
    data = request.get_json()
    if not data or 'creator_id' not in data:
        return jsonify({"error": "creator_id is required"}), 400
    
    try:
        creator_id = str(data['creator_id'])
        page_num = int(data.get('page_num', 1))
        if not (1 <= page_num <= 1000):
            return jsonify({"error": "Page number must be between 1 and 1000"}), 400
    except (ValueError, TypeError):
        return jsonify({"error": "page_num must be a valid integer"}), 400
    
    task_id = uuid.uuid4().hex[:8]
    scraper.active_tasks[task_id] = {
        'creator_id': creator_id, 'page_num': page_num, 'status': 'starting',
        'processed': 0, 'total': 0, 'start_time': time.time(), 'last_update': time.time(),
        'cancelled': False, 'quiz_ids': [], 'result_files': [], 
        'quiz_ids_json': None, 'zip_file': None
    }
    
    thread = threading.Thread(target=run_async_task, args=(task_id,))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "task_id": task_id,
        "status": "started",
        "message": f"Scraping task started for creator {creator_id}",
        "details": {"pages": page_num, "batch_size": BATCH_SIZE}
    })

@app.route('/api/scrape/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    task = scraper.active_tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    
    progress = 0
    if task['total'] > 0:
        progress = round((task['processed'] / task['total']) * 100, 1)
    
    response = {
        "task_id": task_id,
        "status": task['status'],
        "progress": task['processed'],
        "total": task['total'],
        "progress_percentage": progress,
        "elapsed_time": round(time.time() - task['start_time'], 1),
        "creator_id": task['creator_id'],
        "quiz_ids_found": len(task.get('quiz_ids', [])),
        "batch_size": BATCH_SIZE
    }
    
    if task.get('quiz_ids_json') and os.path.exists(task['quiz_ids_json']):
        response['quiz_ids_ready'] = True
    
    if task.get('zip_file') and os.path.exists(task['zip_file']):
        response['download_ready'] = True
        response['file_size'] = os.path.getsize(task['zip_file'])
    
    if task.get('error'): response['error'] = task['error']
    
    return jsonify(response)

@app.route('/api/scrape/download/quiz-ids/<task_id>', methods=['GET'])
def download_quiz_ids(task_id):
    """Download the quiz IDs JSON file."""
    task = scraper.active_tasks.get(task_id)
    if not task or not task.get('quiz_ids_json'):
        return jsonify({"error": "Task not found or quiz IDs not ready"}), 404
    
    json_file = task['quiz_ids_json']
    if not os.path.exists(json_file):
        return jsonify({"error": "Quiz IDs file not found on server"}), 404
    
    return send_file(json_file, as_attachment=True, 
                    download_name=f"quiz_ids_{task['creator_id']}.json")

@app.route('/api/scrape/download/quizzes/<task_id>', methods=['GET'])
def download_quizzes(task_id):
    """Download the quizzes zip file."""
    task = scraper.active_tasks.get(task_id)
    if not task or not task.get('zip_file'):
        return jsonify({"error": "Task not found or results not ready"}), 404
    
    zip_file = task['zip_file']
    if not os.path.exists(zip_file):
        return jsonify({"error": "File not found on server, may have been cleaned up"}), 404
    
    return send_file(zip_file, as_attachment=True, 
                    download_name=f"quizzes_{task['creator_id']}.zip")

@app.route('/api/scrape/cancel/<task_id>', methods=['POST'])
def cancel_task(task_id):
    task = scraper.active_tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    
    if task['status'] not in ['starting', 'scraping_ids', 'creating_json', 'scraping_quizzes', 'zipping']:
        return jsonify({"message": "Task is already completed or cancelled"}), 400

    task['cancelled'] = True
    task['status'] = 'cancelling'
    return jsonify({"message": "Task cancellation requested."})

@app.route('/api/scrape/cleanup/<task_id>', methods=['POST'])
def cleanup_task_route(task_id):
    if task_id not in scraper.active_tasks:
        return jsonify({"error": "Task not found or already cleaned up"}), 404
    scraper.cleanup_task(task_id)
    return jsonify({"message": f"Task {task_id} cleaned up."})

@app.route('/api/scrape/list', methods=['GET'])
def list_tasks():
    active_tasks_summary = {}
    for task_id, task in list(scraper.active_tasks.items()):
        active_tasks_summary[task_id] = {
            "status": task.get('status'),
            "creator_id": task.get('creator_id'),
            "progress": task.get('processed'),
            "total": task.get('total'),
            "quiz_ids_found": len(task.get('quiz_ids', [])),
            "elapsed_time": round(time.time() - task.get('start_time', 0), 1)
        }
    return jsonify({"active_tasks": active_tasks_summary, "total_tasks": len(active_tasks_summary)})

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "message": "Scraper API is operational.",
        "endpoints": {
            "GET /api/health": "Health check",
            "POST /api/scrape/start": "Body: {'creator_id': str, 'page_num': int}",
            "GET /api/scrape/status/<task_id>": "Check task status",
            "GET /api/scrape/download/quiz-ids/<task_id>": "Download quiz IDs JSON",
            "GET /api/scrape/download/quizzes/<task_id>": "Download quizzes zip",
            "POST /api/scrape/cancel/<task_id>": "Cancel running task",
            "POST /api/scrape/cleanup/<task_id>": "Manually clean up a task's files",
            "GET /api/scrape/list": "List all active tasks"
        },
        "configuration": {
            "timeout": TIMEOUT,
            "batch_size": BATCH_SIZE,
            "question_workers": QUESTION_WORKERS
        }
    })

# --- Cleanup Scheduler ---
def cleanup_old_tasks():
    while True:
        time.sleep(600)
        try:
            current_time = time.time()
            tasks_to_remove = [
                task_id for task_id, task in list(scraper.active_tasks.items())
                if current_time - task.get('start_time', 0) > 36000
            ]
            if tasks_to_remove:
                logger.info(f"Auto-cleaning {len(tasks_to_remove)} old tasks.")
                for task_id in tasks_to_remove:
                    scraper.cleanup_task(task_id)
        except Exception as e:
            logger.error(f"Error in cleanup thread: {e}")

if __name__ == '__main__':
    cleanup_thread = threading.Thread(target=cleanup_old_tasks, daemon=True)
    cleanup_thread.start()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
