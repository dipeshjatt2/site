from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import os
import zipfile
import time
import json
from pathlib import Path
import logging
from datetime import datetime
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Configuration
BATCH_SIZE = 5
QUESTION_WORKERS = 10
MAX_WORKERS = 20
TIMEOUT = 30

class ScraperAPI:
    def __init__(self):
        self.active_tasks = {}
        self.base_url = "https://testnookapp-f602da876a9b.herokuapp.com"
        
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
        
        self.post_headers = {
            **self.headers,
            'Accept': '*/*',
            'Content-Type': 'application/json',
            'Origin': self.base_url,
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
        }

    def sanitize_filename(self, name):
        """Removes invalid characters from a string to make it a valid filename."""
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
            
            results = await asyncio.gather(*tasks)
            
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
                if file_path:
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
                # Check if quiz is accessible
                first_q_url = f"{self.base_url}/quiz/{quiz_id}/question/0"
                async with session.get(first_q_url, timeout=TIMEOUT) as response:
                    response_text = await response.text()
                    
                    if "Quiz Complete" in response_text:
                        with open(output_filename, 'w', encoding='utf-8') as f:
                            f.write("Quiz is empty or already complete.")
                        return output_filename
                    
                    # Get total questions
                    soup = BeautifulSoup(response_text, 'html.parser')
                    progress_span = soup.find('div', class_='question-progress')
                    if progress_span:
                        progress_text = progress_span.find('span').get_text(strip=True) if progress_span.find('span') else ""
                        if '/' in progress_text:
                            total_questions = int(progress_text.split('/')[1])
                        else:
                            total_questions = 1
                    else:
                        total_questions = 1
                
                # Fetch all questions
                question_tasks = []
                for q_num in range(total_questions):
                    question_tasks.append(self.fetch_and_solve_question(session, quiz_id, q_num))
                
                question_results = await asyncio.gather(*question_tasks)
                
                # Sort results by question number and write to file
                sorted_results = sorted([r for r in question_results if r], key=lambda x: x['q_num'])
                
                with open(output_filename, 'w', encoding='utf-8') as f:
                    for result in sorted_results:
                        if "error" in result:
                            f.write(f"{result['q_num'] + 1}. FAILED TO FETCH QUESTION: {result['error']}\n\n")
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
            # Write error file
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(f"Error scraping quiz: {str(e)}")
            return output_filename

    async def fetch_and_solve_question(self, session, quiz_id, q_num):
        """Fetches and processes a single question."""
        try:
            q_url = f"{self.base_url}/quiz/{quiz_id}/question/{q_num}"
            async with session.get(q_url, timeout=TIMEOUT) as response:
                response.raise_for_status()
                html = await response.text()
            
            soup = BeautifulSoup(html, 'html.parser')
            
            question_text_elem = soup.find('div', class_='question-text')
            question_text = question_text_elem.get_text(strip=True) if question_text_elem else "Unknown Question"
            
            options = []
            option_elements = soup.find_all('div', class_='option')
            for opt in option_elements:
                options.append(opt.get_text(strip=True))
            
            # Get correct answer
            answer_url = f"{self.base_url}/quiz/{quiz_id}/answer"
            post_headers_with_ref = {**self.post_headers, 'Referer': q_url}
            payload = {"question_num": q_num, "selected_option": 0}
            
            async with session.post(answer_url, headers=post_headers_with_ref, json=payload, timeout=TIMEOUT) as resp:
                answer_data = await resp.json()
            
            if not answer_data.get('success'):
                raise Exception("Failed to get answer from server")
            
            return {
                "q_num": q_num,
                "text": question_text,
                "options": options,
                "correct_index": answer_data['correct_option']
            }
            
        except Exception as e:
            return {"q_num": q_num, "error": str(e)}

    def create_zip_file(self, creator_id, scraped_files, task_id):
        """Creates a zip file with all scraped quizzes."""
        zip_filename = f"temp_{task_id}_{creator_id}_quizzes.zip"
        
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in scraped_files:
                if os.path.exists(file):
                    zipf.write(file, os.path.basename(file))
        
        # Clean up individual files
        for file in scraped_files:
            try:
                os.remove(file)
            except:
                pass
        
        return zip_filename

    def cleanup_task(self, task_id):
        """Clean up files for a task."""
        if task_id in self.active_tasks:
            # Clean up any remaining files
            for file in os.listdir('.'):
                if file.startswith(f"temp_{task_id}"):
                    try:
                        os.remove(file)
                    except:
                        pass
            del self.active_tasks[task_id]

# Initialize the scraper
scraper = ScraperAPI()

# API Routes
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_tasks": len(scraper.active_tasks)
    })

@app.route('/api/scrape/start', methods=['POST'])
async def start_scraping():
    """Start a new scraping task."""
    data = request.json
    creator_id = data.get('creator_id')
    page_num = data.get('page_num', 1)
    workers = data.get('workers', 5)
    
    if not creator_id or not creator_id.isdigit():
        return jsonify({"error": "Invalid creator ID"}), 400
    
    if page_num < 1:
        return jsonify({"error": "Page number must be at least 1"}), 400
    
    if workers < 1 or workers > MAX_WORKERS:
        return jsonify({"error": f"Workers must be between 1 and {MAX_WORKERS}"}), 400
    
    # Generate task ID
    task_id = uuid.uuid4().hex
    
    # Initialize task
    scraper.active_tasks[task_id] = {
        'creator_id': creator_id,
        'page_num': page_num,
        'workers': workers,
        'status': 'starting',
        'progress': 0,
        'total': 0,
        'processed': 0,
        'start_time': time.time(),
        'last_update': time.time(),
        'cancelled': False,
        'quiz_ids': [],
        'result_files': []
    }
    
    # Start scraping in background
    asyncio.create_task(execute_scraping_task(task_id))
    
    return jsonify({
        "task_id": task_id,
        "status": "started",
        "message": f"Scraping task started for creator {creator_id}"
    })

async def execute_scraping_task(task_id):
    """Execute the scraping task."""
    if task_id not in scraper.active_tasks:
        return
    
    task = scraper.active_tasks[task_id]
    creator_id = task['creator_id']
    page_num = task['page_num']
    
    try:
        # Step 1: Scrape quiz IDs
        task['status'] = 'scraping_ids'
        quiz_ids = await scraper.scrape_quiz_ids(creator_id, page_num)
        
        if task.get('cancelled'):
            task['status'] = 'cancelled'
            scraper.cleanup_task(task_id)
            return
        
        if not quiz_ids:
            task['status'] = 'completed'
            task['error'] = "No quiz IDs found"
            return
        
        task['quiz_ids'] = quiz_ids
        task['total'] = len(quiz_ids)
        
        # Step 2: Scrape quizzes in batches
        task['status'] = 'scraping_quizzes'
        batch_size = min(BATCH_SIZE, task['workers'])
        
        for i in range(0, len(quiz_ids), batch_size):
            if task.get('cancelled'):
                break
                
            batch = quiz_ids[i:i + batch_size]
            scraped_files = await scraper.scrape_quiz_batch(batch, task_id)
            task['result_files'].extend(scraped_files)
        
        if task.get('cancelled'):
            task['status'] = 'cancelled'
            # Create partial results zip
            if task['result_files']:
                zip_file = scraper.create_zip_file(creator_id, task['result_files'], task_id)
                task['zip_file'] = zip_file
        else:
            # Create final zip
            task['status'] = 'completed'
            if task['result_files']:
                zip_file = scraper.create_zip_file(creator_id, task['result_files'], task_id)
                task['zip_file'] = zip_file
            task['progress'] = 100
        
    except Exception as e:
        logger.error(f"Error in task {task_id}: {e}")
        task['status'] = 'error'
        task['error'] = str(e)
        scraper.cleanup_task(task_id)

@app.route('/api/scrape/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """Get the status of a scraping task."""
    if task_id not in scraper.active_tasks:
        return jsonify({"error": "Task not found"}), 404
    
    task = scraper.active_tasks[task_id]
    
    response = {
        "task_id": task_id,
        "status": task['status'],
        "progress": task['processed'],
        "total": task['total'],
        "progress_percentage": round((task['processed'] / task['total']) * 100, 1) if task['total'] > 0 else 0,
        "elapsed_time": round(time.time() - task['start_time'], 1)
    }
    
    if task['status'] == 'completed' and 'zip_file' in task:
        response['download_ready'] = True
        response['zip_file'] = task['zip_file']
    
    if task['status'] == 'error':
        response['error'] = task.get('error', 'Unknown error')
    
    return jsonify(response)

@app.route('/api/scrape/download/<task_id>', methods=['GET'])
def download_results(task_id):
    """Download the results zip file."""
    if task_id not in scraper.active_tasks:
        return jsonify({"error": "Task not found"}), 404
    
    task = scraper.active_tasks[task_id]
    
    if task['status'] != 'completed' or 'zip_file' not in task:
        return jsonify({"error": "Results not ready"}), 400
    
    if not os.path.exists(task['zip_file']):
        return jsonify({"error": "File not found"}), 404
    
    return send_file(
        task['zip_file'],
        as_attachment=True,
        download_name=f"creator_{task['creator_id']}_quizzes.zip",
        mimetype='application/zip'
    )

@app.route('/api/scrape/cancel/<task_id>', methods=['POST'])
def cancel_task(task_id):
    """Cancel a scraping task."""
    if task_id not in scraper.active_tasks:
        return jsonify({"error": "Task not found"}), 404
    
    task = scraper.active_tasks[task_id]
    task['cancelled'] = True
    
    return jsonify({
        "task_id": task_id,
        "status": "cancellation_requested",
        "message": "Task cancellation requested"
    })

@app.route('/api/scrape/cleanup/<task_id>', methods=['POST'])
def cleanup_task(task_id):
    """Clean up a completed task."""
    scraper.cleanup_task(task_id)
    return jsonify({"message": "Task cleaned up"})

@app.route('/api/scrape/list', methods=['GET'])
def list_tasks():
    """List all active tasks."""
    tasks = {}
    for task_id, task in scraper.active_tasks.items():
        tasks[task_id] = {
            "status": task['status'],
            "creator_id": task['creator_id'],
            "progress": task['processed'],
            "total": task['total'],
            "elapsed_time": round(time.time() - task['start_time'], 1)
        }
    
    return jsonify({"active_tasks": tasks})

# Cleanup old tasks periodically
import atexit
import threading

def cleanup_old_tasks():
    """Clean up tasks older than 1 hour."""
    current_time = time.time()
    tasks_to_remove = []
    
    for task_id, task in scraper.active_tasks.items():
        if current_time - task['start_time'] > 3600:  # 1 hour
            tasks_to_remove.append(task_id)
    
    for task_id in tasks_to_remove:
        scraper.cleanup_task(task_id)

def schedule_cleanup():
    """Schedule periodic cleanup."""
    cleanup_old_tasks()
    threading.Timer(300, schedule_cleanup).start()  # Run every 5 minutes

# Start cleanup scheduler when app starts
schedule_cleanup()

# Register cleanup on exit
atexit.register(lambda: [scraper.cleanup_task(task_id) for task_id in list(scraper.active_tasks.keys())])

if __name__ == '__main__':
    # Create temp directory if it doesn't exist
    if not os.path.exists('temp'):
        os.makedirs('temp')
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
