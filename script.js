:root {
    --primary-color: #7d30e9;
    --secondary-color: #5e27cc;
    --text-color: #333;
    --light-gray: #f4f4f8;
    --white: #ffffff;
    --error-color: #e74c3c;
    --success-color: #2ecc71;
    --warning-color: #f39c12;
}

* {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
}

body {
    font-family: 'Poppins', sans-serif;
    background: var(--light-gray);
    color: var(--text-color);
    line-height: 1.6;
    padding: 20px;
    min-height: 100vh;
}

.container {
    max-width: 1000px;
    margin: 0 auto;
    padding: 30px;
    background: var(--white);
    border-radius: 15px;
    box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
}

header {
    text-align: center;
    margin-bottom: 30px;
}

.title {
    color: var(--primary-color);
    font-size: 2.2rem;
    font-weight: 700;
    margin-bottom: 5px;
    background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.subtitle {
    color: var(--primary-color);
    font-size: 1rem;
    opacity: 0.8;
    font-weight: 400;
}

/* Flash messages */
.flash-messages {
    margin-bottom: 20px;
}

.flash-error {
    background: rgba(231, 76, 60, 0.1);
    color: var(--error-color);
    padding: 10px 15px;
    border-radius: 8px;
    margin-bottom: 10px;
    border-left: 4px solid var(--error-color);
}

.flash-success {
    background: rgba(46, 204, 113, 0.1);
    color: var(--success-color);
    padding: 10px 15px;
    border-radius: 8px;
    margin-bottom: 10px;
    border-left: 4px solid var(--success-color);
}

/* Dashboard styles */
.dashboard {
    display: flex;
    flex-direction: column;
    gap: 30px;
}

.user-info {
    text-align: center;
}

.user-info h2 {
    color: var(--primary-color);
    margin-bottom: 5px;
}

.user-info p {
    color: #666;
}

/* Upload section */
.upload-section {
    background: rgba(125, 48, 233, 0.05);
    padding: 20px;
    border-radius: 10px;
    border: 2px dashed rgba(125, 48, 233, 0.2);
}

.file-upload-label {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 15px;
    background: white;
    border-radius: 8px;
    cursor: pointer;
    transition: all 0.3s ease;
    border: 1px solid #ddd;
}

.file-upload-label:hover {
    border-color: var(--primary-color);
}

.file-upload-label i {
    color: var(--primary-color);
    font-size: 1.2rem;
}

.file-upload-label span {
    font-size: 0.9rem;
}

#file-upload {
    display: none;
}

.upload-btn {
    background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
    color: white;
    border: none;
    padding: 12px 20px;
    border-radius: 8px;
    font-size: 1rem;
    font-weight: 500;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 10px;
    transition: all 0.3s ease;
    margin-top: 15px;
    width: 100%;
}

.upload-btn:hover {
    transform: translateY(-2px);
    box-shadow: 0 5px 15px rgba(125, 48, 233, 0.3);
}

/* File list */
.file-list {
    background: white;
    border-radius: 10px;
    padding: 20px;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
}

.file-list-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
}

.file-list-header h3 {
    color: var(--primary-color);
}

.file-list-header span {
    background: rgba(125, 48, 233, 0.1);
    color: var(--primary-color);
    padding: 5px 10px;
    border-radius: 20px;
    font-size: 0.8rem;
    font-weight: 500;
}

.files-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
    gap: 15px;
}

.file-card {
    background: white;
    border-radius: 8px;
    padding: 15px;
    border: 1px solid #eee;
    transition: all 0.3s ease;
}

.file-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
}

.file-card.running {
    border-left: 4px solid var(--success-color);
}

.file-card.stopped {
    border-left: 4px solid #ddd;
}

.file-info h4 {
    font-size: 1rem;
    margin-bottom: 5px;
    color: var(--text-color);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.file-type {
    background: rgba(125, 48, 233, 0.1);
    color: var(--primary-color);
    padding: 3px 8px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 500;
}

.status-badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 0.8rem;
    padding: 3px 8px;
    border-radius: 4px;
}

.status-badge.running {
    background: rgba(46, 204, 113, 0.1);
    color: var(--success-color);
}

.status-badge.stopped {
    background: rgba(231, 76, 60, 0.1);
    color: var(--error-color);
}

.status-badge i {
    font-size: 0.6rem;
}

.file-actions {
    display: flex;
    gap: 8px;
    margin-top: 15px;
    flex-wrap: wrap;
}

.btn {
    padding: 6px 12px;
    border-radius: 6px;
    font-size: 0.8rem;
    display: flex;
    align-items: center;
    gap: 5px;
    cursor: pointer;
    transition: all 0.2s ease;
    border: none;
}

.start-btn {
    background: rgba(46, 204, 113, 0.1);
    color: var(--success-color);
}

.start-btn:hover {
    background: rgba(46, 204, 113, 0.2);
}

.stop-btn {
    background: rgba(231, 76, 60, 0.1);
    color: var(--error-color);
}

.stop-btn:hover {
    background: rgba(231, 76, 60, 0.2);
}

.logs-btn {
    background: rgba(52, 152, 219, 0.1);
    color: #3498db;
}

.logs-btn:hover {
    background: rgba(52, 152, 219, 0.2);
}

.delete-btn {
    background: rgba(231, 76, 60, 0.1);
    color: var(--error-color);
}

.delete-btn:hover {
    background: rgba(231, 76, 60, 0.2);
}

.no-files {
    text-align: center;
    padding: 40px 20px;
    color: #999;
}

.no-files i {
    font-size: 2rem;
    margin-bottom: 10px;
    color: #ddd;
}

/* Auth pages */
.auth-container {
    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 70vh;
}

.auth-card {
    background: white;
    border-radius: 10px;
    padding: 30px;
    width: 100%;
    max-width: 400px;
    box-shadow: 0 5px 15px rgba(0, 0, 0, 0.05);
}

.auth-card h2 {
    color: var(--primary-color);
    text-align: center;
    margin-bottom: 25px;
    font-size: 1.5rem;
}

.form-group {
    margin-bottom: 20px;
}

.form-group label {
    display: block;
    margin-bottom: 8px;
    font-size: 0.9rem;
    color: #555;
}

.form-group input {
    width: 100%;
    padding: 12px 15px;
    border: 1px solid #ddd;
    border-radius: 8px;
    font-size: 1rem;
    font-family: 'Poppins', sans-serif;
    transition: all 0.3s ease;
}

.form-group input:focus {
    outline: none;
    border-color: var(--primary-color);
    box-shadow: 0 0 0 3px rgba(125, 48, 233, 0.2);
}

.auth-btn {
    background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
    color: white;
    border: none;
    padding: 12px;
    border-radius: 8px;
    font-size: 1rem;
    font-weight: 500;
    cursor: pointer;
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 10px;
    transition: all 0.3s ease;
}

.auth-btn:hover {
    transform: translateY(-2px);
    box-shadow: 0 5px 15px rgba(125, 48, 233, 0.3);
}

.auth-footer {
    text-align: center;
    margin-top: 20px;
    font-size: 0.9rem;
    color: #666;
}

.auth-footer a {
    color: var(--primary-color);
    text-decoration: none;
    font-weight: 500;
}

/* Footer */
footer {
    text-align: center;
    margin-top: 40px;
    color: #666;
    font-size: 0.85rem;
    padding-top: 20px;
    border-top: 1px solid #eee;
}

/* Responsive */
@media (max-width: 768px) {
    .container {
        padding: 20px;
    }
    
    .title {
        font-size: 1.8rem;
    }
    
    .files-grid {
        grid-template-columns: 1fr;
    }
}

@media (max-width: 480px) {
    body {
        padding: 10px;
    }
    
    .title {
        font-size: 1.5rem;
    }
    
    .subtitle {
        font-size: 0.9rem;
    }
    
    .auth-card {
        padding: 20px;
    }
}