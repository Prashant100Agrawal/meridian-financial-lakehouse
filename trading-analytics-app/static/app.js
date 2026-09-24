// Chat functionality
const chatContainer = document.getElementById('chatContainer');
const questionInput = document.getElementById('questionInput');
const sendButton = document.getElementById('sendButton');
const loading = document.getElementById('loading');

// Add message to chat
function addMessage(text, isUser) {
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${isUser ? 'user' : 'agent'}`;
    
    const contentDiv = document.createElement('div');
    contentDiv.className = 'message-content';
    contentDiv.textContent = text;
    
    messageDiv.appendChild(contentDiv);
    chatContainer.appendChild(messageDiv);
    
    // Scroll to bottom
    chatContainer.scrollTop = chatContainer.scrollHeight;
}

// Add error message
function addError(text) {
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-message';
    errorDiv.textContent = `Error: ${text}`;
    chatContainer.appendChild(errorDiv);
    chatContainer.scrollTop = chatContainer.scrollHeight;
}

// Send question to backend
async function askQuestion() {
    const question = questionInput.value.trim();
    
    if (!question) {
        return;
    }
    
    // Clear input and disable button
    questionInput.value = '';
    sendButton.disabled = true;
    loading.style.display = 'flex';
    
    // Add user message
    addMessage(question, true);
    
    try {
        const response = await fetch('/api/ask', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ question })
        });
        
        const data = await response.json();
        
        if (data.status === 'success') {
            addMessage(data.answer, false);
        } else {
            addError(data.error || 'Unknown error occurred');
        }
    } catch (error) {
        addError(`Failed to connect: ${error.message}`);
    } finally {
        sendButton.disabled = false;
        loading.style.display = 'none';
        questionInput.focus();
    }
}

// Event listeners
sendButton.addEventListener('click', askQuestion);

questionInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        askQuestion();
    }
});

// Focus input on load
questionInput.focus();
