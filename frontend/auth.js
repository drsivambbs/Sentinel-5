// Firebase Auth Module - Include this in all protected pages
import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js';
import { getAuth, onAuthStateChanged, signOut } from 'https://www.gstatic.com/firebasejs/10.7.1/firebase-auth.js';

const firebaseConfig = {
    apiKey: "AIzaSyAoVEja49Np2SxbEpj9dtrwzKXBFrlh4J8",
    authDomain: "sentinel-h-5.firebaseapp.com",
    projectId: "sentinel-h-5",
    storageBucket: "sentinel-h-5.firebasestorage.app",
    messagingSenderId: "196547645490",
    appId: "1:196547645490:web:00bf01c40de5776e4aac2b"
};

const app = initializeApp(firebaseConfig);
const auth = getAuth(app);

// Check authentication on page load
onAuthStateChanged(auth, (user) => {
    if (!user) {
        window.location.href = 'login.html';
    } else {
        // Update user display if element exists
        const userDisplay = document.getElementById('userIdDisplay');
        if (userDisplay) {
            userDisplay.textContent = user.email;
        }
    }
});

// Logout function
window.logout = async function() {
    try {
        await signOut(auth);
        window.location.href = 'login.html';
    } catch (error) {
        console.error('Logout error:', error);
    }
};

// Get current user token for API calls
window.getAuthToken = async function() {
    const user = auth.currentUser;
    if (user) {
        return await user.getIdToken();
    }
    return null;
};

export { auth };