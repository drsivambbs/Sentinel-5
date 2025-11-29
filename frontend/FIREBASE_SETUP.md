# Firebase Authentication Setup

## 1. Update Firebase Config

Replace the placeholder config in `auth.js` and `login.html` with your actual Firebase config:

```javascript
const firebaseConfig = {
    apiKey: "YOUR_ACTUAL_API_KEY",
    authDomain: "sentinel-h-5.firebaseapp.com",
    projectId: "sentinel-h-5",
    storageBucket: "sentinel-h-5.appspot.com",
    messagingSenderId: "YOUR_MESSAGING_SENDER_ID",
    appId: "YOUR_APP_ID"
};
```

## 2. Enable Authentication in Firebase Console

1. Go to [Firebase Console](https://console.firebase.google.com/)
2. Select your `sentinel-h-5` project
3. Go to Authentication > Sign-in method
4. Enable "Email/Password" provider

## 3. Create User Accounts

In Firebase Console > Authentication > Users:
- Click "Add user"
- Enter email and password for each user

## 4. Test Login

1. Open `login.html` in browser
2. Use the email/password you created
3. Should redirect to `index.html` after successful login

## 5. Security Features

✅ **All pages protected** - Redirects to login if not authenticated  
✅ **Auto logout** - Logout button in sidebar  
✅ **Token-based** - Firebase handles secure tokens  
✅ **Session management** - Stays logged in until logout  

## Files Modified

- `login.html` - New login page
- `auth.js` - Authentication module
- `index.html` - Added auth protection + logout
- `data-import.html` - Added auth protection  
- `clusters-view.html` - Added auth protection
- `cluster-engine.html` - Added auth protection

## Next Steps (Optional)

- Protect your backend APIs with Firebase Admin SDK
- Add user roles/permissions
- Add password reset functionality