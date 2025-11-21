const express = require('express');
const multer = require('multer');
const { Storage } = require('@google-cloud/storage');
const cors = require('cors');
require('dotenv').config();

const app = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(cors());
app.use(express.json());

const storage = new Storage({
  projectId: process.env.PROJECT_NAME
});

const bucket = storage.bucket(process.env.BUCKET_NAME);

app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const now = new Date();
    const timestamp = now.toISOString().replace(/[:.]/g, '-');
    const fileName = `${req.file.originalname.split('.')[0]}_${timestamp}.csv`;
    const filePath = `${process.env.FOLDER_NAME}/${fileName}`;

    const file = bucket.file(filePath);
    const stream = file.createWriteStream({
      metadata: {
        contentType: req.file.mimetype,
      },
    });

    stream.on('error', (err) => {
      res.status(500).json({ error: err.message });
    });

    stream.on('finish', () => {
      res.json({ 
        message: 'File uploaded successfully',
        fileName: fileName,
        path: filePath
      });
    });

    stream.end(req.file.buffer);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});