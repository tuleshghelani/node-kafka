const express = require('express');
const { body } = require('express-validator');
const router = express.Router();
const authMiddleware = require('../middleware/auth');
const {
  createContact,
  getAllContacts,
  updateContactStatus
} = require('../controllers/contactController');

// Validation middleware
const validateContact = [
  body('full_name').notEmpty().trim().escape(),
  body('subject').notEmpty().trim().escape(),
  body('mobilenumber').optional().trim(),
  body('message').optional().trim().escape()
];

// Public route
router.post('/', validateContact, createContact);

// Protected routes
router.use(authMiddleware);
router.post('/getAllContacts/', getAllContacts);
router.put('/updateStatus/', updateContactStatus);

module.exports = router; 