const express = require('express');
const { body } = require('express-validator');
const router = express.Router();
const { register, login } = require('../controllers/authController');

// Validation middleware
const validateRegistration = [
  body('username').notEmpty().trim().escape(),
  body('email').isEmail().normalizeEmail(),
  body('password').isLength({ min: 6 })
];

const validateLogin = [
  body('email').isEmail().normalizeEmail(),
  body('password').notEmpty()
];

router.post('/register', validateRegistration, register);
router.post('/login', validateLogin, login);
router.get('/hello', (req, res) => {
  res.send('Hello World');
});

module.exports = router;