const express = require('express');
const { body } = require('express-validator');
const router = express.Router();
const authMiddleware = require('../middleware/auth');
const {
  createEnquiry,
  getAllEnquiries,
  getEnquiryById,
  updateEnquiry,
  deleteEnquiry,
  updateEnquiryStatus
} = require('../controllers/enquiryController');

// Validation middleware
const validateEnquiry = [
  body('name').notEmpty().trim().escape(),
  body('mobilenumber').notEmpty().trim(),
  body('company_name').optional().trim().escape(),
  body('address').optional().trim().escape()
];

// Public route - Create enquiry
router.post('/', validateEnquiry, createEnquiry);
// router.post('/hello', (req, res) => {
//   res.send('Hello World');
// });

// Protected routes
router.use(authMiddleware);
router.put('/updateStatus', updateEnquiryStatus);
router.post('/getAllEnquiries/', getAllEnquiries);
router.get('/:id', getEnquiryById);
router.put('/:id', validateEnquiry, updateEnquiry);
router.delete('/:id', deleteEnquiry);

module.exports = router;