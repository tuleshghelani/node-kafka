const pool = require('../config/database');
const { validationResult } = require('express-validator');

// Create contact
const createContact = async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { full_name, email, subject, mobilenumber, message, company_name, designation, country } = req.body;
    
    const result = await pool.query(
      'INSERT INTO contact (full_name, email, subject, mobilenumber, message, company_name, designation, country) ' +
      ' VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *',
      [full_name, email, subject, mobilenumber || null, message || null, company_name || null, designation || null, country || null]
    );

    res.status(201).json({message : "Contact us sent successfully"});
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Get all contacts with pagination
const getAllContacts = async (req, res) => {
  try {
    const page = parseInt(req.body.page) || 1;
    const pageSize = parseInt(req.body.pageSize) || 10;
    const offset = (page - 1) * pageSize;

    const countResult = await pool.query('SELECT COUNT(*) FROM contact');
    const totalRecords = parseInt(countResult.rows[0].count);

    const result = await pool.query(
      'SELECT * FROM contact ORDER BY id DESC LIMIT $1 OFFSET $2',
      [pageSize, offset]
    );

    const totalPages = Math.ceil(totalRecords / pageSize);

    res.json({
      data: result.rows,
      pagination: {
        currentPage: page,
        pageSize: pageSize,
        totalRecords: totalRecords,
        totalPages: totalPages,
        hasNext: page < totalPages,
        hasPrevious: page > 1
      }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

// Update contact status
const updateContactStatus = async (req, res) => {
  try {
    const { id } = req.body;
    const { status } = req.body;

    // Validate status
    if (!['C', 'D', 'P'].includes(status)) {
      return res.status(400).json({ message: 'Invalid status. Use C or D' });
    }

    const result = await pool.query(
      'UPDATE contact SET status = $1 WHERE id = $2 RETURNING *',
      [status, id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Contact not found or already processed' });
    }

    res.json({message : "Contact status updated successfully"});
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

module.exports = {
  createContact,
  getAllContacts,
  updateContactStatus
}; 