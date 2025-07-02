const pool = require('../config/database');
const { validationResult } = require('express-validator');

const createEnquiry = async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { name, email, mobilenumber, petName, typeOfPet, petAge, likeToBuy } = req.body;
    
    const result = await pool.query(
      'INSERT INTO enquiry_master (name, email, mobilenumber, pet_name, type_of_pet, pet_age, like_to_buy, status) ' +
      ' VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *',
      [name, email, mobilenumber, petName, typeOfPet, petAge, likeToBuy, 'P']
    );

    res.status(201).json({message : "Enquiry sent successfully"});
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

const getAllEnquiries = async (req, res) => {
  try {
    const page = parseInt(req.body.page) || 1;
    const pageSize = parseInt(req.body.pageSize) || 10;
    const offset = (page - 1) * pageSize;

    const countResult = await pool.query('SELECT COUNT(*) FROM enquiry_master');
    const totalRecords = parseInt(countResult.rows[0].count);

    const result = await pool.query(
      'SELECT * FROM enquiry_master ORDER BY id DESC LIMIT $1 OFFSET $2',
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

const getEnquiryById = async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query('SELECT * FROM enquiry_master WHERE id = $1', [id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Enquiry not found' });
    }
    
    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

const updateEnquiry = async (req, res) => {
  try {
    const { id } = req.params;
    const { name, company_name, email, mobilenumber, address } = req.body;
    
    const result = await pool.query(
      'UPDATE enquiry_master SET name = $1, company_name = $2, email = $3, mobilenumber = $4, address = $5 WHERE id = $6 RETURNING *',
      [name, company_name, email, mobilenumber, address, id]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Enquiry not found' });
    }
    
    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

const deleteEnquiry = async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query('DELETE FROM enquiry_master WHERE id = $1 RETURNING *', [id]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Enquiry not found' });
    }
    
    res.json({ message: 'Enquiry deleted successfully' });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

const updateEnquiryStatus = async (req, res) => {
  try {
    const { id } = req.body;
    const { status } = req.body;

    // Validate status
    if (!['C', 'D', 'P'].includes(status)) {
      return res.status(400).json({ message: 'Invalid status. Use C, D, or P' });
    }

    const result = await pool.query(
      'UPDATE enquiry_master SET status = $1 WHERE id = $2 RETURNING *',
      [status, id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: 'Enquiry not found' });
    }

    res.json({ message: 'Enquiry status updated successfully' });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

module.exports = {
  createEnquiry,
  getAllEnquiries,
  getEnquiryById,
  updateEnquiry,
  deleteEnquiry,
  updateEnquiryStatus
};