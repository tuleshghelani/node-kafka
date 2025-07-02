const bcrypt = require('bcryptjs');
const pool = require('../config/database');
const { validationResult } = require('express-validator');
const { Kafka } = require('kafkajs');
require('dotenv').config();

const register = async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { username, password, email } = req.body;
    // Check if user already exists
    const userExists = await pool.query('SELECT * FROM users WHERE email = $1', [email]);
    if (userExists.rows.length > 0) {
      return res.status(400).json({ message: 'User already exists' });
    }
    // Hash password
    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(password, salt);
    // Create user
    const result = await pool.query(
      'INSERT INTO users (username, email, password) VALUES ($1, $2, $3) RETURNING id, username, email',
      [username, email, hashedPassword]
    );
    res.status(201).json({
      user: result.rows[0]
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

const login = async (req, res) => {
  try {
    const errors = validationResult(req);
    if (!errors.isEmpty()) {
      return res.status(400).json({ errors: errors.array() });
    }

    const { email, password } = req.body;
    // Check if user exists
    const result = await pool.query('SELECT * FROM users WHERE email = $1', [email]);
    if (result.rows.length === 0) {
      return res.status(401).json({ message: 'Invalid credentials' });
    }
    const user = result.rows[0];
    // Verify password
    const isValidPassword = await bcrypt.compare(password, user.password);
    if (!isValidPassword) {
      return res.status(401).json({ message: 'Invalid credentials' });
    }
    res.json({
      user: {
        id: user.id,
        username: user.username,
        email: user.email
      }
    });
  } catch (error) {
    res.status(500).json({ message: 'Server error', error: error.message });
  }
};

const consumeKafkaMessage = async (req, res) => {
  const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'node-kafka-client',
    brokers: [process.env.KAFKA_BOOTSTRAP_SERVERS],
    ssl: true,
    sasl: {
      mechanism: process.env.KAFKA_SASL_MECHANISM,
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD,
    },
  });

  const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID,
  });

  try {
    await consumer.connect();
    await consumer.subscribe({ topic: process.env.KAFKA_TOPIC, fromBeginning: process.env.KAFKA_AUTO_OFFSET_RESET === 'earliest' });

    let messageConsumed = null;
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        messageConsumed = {
          topic,
          partition,
          offset: message.offset,
          value: message.value.toString(),
        };
        // Stop after first message
        await consumer.stop();
      },
    });

    // Wait a short time for message to be consumed
    await new Promise(resolve => setTimeout(resolve, 2000));
    await consumer.disconnect();

    if (messageConsumed) {
      res.json({ message: messageConsumed });
    } else {
      res.status(404).json({ message: 'No message found in topic.' });
    }
  } catch (error) {
    await consumer.disconnect();
    res.status(500).json({ message: 'Kafka error', error: error.message });
  }
};

module.exports = {
  register,
  login,
  consumeKafkaMessage
};