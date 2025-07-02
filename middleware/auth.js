// JWT authentication middleware removed as per request.
module.exports = (req, res, next) => {
  // No authentication enforced
  next();
};