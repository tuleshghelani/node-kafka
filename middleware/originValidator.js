const validateOrigin = (req, res, next) => {
//   const allowedOrigin = process.env.ALLOWED_ORIGIN;
//   const requestOrigin = req.get('origin');

//   console.log('Request Origin:', requestOrigin);
//   console.log('Allowed Origins:', allowedOrigin);

//   if (!allowedOrigin) {
//     console.warn('ALLOWED_ORIGIN is not set in environment variables');
//     return next();
//   }

//   const allowedOrigins = allowedOrigin.split(',').map(origin => origin.trim());
  
//   if (!requestOrigin || allowedOrigins.some(allowed => 
//     requestOrigin.startsWith(allowed) || allowed.startsWith(requestOrigin)
//   )) {
//     // Set specific CORS headers for email routes
//     res.header('Access-Control-Allow-Origin', requestOrigin);
//     res.header('Access-Control-Allow-Methods', 'POST');
//     res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
//     res.header('Access-Control-Allow-Credentials', 'true');
//     next();
//   } else {
//     console.error(`Blocked request from unauthorized origin: ${requestOrigin}`);
//     return res.status(403).json({
//       success: false,
//       message: 'Access forbidden: Invalid origin'
//     });
//   }
};

module.exports = validateOrigin; 