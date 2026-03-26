exports.handler = async () => ({
  statusCode: 200,
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    status: 'ok',
    meta: !!process.env.META_ACCESS_TOKEN,
    hubspot: !!process.env.HUBSPOT_ACCESS_TOKEN,
    password_protected: !!process.env.DASHBOARD_PASSWORD
  })
});
