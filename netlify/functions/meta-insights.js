const fetch = require('node-fetch');

exports.handler = async (event) => {
  const headers = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };

  const password = process.env.DASHBOARD_PASSWORD;
  if (password && event.headers['x-dashboard-password'] !== password) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const token = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_AD_ACCOUNT_ID;
  if (!token || !accountId) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: 'Meta credentials not set.' }) };
  }

  const days = parseInt(event.queryStringParameters?.days) || 30;
  const since = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];
  const until = new Date().toISOString().split('T')[0];

  try {
    const url = `https://graph.facebook.com/v18.0/act_${accountId}/insights?fields=campaign_name,campaign_id,adset_name,adset_id,ad_name,ad_id,spend,impressions,clicks,ctr,cpc,frequency&time_range={"since":"${since}","until":"${until}"}&level=ad&limit=200&access_token=${token}`;
    const r = await fetch(url);
    const data = await r.json();
    if (data.error) return { statusCode: 400, headers, body: JSON.stringify({ error: data.error.message }) };
    return { statusCode: 200, headers, body: JSON.stringify(data) };
  } catch (e) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: e.message }) };
  }
};
