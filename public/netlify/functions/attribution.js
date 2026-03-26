const fetch = require('node-fetch');

const META_BASE = 'https://graph.facebook.com/v18.0';
const HUBSPOT_BASE = 'https://api.hubapi.com';

exports.handler = async (event) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Content-Type': 'application/json'
  };

  // Password check
  const password = process.env.DASHBOARD_PASSWORD;
  if (password && event.headers['x-dashboard-password'] !== password) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const metaToken = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_AD_ACCOUNT_ID;
  const hsToken = process.env.HUBSPOT_ACCESS_TOKEN;

  if (!metaToken || !accountId || !hsToken) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: 'Missing server credentials. Set META_ACCESS_TOKEN, META_AD_ACCOUNT_ID, and HUBSPOT_ACCESS_TOKEN in Netlify environment variables.' }) };
  }

  const days = parseInt(event.queryStringParameters?.days) || 30;
  const since = new Date(Date.now() - days * 86400000).toISOString().split('T')[0];
  const until = new Date().toISOString().split('T')[0];

  try {
    // ── Meta campaign insights ──
    const insightsUrl = `${META_BASE}/act_${accountId}/insights?fields=campaign_name,campaign_id,spend,impressions,clicks,ctr,cpc,frequency&time_range={"since":"${since}","until":"${until}"}&level=campaign&limit=100&access_token=${metaToken}`;
    const metaR = await fetch(insightsUrl);
    const metaData = await metaR.json();
    if (metaData.error) return { statusCode: 400, headers, body: JSON.stringify({ error: 'Meta: ' + metaData.error.message }) };

    // ── HubSpot contacts ──
    const properties = 'firstname,lastname,email,createdate,lifecyclestage,hs_latest_source_data_1,hs_latest_source_data_2,hs_analytics_source_data_1,hs_analytics_source_data_2,utm_campaign,utm_source,utm_medium,utm_content,utm_term,country';
    let allContacts = [], after = null, page = 0;
    do {
      const url = `${HUBSPOT_BASE}/crm/v3/objects/contacts?limit=100&properties=${properties}${after ? `&after=${after}` : ''}`;
      const r = await fetch(url, { headers: { Authorization: `Bearer ${hsToken}` } });
      const data = await r.json();
      if (data.status === 'error') return { statusCode: 400, headers, body: JSON.stringify({ error: 'HubSpot: ' + data.message }) };
      allContacts = allContacts.concat(data.results || []);
      after = data.paging?.next?.after || null;
      page++;
    } while (after && page < 10);

    // Filter to window
    const sinceDate = new Date(Date.now() - days * 86400000);
    const windowContacts = allContacts.filter(c => new Date(c.properties?.createdate) >= sinceDate);

    // Build campaign map — index by BOTH name and ID so we can match either
    const campaignMap = {};
    const campaignLookup = {}; // maps any identifier (name or ID) → campaign key

    (metaData.data || []).forEach(row => {
      const key = row.campaign_name;
      campaignMap[key] = {
        campaign_id: row.campaign_id, campaign_name: row.campaign_name,
        spend: parseFloat(row.spend || 0), impressions: parseInt(row.impressions || 0),
        clicks: parseInt(row.clicks || 0), ctr: parseFloat(row.ctr || 0),
        cpc: parseFloat(row.cpc || 0), frequency: parseFloat(row.frequency || 0),
        leads: 0, contacts: []
      };
      // Index by both name and ID
      if (row.campaign_name) campaignLookup[row.campaign_name.trim()] = key;
      if (row.campaign_id) campaignLookup[row.campaign_id.trim()] = key;
    });

    // Also pull ad-level data so we can match by ad name / ad ID
    const adInsightsUrl = `${META_BASE}/act_${accountId}/insights?fields=campaign_name,campaign_id,adset_name,adset_id,ad_name,ad_id&time_range={"since":"${since}","until":"${until}"}&level=ad&limit=500&access_token=${metaToken}`;
    const adR = await fetch(adInsightsUrl);
    const adData = await adR.json();
    (adData.data || []).forEach(row => {
      const key = row.campaign_name;
      if (row.ad_name) campaignLookup[row.ad_name.trim()] = key;
      if (row.ad_id) campaignLookup[row.ad_id.trim()] = key;
      if (row.adset_name) campaignLookup[row.adset_name.trim()] = key;
      if (row.adset_id) campaignLookup[row.adset_id.trim()] = key;
    });

    // Match contacts via UTMs — try all possible identifier fields
    let unmatchedCount = 0;
    windowContacts.forEach(contact => {
      const props = contact.properties || {};

      // Try every possible UTM field HubSpot might store
      const candidates = [
        props.utm_campaign,
        props.utm_content,
        props.hs_latest_source_data_1,
        props.hs_latest_source_data_2,
        props.hs_analytics_source_data_1,
        props.hs_analytics_source_data_2
      ].filter(Boolean).map(v => v.trim());

      let matched = false;
      for (const candidate of candidates) {
        const key = campaignLookup[candidate];
        if (key && campaignMap[key]) {
          campaignMap[key].leads++;
          campaignMap[key].contacts.push({
            id: contact.id,
            name: `${props.firstname || ''} ${props.lastname || ''}`.trim(),
            email: props.email, created: props.createdate,
            stage: props.lifecyclestage, country: props.country,
            matched_via: candidate
          });
          matched = true;
          break;
        }
      }
      if (!matched) unmatchedCount++;
    });

    const campaigns = Object.values(campaignMap)
      .map(c => ({ ...c, cpl: c.leads > 0 ? (c.spend / c.leads).toFixed(2) : null }))
      .sort((a, b) => b.spend - a.spend);

    return {
      statusCode: 200, headers,
      body: JSON.stringify({
        period_days: days,
        total_spend: campaigns.reduce((s, c) => s + c.spend, 0).toFixed(2),
        total_leads: windowContacts.length,
        matched_leads: windowContacts.length - unmatchedCount,
        unmatched_leads: unmatchedCount,
        attribution_rate: windowContacts.length > 0
          ? (((windowContacts.length - unmatchedCount) / windowContacts.length) * 100).toFixed(1) : 0,
        campaigns
      })
    };
  } catch (e) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: e.message }) };
  }
};
