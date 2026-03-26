const fetch = require('node-fetch');

const META_BASE = 'https://graph.facebook.com/v18.0';
const HUBSPOT_BASE = 'https://api.hubapi.com';

// Normalize a string for comparison — lowercase + collapse spaces
function norm(s) {
  if (!s) return '';
  return String(s).toLowerCase().replace(/\s+/g, ' ').trim();
}

exports.handler = async (event) => {
  const headers = {
    'Access-Control-Allow-Origin': '*',
    'Content-Type': 'application/json'
  };

  const password = process.env.DASHBOARD_PASSWORD;
  if (password && event.headers['x-dashboard-password'] !== password) {
    return { statusCode: 401, headers, body: JSON.stringify({ error: 'Unauthorized' }) };
  }

  const metaToken = process.env.META_ACCESS_TOKEN;
  const accountId = process.env.META_AD_ACCOUNT_ID;
  const hsToken = process.env.HUBSPOT_ACCESS_TOKEN;

  if (!metaToken || !accountId || !hsToken) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: 'Missing server credentials.' }) };
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

    // ── Meta ad-level for ID/name lookups ──
    const adInsightsUrl = `${META_BASE}/act_${accountId}/insights?fields=campaign_name,campaign_id,adset_name,adset_id,ad_name,ad_id&time_range={"since":"${since}","until":"${until}"}&level=ad&limit=500&access_token=${metaToken}`;
    const adR = await fetch(adInsightsUrl);
    const adData = await adR.json();

    // ── Build campaign lookup — normalized keys ──
    const campaignMap = {};
    const campaignLookup = {}; // normalized string → campaign_name key

    (metaData.data || []).forEach(row => {
      const key = row.campaign_name;
      campaignMap[key] = {
        campaign_id: row.campaign_id, campaign_name: row.campaign_name,
        spend: parseFloat(row.spend || 0), impressions: parseInt(row.impressions || 0),
        clicks: parseInt(row.clicks || 0), ctr: parseFloat(row.ctr || 0),
        cpc: parseFloat(row.cpc || 0), frequency: parseFloat(row.frequency || 0),
        leads: 0, contacts: []
      };
      // Index by normalized campaign name AND raw campaign ID
      campaignLookup[norm(row.campaign_name)] = key;
      if (row.campaign_id) campaignLookup[norm(row.campaign_id)] = key;
    });

    // Index by ad name, ad ID, adset name, adset ID
    (adData.data || []).forEach(row => {
      const key = row.campaign_name;
      if (!key) return;
      if (row.ad_name)   campaignLookup[norm(row.ad_name)]   = key;
      if (row.ad_id)     campaignLookup[norm(row.ad_id)]     = key;
      if (row.adset_name) campaignLookup[norm(row.adset_name)] = key;
      if (row.adset_id)   campaignLookup[norm(row.adset_id)]   = key;
    });

    // ── Pull HubSpot contacts ──
    const properties = [
      'firstname','lastname','email','createdate','lifecyclestage',
      'utm_campaign','utm_content','utm_source','utm_medium','utm_adset',
      'hs_analytics_source','hs_analytics_source_data_1','hs_analytics_source_data_2',
      'hs_latest_source','hs_latest_source_data_1','hs_latest_source_data_2',
      'country','hs_lead_status'
    ].join(',');

    let allContacts = [], after = null, page = 0;
    do {
      const url = `${HUBSPOT_BASE}/crm/v3/objects/contacts?limit=100&properties=${properties}${after ? `&after=${after}` : ''}`;
      const r = await fetch(url, { headers: { Authorization: `Bearer ${hsToken}` } });
      const data = await r.json();
      if (data.status === 'error') return { statusCode: 400, headers, body: JSON.stringify({ error: 'HubSpot: ' + data.message }) };
      allContacts = allContacts.concat(data.results || []);
      after = data.paging?.next?.after || null;
      page++;
    } while (after && page < 20);

    // Filter to date window
    const sinceDate = new Date(Date.now() - days * 86400000);
    const windowContacts = allContacts.filter(c => new Date(c.properties?.createdate) >= sinceDate);

    // ── Match contacts to campaigns (normalized, case-insensitive) ──
    let unmatchedCount = 0;
    const unmatchedSamples = [];

    windowContacts.forEach(contact => {
      const props = contact.properties || {};

      // All possible fields that could contain a campaign identifier
      const candidates = [
        props.utm_campaign,
        props.utm_content,
        props.utm_adset,
        props.hs_latest_source_data_1,
        props.hs_latest_source_data_2,
        props.hs_analytics_source_data_1,
        props.hs_analytics_source_data_2,
      ].filter(Boolean);

      let matched = false;
      for (const candidate of candidates) {
        const key = campaignLookup[norm(candidate)];
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

      if (!matched) {
        unmatchedCount++;
        if (unmatchedSamples.length < 5) {
          unmatchedSamples.push({
            email: props.email,
            utm_campaign: props.utm_campaign,
            utm_content: props.utm_content,
            hs_latest_source_data_1: props.hs_latest_source_data_1,
            hs_latest_source_data_2: props.hs_latest_source_data_2,
          });
        }
      }
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
        campaigns,
        debug: { unmatched_samples: unmatchedSamples }
      })
    };
  } catch (e) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: e.message }) };
  }
};
