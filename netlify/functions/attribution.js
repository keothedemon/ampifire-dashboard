const fetch = require('node-fetch');

const META_BASE = 'https://graph.facebook.com/v18.0';
const HUBSPOT_BASE = 'https://api.hubapi.com';

function norm(s) {
  if (!s) return '';
  return String(s).toLowerCase().replace(/\s+/g, ' ').trim();
}

exports.handler = async (event) => {
  const headers = { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' };

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
    // ── Meta campaign insights (for spend data) ──
    const insightsUrl = `${META_BASE}/act_${accountId}/insights?fields=campaign_name,campaign_id,spend,impressions,clicks,ctr,cpc,frequency&time_range={"since":"${since}","until":"${until}"}&level=campaign&limit=100&access_token=${metaToken}`;
    const metaR = await fetch(insightsUrl);
    const metaData = await metaR.json();
    if (metaData.error) return { statusCode: 400, headers, body: JSON.stringify({ error: 'Meta: ' + metaData.error.message }) };

    // ── Meta ad-level for ID lookups ──
    const adInsightsUrl = `${META_BASE}/act_${accountId}/insights?fields=campaign_name,campaign_id,adset_name,adset_id,ad_name,ad_id&time_range={"since":"${since}","until":"${until}"}&level=ad&limit=500&access_token=${metaToken}`;
    const adR = await fetch(adInsightsUrl);
    const adData = await adR.json();

    // ── Build Meta spend lookup (normalized) ──
    const metaSpendMap = {}; // norm(campaign_name) → spend data
    (metaData.data || []).forEach(row => {
      metaSpendMap[norm(row.campaign_name)] = {
        campaign_id: row.campaign_id,
        campaign_name: row.campaign_name,
        spend: parseFloat(row.spend || 0),
        impressions: parseInt(row.impressions || 0),
        clicks: parseInt(row.clicks || 0),
        ctr: parseFloat(row.ctr || 0),
        cpc: parseFloat(row.cpc || 0),
        frequency: parseFloat(row.frequency || 0),
      };
      if (row.campaign_id) metaSpendMap[norm(row.campaign_id)] = metaSpendMap[norm(row.campaign_name)];
    });

    // Index ad IDs → campaign name
    (adData.data || []).forEach(row => {
      if (!row.campaign_name) return;
      const spendData = metaSpendMap[norm(row.campaign_name)];
      if (spendData) {
        if (row.ad_id) metaSpendMap[norm(row.ad_id)] = spendData;
        if (row.ad_name) metaSpendMap[norm(row.ad_name)] = spendData;
        if (row.adset_id) metaSpendMap[norm(row.adset_id)] = spendData;
        if (row.adset_name) metaSpendMap[norm(row.adset_name)] = spendData;
      }
    });

    // ── Pull HubSpot contacts ──
    const properties = [
      'firstname','lastname','email','createdate','lifecyclestage',
      'utm_campaign','utm_content','utm_source','utm_medium','utm_adset',
      'hs_analytics_source_data_1','hs_analytics_source_data_2',
      'hs_latest_source_data_1','hs_latest_source_data_2',
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

    // ── Group HubSpot contacts by utm_campaign ──
    // Key insight: group by HubSpot utm_campaign value first,
    // THEN join Meta spend where available
    const campaignMap = {};

    windowContacts.forEach(contact => {
      const props = contact.properties || {};

      // Get the primary campaign identifier from HubSpot
      const campaignName = props.utm_campaign
        || props.hs_latest_source_data_1
        || props.hs_analytics_source_data_1
        || 'Unknown / No UTM';

      if (!campaignMap[campaignName]) {
        campaignMap[campaignName] = {
          campaign_name: campaignName,
          campaign_id: null,
          spend: 0, impressions: 0, clicks: 0,
          ctr: 0, cpc: 0, frequency: 0,
          leads: 0, contacts: [],
          has_meta_data: false
        };

        // Try to join Meta spend data
        const candidates = [
          norm(campaignName),
          norm(props.utm_content),
          norm(props.utm_adset),
          norm(props.hs_latest_source_data_2),
          norm(props.hs_analytics_source_data_2),
        ].filter(Boolean);

        for (const candidate of candidates) {
          const metaData = metaSpendMap[candidate];
          if (metaData) {
            campaignMap[campaignName].campaign_id = metaData.campaign_id;
            campaignMap[campaignName].spend = metaData.spend;
            campaignMap[campaignName].impressions = metaData.impressions;
            campaignMap[campaignName].clicks = metaData.clicks;
            campaignMap[campaignName].ctr = metaData.ctr;
            campaignMap[campaignName].cpc = metaData.cpc;
            campaignMap[campaignName].frequency = metaData.frequency;
            campaignMap[campaignName].has_meta_data = true;
            break;
          }
        }
      }

      campaignMap[campaignName].leads++;
      campaignMap[campaignName].contacts.push({
        id: contact.id,
        name: `${props.firstname || ''} ${props.lastname || ''}`.trim(),
        email: props.email,
        created: props.createdate,
        stage: props.lifecyclestage,
        country: props.country,
        utm_source: props.utm_source,
        utm_medium: props.utm_medium,
      });
    });

    const campaigns = Object.values(campaignMap)
      .map(c => ({ ...c, cpl: c.leads > 0 && c.spend > 0 ? (c.spend / c.leads).toFixed(2) : null }))
      .sort((a, b) => b.leads - a.leads); // sort by most leads

    const totalSpend = (metaData.data || []).reduce((s, r) => s + parseFloat(r.spend || 0), 0);

    return {
      statusCode: 200, headers,
      body: JSON.stringify({
        period_days: days,
        total_spend: totalSpend.toFixed(2),
        total_leads: windowContacts.length,
        matched_leads: Object.values(campaignMap).filter(c => c.has_meta_data).reduce((s, c) => s + c.leads, 0),
        unmatched_leads: Object.values(campaignMap).filter(c => !c.has_meta_data).reduce((s, c) => s + c.leads, 0),
        attribution_rate: windowContacts.length > 0
          ? ((Object.values(campaignMap).filter(c => c.has_meta_data).reduce((s, c) => s + c.leads, 0) / windowContacts.length) * 100).toFixed(1)
          : 0,
        campaigns
      })
    };
  } catch (e) {
    return { statusCode: 500, headers, body: JSON.stringify({ error: e.message }) };
  }
};
