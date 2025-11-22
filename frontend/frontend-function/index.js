const functions = require('@google-cloud/functions-framework');
const { BigQuery } = require('@google-cloud/bigquery');

const bigquery = new BigQuery({
  projectId: 'sentinel-h-5'
});

functions.http('fetch_pending', async (req, res) => {
  // Enable CORS
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(204).send('');
    return;
  }

  try {
    const query = `
      SELECT 
        t.cluster_id,
        COUNT(m.unique_id) as cases,
        t.algorithm as algorithm_type,
        t.created_date as input_date,
        'TN001' as sitecode
      FROM \`sentinel-h-5.sentinel_h_5.temp_cluster_table\` t
      LEFT JOIN \`sentinel-h-5.sentinel_h_5.cluster_members\` m ON t.cluster_id = m.cluster_id
      WHERE t.accept_status = 'pending'
      GROUP BY t.cluster_id, t.algorithm, t.created_date
      ORDER BY t.created_date DESC
    `;

    const [rows] = await bigquery.query(query);
    
    res.json({
      success: true,
      total_pending: rows.length,
      clusters: rows
    });

  } catch (error) {
    console.error('Error fetching pending clusters:', error);
    res.status(500).json({
      success: false,
      message: error.message
    });
  }
});