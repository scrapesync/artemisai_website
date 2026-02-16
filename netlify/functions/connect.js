// netlify/functions/connect.js
// Handles token submission from connect.html
// Saves to AWS Redshift + sends email via AWS SES

const { Client } = require("pg");
const { SESClient, SendEmailCommand } = require("@aws-sdk/client-ses");

exports.handler = async (event) => {
  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Content-Type": "application/json",
  };

  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers };
  }

  if (event.httpMethod !== "POST") {
    return { statusCode: 405, headers, body: JSON.stringify({ error: "Method not allowed" }) };
  }

  let payload;
  try {
    payload = JSON.parse(event.body);
  } catch {
    return { statusCode: 400, headers, body: JSON.stringify({ error: "Invalid JSON" }) };
  }

  const { type, user_token, user_name, user_email, pages, code } = payload;

  if (!pages || !Array.isArray(pages) || pages.length === 0) {
    return { statusCode: 400, headers, body: JSON.stringify({ error: "No pages provided" }) };
  }

  // ─── Save to Redshift ───
  const client = new Client({
    host: process.env.REDSHIFT_HOST,
    port: parseInt(process.env.REDSHIFT_PORT || "5439"),
    database: process.env.REDSHIFT_DATABASE,
    user: process.env.REDSHIFT_USER,
    password: process.env.REDSHIFT_PASSWORD,
    ssl: { rejectUnauthorized: false },
    connectionTimeoutMillis: 10000,
    query_timeout: 15000,
  });

  let savedCount = 0;
  const errors = [];

  try {
    await client.connect();

    await client.query(`
      CREATE TABLE IF NOT EXISTS artemis_fb_connections (
        id              INTEGER IDENTITY(1,1) PRIMARY KEY,
        user_name       VARCHAR(256),
        user_email      VARCHAR(256),
        user_token      VARCHAR(1024),
        page_id         VARCHAR(64) NOT NULL,
        page_name       VARCHAR(512),
        page_token      VARCHAR(1024),
        page_category   VARCHAR(256),
        tasks           VARCHAR(1024),
        connection_type VARCHAR(32),
        connected_at    TIMESTAMP DEFAULT GETDATE(),
        status          VARCHAR(32) DEFAULT 'active'
      )
    `);

    for (const page of pages) {
      try {
        await client.query(`DELETE FROM artemis_fb_connections WHERE page_id = $1`, [page.id]);
        await client.query(
          `INSERT INTO artemis_fb_connections 
           (user_name, user_email, user_token, page_id, page_name, page_token, page_category, tasks, connection_type)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
          [
            user_name || null,
            user_email || null,
            user_token || null,
            page.id,
            page.name || null,
            page.access_token || null,
            page.category || null,
            JSON.stringify(page.tasks || []),
            type || "manual_token",
          ]
        );
        savedCount++;
      } catch (err) {
        errors.push({ page_id: page.id, error: err.message });
      }
    }
  } catch (err) {
    console.error("Redshift connection error:", err);
    await sendEmailNotification({ success: false, error: err.message, user_name, user_email, pages });
    return { statusCode: 500, headers, body: JSON.stringify({ error: "Database connection failed", detail: err.message }) };
  } finally {
    try { await client.end(); } catch {}
  }

  // ─── Send Email via AWS SES ───
  await sendEmailNotification({ success: true, user_name, user_email, pages, savedCount, errors, type });

  return {
    statusCode: 200,
    headers,
    body: JSON.stringify({ success: true, saved: savedCount, total: pages.length, errors: errors.length > 0 ? errors : undefined }),
  };
};

// ─── AWS SES Email ───
async function sendEmailNotification(data) {
  const notifyEmail = process.env.NOTIFICATION_EMAIL;
  const fromEmail = process.env.SES_FROM_EMAIL || notifyEmail;
  if (!notifyEmail) { console.warn("NOTIFICATION_EMAIL not set"); return; }

  const ses = new SESClient({
    region: process.env.AWS_SES_REGION || "eu-west-2",
    credentials: {
      accessKeyId: process.env.AWS_SES_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SES_SECRET_ACCESS_KEY,
    },
  });

  const pageListHtml = (data.pages || [])
    .map((p) => `<li><strong>${p.name || "Unknown"}</strong> — ID: ${p.id}</li>`)
    .join("");

  const pageListText = (data.pages || [])
    .map((p) => `  - ${p.name || "Unknown"} (ID: ${p.id})`)
    .join("\n");

  let subject, textBody, htmlBody;

  if (data.success) {
    subject = `New FB Connection: ${data.savedCount} page(s) from ${data.user_name || "Unknown User"}`;

    textBody = `NEW FACEBOOK PAGES CONNECTED\n============================\n\nName: ${data.user_name || "N/A"}\nEmail: ${data.user_email || "N/A"}\nPages: ${data.savedCount}/${data.pages.length}\nMethod: ${data.type || "manual"}\nTime: ${new Date().toISOString()}\n\nPages:\n${pageListText}\n\n${data.errors && data.errors.length > 0 ? data.errors.length + " error(s)" : "All saved successfully."}`;

    htmlBody = `
      <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;">
        <div style="background:#060a13;padding:24px;border-radius:12px 12px 0 0;">
          <h1 style="color:#00d4aa;margin:0;font-size:20px;">New Facebook Pages Connected</h1>
        </div>
        <div style="background:#0f1729;padding:24px;color:#e8ecf4;">
          <table style="width:100%;border-collapse:collapse;margin-bottom:16px;">
            <tr><td style="padding:8px 0;color:#7b8ba8;width:140px;">Name</td><td style="padding:8px 0;color:#e8ecf4;font-weight:600;">${data.user_name || "N/A"}</td></tr>
            <tr><td style="padding:8px 0;color:#7b8ba8;">Email</td><td style="padding:8px 0;color:#e8ecf4;">${data.user_email || "N/A"}</td></tr>
            <tr><td style="padding:8px 0;color:#7b8ba8;">Pages Saved</td><td style="padding:8px 0;color:#00d4aa;font-weight:600;">${data.savedCount} / ${data.pages.length}</td></tr>
            <tr><td style="padding:8px 0;color:#7b8ba8;">Method</td><td style="padding:8px 0;color:#e8ecf4;">${data.type || "manual"}</td></tr>
            <tr><td style="padding:8px 0;color:#7b8ba8;">Time</td><td style="padding:8px 0;color:#e8ecf4;">${new Date().toLocaleString("en-GB", { timeZone: "Europe/London" })}</td></tr>
          </table>
          <h3 style="color:#00d4aa;font-size:14px;margin:16px 0 8px;">Connected Pages</h3>
          <ul style="color:#e8ecf4;padding-left:20px;margin:0;">${pageListHtml}</ul>
          ${data.errors && data.errors.length > 0
            ? `<div style="margin-top:16px;padding:12px;background:rgba(248,113,113,0.1);border:1px solid rgba(248,113,113,0.3);border-radius:8px;"><strong style="color:#f87171;">${data.errors.length} error(s)</strong></div>`
            : `<div style="margin-top:16px;padding:12px;background:rgba(0,212,170,0.1);border:1px solid rgba(0,212,170,0.2);border-radius:8px;color:#00d4aa;">All pages saved to Redshift successfully.</div>`
          }
        </div>
        <div style="background:#060a13;padding:16px 24px;border-radius:0 0 12px 12px;text-align:center;">
          <span style="color:#4a5568;font-size:12px;">Artemis AI</span>
        </div>
      </div>`;
  } else {
    subject = `FB Connection FAILED — ${data.user_name || "Unknown"}`;
    textBody = `FAILED\n\nName: ${data.user_name || "N/A"}\nError: ${data.error}\nPages attempted: ${(data.pages || []).length}`;
    htmlBody = `<div style="font-family:Arial;max-width:600px;margin:0 auto;"><div style="background:#060a13;padding:24px;border-radius:12px 12px 0 0;"><h1 style="color:#f87171;margin:0;font-size:20px;">Connection Failed</h1></div><div style="background:#0f1729;padding:24px;color:#e8ecf4;"><p><strong>Error:</strong> ${data.error}</p><p><strong>Pages attempted:</strong></p><ul>${pageListHtml}</ul></div></div>`;
  }

  try {
    await ses.send(new SendEmailCommand({
      Source: fromEmail,
      Destination: { ToAddresses: [notifyEmail] },
      Message: {
        Subject: { Data: subject, Charset: "UTF-8" },
        Body: {
          Text: { Data: textBody, Charset: "UTF-8" },
          Html: { Data: htmlBody, Charset: "UTF-8" },
        },
      },
    }));
    console.log("Email sent successfully");
  } catch (err) {
    console.error("SES email failed:", err);
  }
}