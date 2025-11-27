# HEBS-DSR Report Template

## Frontend JavaScript Template

```javascript
function generateHEBSReport(data) {
    const template = `
# **Hospital Event Based Surveillance – Daily Summary Report (HEBS-DSR)**

**Report ID:** ${data.report_id}
**Date:** ${data.report_date}
**Period:** ${data.start_date} to ${data.end_date}

---

## **Case Summary (Last 7–10 Days)**

* **AUFI Cases (last 7 days):** ${data.summary.aufi_count || 0}
* **ADD Cases (last 7 days):** ${data.summary.add_count || 0}
* **SARI Cases (last 7–10 days):** ${data.summary.sari_count || 0}
* **AES Cases (last 7–10 days):** ${data.summary.aes_count || 0}
* **Jaundice (last 7 days):** ${data.summary.jaundice_count || 0}
* **Fever with rash (last 7 days):** ${data.summary.fever_rash_count || 0}

---

# **List of Signals Reported**

---

${generateSignalsSection(data.signals)}

---

### **Total Signals Reported**

${generateSignalSummary(data.signals)}

---

# **List of New Cases Added to Existing Signals**

*(No new cases for this period)*

---
`;
    return template;
}

function generateSignalsSection(signals) {
    if (!signals || signals.length === 0) {
        return '*(No signals detected for this period)*';
    }
    
    return signals.map(signal => {
        const casesTable = signal.cases.map(case => 
            `| ${case.admission_date} | ${case.state} – ${case.district} | ${case.sub_district} | ${case.village} | ${case.name} | ${case.age} | ${case.patient_id} |`
        ).join('\n');
        
        return `## **Signal ID: ${signal.signal_id} — Syndrome: ${signal.syndrome}**

| Date of Admission | State & District | Sub-district | Village | Name | Age (yrs) | Patient ID |
| ----------------- | ---------------- | ------------ | ------- | ---- | --------- | ---------- |
${casesTable}`;
    }).join('\n\n---\n\n');
}

function generateSignalSummary(signals) {
    if (!signals || signals.length === 0) {
        return '* **Total – 0**';
    }
    
    const syndromeCounts = {};
    signals.forEach(signal => {
        syndromeCounts[signal.syndrome] = (syndromeCounts[signal.syndrome] || 0) + 1;
    });
    
    const summaryLines = Object.entries(syndromeCounts)
        .map(([syndrome, count]) => `* **${syndrome} – ${count}**`);
    summaryLines.push(`* **Total – ${signals.length}**`);
    
    return summaryLines.join('\n');
}
```

## API Data Structure

```json
{
  "report_id": "EBS_DSR-20251124-001",
  "report_date": "24.11.2025",
  "start_date": "17.11.2025",
  "end_date": "23.11.2025",
  "summary": {
    "aufi_count": 10,
    "add_count": 5,
    "sari_count": 3,
    "aes_count": 2,
    "jaundice_count": 1,
    "fever_rash_count": 0
  },
  "signals": [
    {
      "signal_id": "MMCS0188",
      "syndrome": "AUFI",
      "cases": [
        {
          "admission_date": "22-11-2025",
          "state": "West Bengal",
          "district": "Malda",
          "sub_district": "Kaliachak-I",
          "village": "Bakharpur",
          "name": "Amir Hamja",
          "age": 8,
          "patient_id": "6311"
        }
      ]
    }
  ]
}
```