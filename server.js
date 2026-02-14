const express = require("express");
const path = require("path");
const fs = require("fs");
const Papa = require("papaparse");

const app = express();
const PORT = process.env.PORT || 3000;

const CSV_PATH = process.env.CSV_PATH || path.join(__dirname, "fans_data.csv");

function normalizeWhitespace(s) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}

function parseNumberLoose(value) {
  if (value == null) return null;
  const s = normalizeWhitespace(value)
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, "") // remove thousands spaces: "18 500" -> "18500"
    .replace(",", "."); // just in case
  if (!s) return null;
  const n = Number.parseFloat(s);
  return Number.isFinite(n) ? n : null;
}

function parseRangeLoose(value) {
  const s = normalizeWhitespace(value);
  if (!s) return { min: null, max: null, raw: "" };
  // Examples: "900 - 3600", "30 - 170", "180"
  const parts = s.split("-").map((p) => normalizeWhitespace(p));
  if (parts.length === 1) {
    const n = parseNumberLoose(parts[0]);
    return { min: n, max: n, raw: s };
  }
  const min = parseNumberLoose(parts[0]);
  const max = parseNumberLoose(parts.slice(1).join("-"));
  return { min, max, raw: s };
}

function slugify(s) {
  return normalizeWhitespace(s)
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, "-")
    .replace(/^-+|-+$/g, "");
}

function rangesOverlap({ min: aMin, max: aMax }, { min: bMin, max: bMax }) {
  // Treat nulls as open bounds (always overlap unless both sides are incompatible)
  const left = aMin ?? -Infinity;
  const right = aMax ?? Infinity;
  const otherLeft = bMin ?? -Infinity;
  const otherRight = bMax ?? Infinity;
  return left <= otherRight && otherLeft <= right;
}

function loadProductsFromCsv(csvPath) {
  const csvText = fs.readFileSync(csvPath, "utf8");
  const parsed = Papa.parse(csvText, {
    header: true,
    skipEmptyLines: true
  });

  if (parsed.errors?.length) {
    // keep going, but surface first error in logs
    console.warn("CSV parse warnings:", parsed.errors.slice(0, 3));
  }

  const rows = parsed.data || [];

  const products = rows
    .map((row, idx) => {
      // PapaParse stores extra columns in __parsed_extra when header count mismatches.
      const extra = Array.isArray(row.__parsed_extra) ? row.__parsed_extra : [];

      const number = normalizeWhitespace(row.number) || String(idx + 1);
      const type = normalizeWhitespace(row.type);
      const model = normalizeWhitespace(row.model);
      const size = normalizeWhitespace(row.size);

      const diameter = parseNumberLoose(row.diameter);

      // In provided CSV, "efficiency" выглядит как диапазон расхода воздуха.
      const airflow = parseRangeLoose(row.efficiency);
      const pressure = parseRangeLoose(row.pressure);

      const power = parseNumberLoose(row.power);
      const noiseLevel = parseNumberLoose(row.noise_level);
      const price = parseNumberLoose(row.price);

      return {
        id: number,
        number,
        type,
        model,
        size,
        diameter,
        airflow, // {min,max,raw}
        pressure, // {min,max,raw}
        power,
        noise_level: noiseLevel,
        price,
        _raw: {
          diameter: normalizeWhitespace(row.diameter),
          efficiency: normalizeWhitespace(row.efficiency),
          pressure: normalizeWhitespace(row.pressure),
          power: normalizeWhitespace(row.power),
          noise_level: normalizeWhitespace(row.noise_level),
          price: normalizeWhitespace(row.price)
        },
        _meta: {
          model_slug: slugify(model),
          extra
        }
      };
    })
    .filter((p) => p.model || p.size || p.type);

  const byId = new Map();
  const byModel = new Map();
  const byModelSlug = new Map();
  for (const p of products) {
    byId.set(String(p.id), p);
    if (p.model) byModel.set(p.model.toLowerCase(), p);
    if (p._meta.model_slug) byModelSlug.set(p._meta.model_slug, p);
  }

  return { products, byId, byModel, byModelSlug };
}

let STORE = null;

function ensureStoreLoaded() {
  if (STORE) return STORE;
  STORE = loadProductsFromCsv(CSV_PATH);
  console.log(`Loaded ${STORE.products.length} products from ${CSV_PATH}`);
  return STORE;
}

app.get("/api/products", (req, res) => {
  const { products } = ensureStoreLoaded();

  const q = normalizeWhitespace(req.query.q).toLowerCase();
  const type = normalizeWhitespace(req.query.type);
  const diameter = parseNumberLoose(req.query.diameter);

  const minPrice = parseNumberLoose(req.query.minPrice);
  const maxPrice = parseNumberLoose(req.query.maxPrice);
  const minPower = parseNumberLoose(req.query.minPower);
  const maxPower = parseNumberLoose(req.query.maxPower);
  const minNoise = parseNumberLoose(req.query.minNoise);
  const maxNoise = parseNumberLoose(req.query.maxNoise);
  const minDiameter = parseNumberLoose(req.query.minDiameter);
  const maxDiameter = parseNumberLoose(req.query.maxDiameter);

  const minAirflow = parseNumberLoose(req.query.minAirflow);
  const maxAirflow = parseNumberLoose(req.query.maxAirflow);
  const minPressure = parseNumberLoose(req.query.minPressure);
  const maxPressure = parseNumberLoose(req.query.maxPressure);

  let out = products.slice();

  if (q) {
    out = out.filter((p) => {
      const hay = `${p.model} ${p.size} ${p.type}`.toLowerCase();
      return hay.includes(q);
    });
  }
  if (type) out = out.filter((p) => p.type === type);
  if (diameter != null) out = out.filter((p) => p.diameter === diameter);

  if (minPrice != null) out = out.filter((p) => p.price != null && p.price >= minPrice);
  if (maxPrice != null) out = out.filter((p) => p.price != null && p.price <= maxPrice);

  if (minPower != null) out = out.filter((p) => p.power != null && p.power >= minPower);
  if (maxPower != null) out = out.filter((p) => p.power != null && p.power <= maxPower);

  if (minNoise != null) out = out.filter((p) => p.noise_level != null && p.noise_level >= minNoise);
  if (maxNoise != null) out = out.filter((p) => p.noise_level != null && p.noise_level <= maxNoise);

  if (minDiameter != null) out = out.filter((p) => p.diameter != null && p.diameter >= minDiameter);
  if (maxDiameter != null) out = out.filter((p) => p.diameter != null && p.diameter <= maxDiameter);

  if (minAirflow != null || maxAirflow != null) {
    out = out.filter((p) =>
      rangesOverlap(p.airflow, { min: minAirflow ?? null, max: maxAirflow ?? null })
    );
  }

  if (minPressure != null || maxPressure != null) {
    out = out.filter((p) =>
      rangesOverlap(p.pressure, { min: minPressure ?? null, max: maxPressure ?? null })
    );
  }

  // simple default sort: cheaper first if price exists, otherwise by model
  out.sort((a, b) => {
    const ap = a.price ?? Infinity;
    const bp = b.price ?? Infinity;
    if (ap !== bp) return ap - bp;
    return String(a.model).localeCompare(String(b.model), "ru");
  });

  res.json(out);
});

app.get("/api/products/:id", (req, res) => {
  const { byId, byModel, byModelSlug } = ensureStoreLoaded();
  const raw = normalizeWhitespace(req.params.id);
  const key = raw.toLowerCase();

  const found = byId.get(raw) || byModel.get(key) || byModelSlug.get(slugify(raw));
  if (!found) return res.status(404).json({ error: "Product not found" });

  res.json(found);
});

app.use(express.static(path.join(__dirname, "public")));

// Simple health/info
app.get("/api/health", (req, res) => {
  const { products } = ensureStoreLoaded();
  res.json({ ok: true, products: products.length });
});

app.listen(PORT, () => {
  ensureStoreLoaded();
  console.log(`Server running: http://localhost:${PORT}`);
});

